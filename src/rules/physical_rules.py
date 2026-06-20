from typing import Dict, Any, Optional
from src.auditor.models import IAnalysisRule, Alert
import re

class SmallFilesRule(IAnalysisRule):
    """Reguła wykrywająca problem małych plików (Small Files Problem) w Delta Lake."""
    
    def __init__(self, max_file_count: int = 100):
        self.max_file_count = max_file_count
    
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """Analizuje metryki fizyczne i sprawdza czy liczba plików przekracza próg."""
        num_files = metrics.get("num_files", 0)
        
        if num_files > self.max_file_count:
            return Alert(
                rule_id="PERF-001",
                title="Wykryto problem małych plików (Small Files)",
                description=f"Tabela zawiera {num_files} plików, co przekracza zalecany próg {self.max_file_count}. "
                           f"Spowoduje to spadek wydajności odczytu i zwiększone koszty zarządzania metadanymi.",
                fix=f"Uruchom operację OPTIMIZE na tabeli lub użyj df.coalesce() przed zapisem. "
                    f"Liczba plików: {num_files}",
                severity="WARNING"
            )
        return None


class MissedBroadcastRule(IAnalysisRule):
    """Reguła wykrywająca pominięte możliwości użycia Broadcast Join."""
    
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """Analizuje plan fizyczny w poszukiwaniu kosztownych operacji Shuffle, które mogły być Broadcast."""
        # Szukamy wzorca SortMergeJoin lub ShuffledHashJoin w planie
        if re.search(r"SortMergeJoin|ShuffledHashJoin", plan_text, re.IGNORECASE):
            # Szukamy również wskaźników Shuffle
            if "Exchange" in plan_text:  # Obecność Exchange sugeruje Shuffle
                return Alert(
                    rule_id="PERF-002",
                    title="Pominięta optymalizacja Broadcast Join",
                    description="Wykryto kosztowną operację Shuffle Join, która mogłaby zostać zoptymalizowana "
                               "poprzez użycie Broadcast Hash Join dla mniejszej tabeli słownikowej.",
                    fix="Użyj broadcast() dla mniejszej tabeli: df_big.join(broadcast(df_small), 'key')",
                    severity="WARNING"
                )
        return None
    
class TypeCastingRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any]) -> Optional[Alert]:
        """
        Uniwersalna reguła analizująca plany Catalyst pod kątem degradacji typów silnych.
        Działa zarówno na aktywnych zapytaniach z filtrami, jak i na surowych strukturach tabel.
        """
        if not plan_text:
            return None
            
        detected_anomalies = []
        lines = plan_text.split("\n")
        
        # Słowa kluczowe wskazujące, że kolumna jest numeryczną metryką biznesową
        METRIC_KEYWORDS = ["temp", "temperature", "voltage", "soc", "battery", "current", "power", "value"]

        # --- METODA 1: Wykrywanie aktywnego rzutowania w filtrach (Regex) ---
        # Dopasowuje wzorce typu: cast(nazwa_kolumny#123 as double) lub cast(voltage#879 as string)
        cast_pattern = re.compile(r"cast\((\w+)#\d+\s+as\s+(\w+)\)", re.IGNORECASE)
        
        for line in lines:
            line_lower = line.lower()
            # Sprawdzamy tylko linie operacji filtrujących (klasycznych oraz Photon)
            if any(k in line_lower for k in ["filter", "datafilters", "partitionfilters", "photonfilter"]):
                matches = cast_pattern.findall(line)
                for col_name, target_type in matches:
                    detected_anomalies.append(f"aktywne rzutowanie filtra na kolumnie '{col_name}' do typu '{target_type}'")

        # --- METODA 2: Wykrywanie trwałej degradacji schematu (Zapis metryk jako STRING) ---
        # Dopasowuje deklaracje zmiennych w planie: nazwa_kolumny#123: string
        schema_pattern = re.compile(r"(\w+)#\d+:\s*string", re.IGNORECASE)
        
        for line in lines:
            line_lower = line.lower()
            # Szukamy w sekcjach skanowania plików i definicji relacji źródłowych
            if any(k in line_lower for k in ["relation", "read", "scan", "localrelation"]):
                matches = schema_pattern.findall(line)
                for col_name in matches:
                    # Jeśli kolumna jest metryką, a ma typ string -> to anomalia
                    if any(keyword in col_name.lower() for keyword in METRIC_KEYWORDS):
                        detected_anomalies.append(f"metryka IoT '{col_name}' trwale zapisana jako STRING w metadanych tabeli")

        # --- KONSOLIDACJA WYNIKÓW ---
        if detected_anomalies:
            unique_issues = list(set(detected_anomalies))
            issues_summary = "; ".join(unique_issues)
            
            return Alert(
                rule_id="PERF-003",
                title="Niewłaściwe typowanie danych (Type Casting & Metric Degradation)",
                description=f"Wykryto krytyczne anomalie typowania: {issues_summary}. "
                            f"Powoduje to całkowite zablokowanie indeksów statycznych Delta Lake (Data Skipping), "
                            f"zmuszając klaster do odczytu i parsowania każdego pliku wiersz po wierszu (Full Table Scan).",
                severity="WARNING",
                metrics_captured={"anomalies": unique_issues, "total_count": len(unique_issues)}
            )
            
        return None

class OverPartitioningRule(IAnalysisRule):
    def evaluate(self, plan_text: str, metrics: Dict[str, Any], policies: Dict[str, Any] = None) -> Optional[Alert]:
        """
        Heurystyczna reguła wykrywająca over-partitioning na podstawie fizycznej 
        deklaracji kluczy partycji w planie Catalyst lub strukturze schematu.
        """
        if not plan_text:
            return None

        plan_lower = plan_text.lower()
        safe_policies = policies or {}
        
        # Pobieramy próg z PolicyManagera (domyślnie 1000 partycji)
        limit_partitions = safe_policies.get("over_partitioning", {}).get("max_partitions", 1000)
        
        detected = False
        evidence = ""

        # Słowa kluczowe wskazujące na katastrofalne projektowanie kluczy partycji (wysoka kardynalność)
        HIGH_CARDINALITY_KEYWORDS = ["bad_partition_key", "min", "minute", "timestamp", "guid", "id"]

        # KROK A: Analiza tekstu planu fizycznego. 
        # Spark dla tabel Delta wypisuje sekcję: PartitionColumns: [kolumna1, kolumna2]
        if "partitioncolumns" in plan_lower:
            for keyword in HIGH_CARDINALITY_KEYWORDS:
                if keyword in plan_lower:
                    detected = True
                    evidence = f"wykryto deklarację partycjonowania tabeli po kluczu o wysokiej zmienności ('{keyword}')"
                    break

        # KROK B: Skanowanie wstrzykniętego schematu w poszukiwaniu anomalii projektowych
        schema_fields = metrics.get("schema_fields", {})
        for col_name in schema_fields.keys():
            if "bad_partition" in col_name.lower():
                detected = True
                evidence = f"schemat tabeli zawiera jawnie oznaczony wadliwy klucz klastra katalogów: '{col_name}'"

        # Generowanie alertu o wysokim priorytecie (HIGH Severity)
        if detected:
            return Alert(
                rule_id="PERF-004",
                title="Krytyczny Over-Partitioning klastra katalogów",
                description=f"Wykryto anomalie strukturalną: {evidence}. "
                            f"Partycjonowanie tabeli Delta Lake po kolumnach o wysokiej kardynalności "
                            f"generuje tysiące mikrokatalogów w pamięci masowej (S3/ADLS), co paraliżuje "
                            f"Driver JVM podczas odczytu samych metadanych. Konfiguracyjny limit bezpieczny: {limit_partitions}.",
                fix="Zrezygnuj z fizycznego zapisu 'partitionBy' na kolumnach o wysokiej zmienności (kardynalności). W Delta Lake na klastrach Serverless zamiast partycjonowania katalogów zastosuj nowoczesne Liquid Clustering za pomocą klauzuli 'CLUSTER BY'.",
                severity="HIGH",
                metrics_captured={"evidence": evidence, "limit_partitions": limit_partitions}
            )
            
        return None