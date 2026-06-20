# Brak importów jest tutaj pożądany - to moduł czystych danych (Data Module).

TEMPLATES = {
    "PERF-001": {
        "text": "Wywołaj operację kompaktowania danych (OPTIMIZE) na tabeli Delta Lake, lub zastosuj instrukcję zrównoważenia wątków zapisu.",
        "code": """spark.sql("OPTIMIZE {table_name}")
# Lub w PySpark DataFrame przed zapisem:
df.coalesce(1)"""
    },
    "PERF-002": {
        "text": "Wymuś strategię rozgłoszeniową dla mniejszej tabeli słownikowej, aby wyeliminować fazę Shuffle.",
        "code": """from pyspark.sql.functions import broadcast
df_final = df_heavy.join(broadcast(df_light), "key")"""
    },
    "PERF-003": {
        "text": "Usuń jawne rzutowanie typów w warunkach filtrowania, aby umożliwić działanie indeksów Data Skipping.",
        "code": """# Zamiast:
df.filter(F.col("temperature").cast("string") == "25")
# Użyj poprawnego typu danych:
df.filter(F.col("temperature") == 25)"""
    }
}