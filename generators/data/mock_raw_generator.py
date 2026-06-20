# src/laboratory/raw_generator.py
import os
import json
import random
from datetime import datetime, timedelta

def generate_fragmented_json_logs(target_path: str, num_files: int = 200):
    """
    Generuje surowe struktury danych telemetrycznych w celu odtworzenia
    anomalii wydajnościowych w środowisku laboratoryjnym.
    """
    # Standaryzacja ścieżek bazowych wewnątrz wolumenu UC
    bess_dir = f"{target_path}/bess_telemetry_raw"
    pv_dir = f"{target_path}/pv_metrics_raw"
    inverter_dir = f"{target_path}/inverter_logs_raw"
    
    os.makedirs(bess_dir, exist_ok=True)
    os.makedirs(pv_dir, exist_ok=True)
    os.makedirs(inverter_dir, exist_ok=True)
    
    # 1. ETL-01: Małe pliki dla BESS (Generujemy fizyczną fragmentację)
    print(f"[GENERATOR] Tworzenie {num_files} mikro-plików JSON w: {bess_dir}")
    for i in range(num_files):
        data = [{
            "id": random.randint(10000, 99999),
            "station_id": f"S{random.randint(1, 3):03d}",
            "battery_temp": round(random.uniform(20.0, 48.0), 2),
            "soc": round(random.uniform(5.0, 98.0), 2),
            "timestamp": (datetime.now() - timedelta(seconds=i*5)).isoformat()
        }]
        with open(f"{bess_dir}/bess_chunk_{i}.json", "w", encoding="utf-8") as f:
            json.dump(data, f)

    # 2. ETL-02: Dane dla PV Metrics (Zrzut do typowania)
    print(f"[GENERATOR] Tworzenie surowego pliku CSV dla PV w: {pv_dir}")
    with open(f"{pv_dir}/pv_data_mock.csv", "w", encoding="utf-8") as f:
        f.write("station_id,temperature,voltage,timestamp\n")
        for i in range(500):
            f.write(f"S{random.randint(1,3):03d},{round(random.uniform(12.0, 38.0), 2)},{round(random.uniform(215.0, 245.0), 2)},{datetime.now().isoformat()}\n")

    # 3. ETL-04: Logi inwerterów ze skośnością (Data Skew)
    print(f"[GENERATOR] Tworzenie skośnego pliku JSON logów w: {inverter_dir}")
    with open(f"{inverter_dir}/inverter_logs.json", "w", encoding="utf-8") as f:
        logs = []
        for i in range(5000):
            # INV001 generuje 90% danych - idealne odwzorowanie skośności klastra
            inv_id = "INV001" if random.random() < 0.9 else f"INV{random.randint(2, 6):03d}"
            logs.append({
                "inverter_id": inv_id,
                "event_code": f"ERR_{random.randint(100, 108)}",
                "severity": "ERROR" if random.random() < 0.15 else "INFO",
                "timestamp": (datetime.now() - timedelta(seconds=i)).isoformat()
            })
        json.dump(logs, f)
        
    print("✅ [GENERATOR] Wszystkie surowe struktury plików zostały przygotowane.")