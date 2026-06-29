# src/laboratory/raw_generator.py
import os
import json
import random
from datetime import datetime, timedelta


def generate_fragmented_json_logs(target_path: str, num_files: int = 200):
    """
    Generates raw telemetry data structures to reproduce
    performance anomalies in a laboratory environment.
    """
    # Standardize base paths within the UC volume
    bess_dir = f"{target_path}/bess_telemetry_raw"
    pv_dir = f"{target_path}/pv_metrics_raw"
    inverter_dir = f"{target_path}/inverter_logs_raw"

    os.makedirs(bess_dir, exist_ok=True)
    os.makedirs(pv_dir, exist_ok=True)
    os.makedirs(inverter_dir, exist_ok=True)

    # 1. ETL-01: Small files for BESS (Generate physical fragmentation)
    print(f"[GENERATOR] Creating {num_files} micro JSON files in: {bess_dir}")
    for i in range(num_files):
        data = [
            {
                "id": random.randint(10000, 99999),
                "station_id": f"S{random.randint(1, 3):03d}",
                "battery_temp": round(random.uniform(20.0, 48.0), 2),
                "soc": round(random.uniform(5.0, 98.0), 2),
                "timestamp": (datetime.now() - timedelta(seconds=i * 5)).isoformat(),
            }
        ]
        with open(f"{bess_dir}/bess_chunk_{i}.json", "w", encoding="utf-8") as f:
            json.dump(data, f)

    # 2. ETL-02: Data for PV Metrics (Type casting scenario)
    print(f"[GENERATOR] Creating raw CSV file for PV in: {pv_dir}")
    with open(f"{pv_dir}/pv_data_mock.csv", "w", encoding="utf-8") as f:
        f.write("station_id,temperature,voltage,timestamp\n")
        for i in range(500):
            f.write(
                f"S{random.randint(1, 3):03d},{round(random.uniform(12.0, 38.0), 2)},{round(random.uniform(215.0, 245.0), 2)},{datetime.now().isoformat()}\n"
            )

    # 3. ETL-04: Inverter logs with skewness (Data Skew)
    print(f"[GENERATOR] Creating skewed JSON log file in: {inverter_dir}")
    with open(f"{inverter_dir}/inverter_logs.json", "w", encoding="utf-8") as f:
        logs = []
        for i in range(5000):
            # INV001 generates 90% of data - perfect representation of cluster skewness
            inv_id = "INV001" if random.random() < 0.9 else f"INV{random.randint(2, 6):03d}"
            logs.append(
                {
                    "inverter_id": inv_id,
                    "event_code": f"ERR_{random.randint(100, 108)}",
                    "severity": "ERROR" if random.random() < 0.15 else "INFO",
                    "timestamp": (datetime.now() - timedelta(seconds=i)).isoformat(),
                }
            )
        json.dump(logs, f)

    print("✅ [GENERATOR] All raw file structures have been prepared.")
