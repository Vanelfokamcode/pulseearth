# infra/init_clickhouse.py

import requests
import os
from dotenv import load_dotenv

load_dotenv()

CH_URL      = f"http://{os.getenv('CLICKHOUSE_HOST', 'localhost')}:{os.getenv('CLICKHOUSE_PORT', '8123')}/"
CH_USER     = os.getenv("CLICKHOUSE_USER", "pulse")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "earth2026")
CH_DB       = os.getenv("CLICKHOUSE_DB", "pulseearth")


def execute(sql: str):
    r = requests.post(
        CH_URL,
        params={"user": CH_USER, "password": CH_PASSWORD},
        data=sql.encode(),
        timeout=10,
    )
    if r.status_code != 200:
        print(f"❌ {r.text[:200]}")
    else:
        print(f"✅ OK")


TABLES = {
    "earthquakes": f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.earthquakes (
            id          String,
            mag         Float32,
            place       String,
            time_ms     Int64,
            lon         Float32,
            lat         Float32,
            depth_km    Float32,
            alert       Nullable(String),
            tsunami     Int32,
            sig         Int32,
            status      String,
            mag_type    String,
            net         String,
            ingested_at String,
            event_time  DateTime
        ) ENGINE = MergeTree()
        ORDER BY (event_time, id)
    """,

    "wildfires": f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.wildfires (
            lat         Float32,
            lon         Float32,
            brightness  Float32,
            frp         Float32,
            confidence  String,
            acq_date    String,
            acq_time    String,
            satellite   String,
            daynight    String,
            scan        Float32,
            track       Float32,
            ingested_at String,
            event_time  DateTime
        ) ENGINE = MergeTree()
        ORDER BY (event_time, lat, lon)
    """,

    "pollution": f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.pollution (
            location_id String,
            sensor_id   String,
            parameter   String,
            value       Float32,
            unit        String,
            lat         Float32,
            lon         Float32,
            country     String,
            measured_at String,
            ingested_at String,
            event_time  DateTime
        ) ENGINE = MergeTree()
        ORDER BY (event_time, parameter, location_id)
    """,
}

if __name__ == "__main__":
    for table, sql in TABLES.items():
        print(f"Création table {table}...", end=" ")
        execute(sql.strip())