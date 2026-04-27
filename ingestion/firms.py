# ingestion/firms.py

import csv
import io
import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

FIRMS_API_KEY  = os.getenv("FIRMS_API_KEY")
BROKER         = os.getenv("BROKER", "localhost:9092")
TOPIC          = "wildfires"
POLL_INTERVAL  = 600  # 10 minutes

# Source VIIRS NOAA-20 — meilleure résolution (375m), données NRT
# "1" = dernières 24h
FIRMS_URL = (
    f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
    f"{FIRMS_API_KEY}/VIIRS_NOAA20_NRT/world/1"
)

# Déduplication en mémoire
seen_ids: set[str] = set()


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=100,
    )


def dedup_key(row: dict) -> str:
    """Clé unique par détection : position + heure + satellite."""
    return f"{row['latitude']}_{row['longitude']}_{row['acq_date']}_{row['acq_time']}_{row['satellite']}"


def parse_row(row: dict) -> dict | None:
    """Convertit une ligne CSV FIRMS en event propre."""
    try:
        confidence = row.get("confidence", "nominal").lower()
        # On skip les détections de faible confiance
        if confidence == "low":
            return None

        return {
            "lat":         float(row["latitude"]),
            "lon":         float(row["longitude"]),
            "brightness":  float(row["bright_ti4"]),   # Kelvin
            "frp":         float(row["frp"]),           # MW
            "confidence":  confidence,                  # "nominal" | "high"
            "acq_date":    row["acq_date"],             # "2026-04-26"
            "acq_time":    row["acq_time"].zfill(4),   # "0048"
            "satellite":   row["satellite"],            # "N" | "A" | "T"
            "daynight":    row["daynight"],             # "D" | "N"
            "scan":        float(row["scan"]),          # taille pixel km
            "track":       float(row["track"]),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    except (KeyError, ValueError) as e:
        logger.warning(f"Parse error: {e} — row: {row}")
        return None


def poll_and_produce(producer: KafkaProducer) -> int:
    """Un cycle de polling FIRMS. Retourne le nombre de nouveaux events."""
    if not FIRMS_API_KEY:
        logger.error("FIRMS_API_KEY manquante dans .env")
        return 0

    try:
        logger.info(f"Fetch FIRMS global (VIIRS NOAA-20, 24h)...")
        r = requests.get(FIRMS_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Fetch FIRMS échoué : {e}")
        return 0

    # Parse CSV
    reader = csv.DictReader(io.StringIO(r.text))
    new_events = 0

    for row in reader:
        key = dedup_key(row)
        if key in seen_ids:
            continue

        event = parse_row(row)
        if event is None:
            continue

        # Clé de partition = satellite source
        producer.send(TOPIC, key=event["satellite"], value=event)
        seen_ids.add(key)
        new_events += 1

    producer.flush()
    return new_events


def main():
    producer = build_producer()
    logger.info(f"Producer FIRMS démarré → topic '{TOPIC}' sur {BROKER}")
    logger.info(f"Polling toutes les {POLL_INTERVAL // 60} minutes")

    while True:
        n = poll_and_produce(producer)
        logger.info(
            f"Cycle terminé — {n} nouveaux foyers | "
            f"{len(seen_ids)} détections en mémoire"
        )
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()