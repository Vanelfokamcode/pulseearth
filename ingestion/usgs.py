# ingestion/usgs.py — version polling intelligent

import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Feed mis à jour toutes les minutes — le plus granulaire disponible
# ingestion/usgs.py — ligne à changer
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
BROKER   = os.getenv("BROKER", "localhost:9092")
TOPIC    = "earthquakes"
POLL_INTERVAL = 60  # secondes

# Déduplication en mémoire — évite d'envoyer 2x le même séisme
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


def parse_event(feature: dict) -> dict | None:
    try:
        props  = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        mag    = props.get("mag")
        if mag is None:
            return None
        return {
            "id":          feature["id"],
            "mag":         float(mag),
            "place":       props.get("place", ""),
            "time_ms":     props["time"],
            "lon":         float(coords[0]),
            "lat":         float(coords[1]),
            "depth_km":    float(coords[2]),
            "alert":       props.get("alert"),
            "tsunami":     int(props.get("tsunami", 0)),
            "sig":         int(props.get("sig", 0)),
            "status":      props.get("status", "automatic"),
            "mag_type":    props.get("magType", ""),
            "net":         props.get("net", "us"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    except (KeyError, TypeError, ValueError) as e:
        logger.warning(f"Parse error: {e}")
        return None


def poll_and_produce(producer: KafkaProducer) -> int:
    """Un cycle de polling. Retourne le nombre de nouveaux events envoyés."""
    try:
        r = requests.get(USGS_URL, timeout=10)
        r.raise_for_status()
        features = r.json().get("features", [])
    except Exception as e:
        logger.error(f"Fetch USGS échoué : {e}")
        return 0

    new_events = 0
    for feature in features:
        event_id = feature.get("id")
        if not event_id or event_id in seen_ids:
            continue  # déjà envoyé

        event = parse_event(feature)
        if event is None:
            continue

        producer.send(TOPIC, key=event["net"], value=event)
        seen_ids.add(event_id)
        new_events += 1

        logger.info(
            f"🌋 M{event['mag']:.1f} | {event['place']} "
            f"| depth {event['depth_km']}km | {event['status']}"
        )

    producer.flush()
    return new_events


def main():
    producer = build_producer()
    logger.info(f"Producer USGS démarré → topic '{TOPIC}' sur {BROKER}")
    logger.info(f"Polling toutes les {POLL_INTERVAL}s — feed 'all_hour' USGS")

    while True:
        n = poll_and_produce(producer)
        logger.info(f"Cycle terminé — {n} nouveaux events | {len(seen_ids)} IDs en mémoire")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()