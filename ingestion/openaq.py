# ingestion/openaq.py

import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
BROKER         = os.getenv("BROKER", "localhost:9092")
TOPIC          = "pollution"
POLL_INTERVAL  = 3600  # 1 heure

PARAMETERS = {
    2: "pm25",
    5: "no2",
    3: "o3",
}

PRIORITY_COUNTRIES = {
    22:  "France",
    68:  "United States",
    23:  "India",
    13:  "Brazil",
    108: "Indonesia",
    59:  "Japan",
    48:  "Turkey",
}

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


def fetch_latest_by_parameter(parameter_id: int, country_id: int) -> list[dict]:
    """Récupère les dernières mesures via /v3/parameters/{id}/latest."""
    try:
        r = requests.get(
            f"https://api.openaq.org/v3/parameters/{parameter_id}/latest",
            params={
                "countries_id": country_id,
                "limit": 100,
            },
            headers={
                "Accept": "application/json",
                "X-API-Key": OPENAQ_API_KEY,
            },
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception as e:
        logger.warning(f"Fetch OpenAQ échoué (country={country_id}, param={parameter_id}): {e}")
        return []


def parse_measurement(m: dict, param_name: str, country_name: str) -> dict | None:
    try:
        value = m.get("value")
        if value is None or float(value) < 0:
            return None

        coords = m.get("coordinates") or {}
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        if lat is None or lon is None:
            return None

        measured_at = (m.get("datetime") or {}).get("utc")
        if not measured_at:
            return None

        sensors_id = m.get("sensorsId", "unknown")
        dedup_key  = f"{sensors_id}_{param_name}_{measured_at}"

        return {
            "_dedup_key":  dedup_key,
            "location_id": str(m.get("locationsId", "")),
            "sensor_id":   str(sensors_id),
            "parameter":   param_name,
            "value":       float(value),
            "unit":        "µg/m³",
            "lat":         float(lat),
            "lon":         float(lon),
            "country":     country_name,
            "measured_at": measured_at,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    except (KeyError, TypeError, ValueError) as e:
        logger.warning(f"Parse error: {e}")
        return None


def poll_and_produce(producer: KafkaProducer) -> int:
    new_events = 0

    for country_id, country_name in PRIORITY_COUNTRIES.items():
        for param_id, param_name in PARAMETERS.items():
            results = fetch_latest_by_parameter(param_id, country_id)

            for m in results:
                event = parse_measurement(m, param_name, country_name)
                if event is None:
                    continue

                dedup_key = event.pop("_dedup_key")
                if dedup_key in seen_ids:
                    continue

                producer.send(TOPIC, key=param_name, value=event)
                seen_ids.add(dedup_key)
                new_events += 1

            time.sleep(0.3)

    producer.flush()
    return new_events


def main():
    producer = build_producer()
    logger.info(f"Producer OpenAQ démarré → topic '{TOPIC}' sur {BROKER}")
    logger.info(
        f"Polling toutes les {POLL_INTERVAL // 60} minutes | "
        f"{len(PRIORITY_COUNTRIES)} pays | {len(PARAMETERS)} paramètres"
    )

    while True:
        n = poll_and_produce(producer)
        logger.info(
            f"Cycle terminé — {n} nouvelles mesures | "
            f"{len(seen_ids)} mesures en mémoire"
        )
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()