# orchestration/assets/ingestion.py

import json
import os
import io
import csv
import time
import requests
from datetime import datetime, timezone

from kafka import KafkaProducer
from dagster import asset, get_dagster_logger

BROKER = os.getenv("BROKER", "localhost:9092")


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


@asset(
    group_name="ingestion",
    description="Fetch USGS earthquakes (last hour) → Redpanda topic 'earthquakes'",
)
def usgs_earthquakes():
    log      = get_dagster_logger()
    producer = build_producer()
    seen     = set()

    r = requests.get(
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        timeout=10,
    )
    r.raise_for_status()
    features = r.json().get("features", [])

    sent = 0
    for f in features:
        if f["id"] in seen:
            continue
        props  = f["properties"]
        coords = f["geometry"]["coordinates"]
        mag    = props.get("mag")
        if mag is None:
            continue

        event = {
            "id":          f["id"],
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
        producer.send("earthquakes", key=event["net"], value=event)
        seen.add(f["id"])
        sent += 1

    producer.flush()
    producer.close()
    log.info(f"USGS → {sent} séismes envoyés dans Redpanda")
    return {"sent": sent, "total_features": len(features)}


@asset(
    group_name="ingestion",
    description="Fetch NASA FIRMS wildfires (24h) → Redpanda topic 'wildfires'",
)
def firms_wildfires():
    log      = get_dagster_logger()
    api_key  = os.getenv("FIRMS_API_KEY")
    producer = build_producer()

    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{api_key}/VIIRS_NOAA20_NRT/world/1"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    reader = csv.DictReader(io.StringIO(r.text))
    sent   = 0
    for row in reader:
        if row.get("confidence", "").lower() == "low":
            continue
        try:
            event = {
                "lat":         float(row["latitude"]),
                "lon":         float(row["longitude"]),
                "brightness":  float(row["bright_ti4"]),
                "frp":         float(row["frp"]),
                "confidence":  row["confidence"].lower(),
                "acq_date":    row["acq_date"],
                "acq_time":    row["acq_time"].zfill(4),
                "satellite":   row["satellite"],
                "daynight":    row["daynight"],
                "scan":        float(row["scan"]),
                "track":       float(row["track"]),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.send("wildfires", key=event["satellite"], value=event)
            sent += 1
        except (KeyError, ValueError):
            continue

    producer.flush()
    producer.close()
    log.info(f"FIRMS → {sent} foyers envoyés dans Redpanda")
    return {"sent": sent}


@asset(
    group_name="ingestion",
    description="Fetch OpenAQ pollution (latest) → Redpanda topic 'pollution'",
)
def openaq_pollution():
    log      = get_dagster_logger()
    api_key  = os.getenv("OPENAQ_API_KEY")
    producer = build_producer()

    PARAMETERS       = {2: "pm25", 5: "no2", 3: "o3"}
    PRIORITY_COUNTRIES = {22: "France", 68: "United States", 23: "India",
                          13: "Brazil", 108: "Indonesia", 59: "Japan", 48: "Turkey"}
    sent = 0

    for country_id, country_name in PRIORITY_COUNTRIES.items():
        for param_id, param_name in PARAMETERS.items():
            try:
                r = requests.get(
                    f"https://api.openaq.org/v3/parameters/{param_id}/latest",
                    params={"countries_id": country_id, "limit": 100},
                    headers={"X-API-Key": api_key},
                    timeout=15,
                )
                r.raise_for_status()
                results = r.json().get("results", [])
            except Exception as e:
                log.warning(f"OpenAQ échoué ({country_name}/{param_name}): {e}")
                continue

            for m in results:
                try:
                    coords = m.get("coordinates") or {}
                    lat    = coords.get("latitude")
                    lon    = coords.get("longitude")
                    value  = m.get("value")
                    measured_at = (m.get("datetime") or {}).get("utc")
                    if None in (lat, lon, value, measured_at) or float(value) < 0:
                        continue
                    event = {
                        "location_id": str(m.get("locationsId", "")),
                        "sensor_id":   str(m.get("sensorsId", "")),
                        "parameter":   param_name,
                        "value":       float(value),
                        "unit":        "µg/m³",
                        "lat":         float(lat),
                        "lon":         float(lon),
                        "country":     country_name,
                        "measured_at": measured_at,
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                    producer.send("pollution", key=param_name, value=event)
                    sent += 1
                except Exception:
                    continue
            time.sleep(0.3)

    producer.flush()
    producer.close()
    log.info(f"OpenAQ → {sent} mesures envoyées dans Redpanda")
    return {"sent": sent}