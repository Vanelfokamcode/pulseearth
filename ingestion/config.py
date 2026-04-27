# ingestion/config.py

BROKER = "localhost:9092"

TOPICS = {
    "earthquakes": "earthquakes",
    "wildfires":   "wildfires",
    "pollution":   "pollution",
}

# Schémas attendus — documentation vivante des events
EARTHQUAKE_SCHEMA = {
    "id":        str,   # ex: "us7000abc1"
    "mag":       float, # magnitude Richter
    "place":     str,   # ex: "24 km NNE of Ivanof Bay, Alaska"
    "time":      int,   # epoch ms
    "lon":       float,
    "lat":       float,
    "depth_km":  float, # profondeur en km — feature ML critique
    "status":    str,   # "automatic" | "reviewed"
    "alert":     str,   # "green" | "yellow" | "orange" | "red" | None
}

WILDFIRE_SCHEMA = {
    "lat":        float,
    "lon":        float,
    "brightness": float, # bright_ti4 en Kelvin
    "frp":        float, # Fire Radiative Power en MW
    "confidence": str,   # "low" | "nominal" | "high"
    "acq_date":   str,   # "2026-04-26"
    "acq_time":   str,   # "0048"
    "satellite":  str,   # "N" (NOAA) | "T" (Terra) | "A" (Aqua)
    "daynight":   str,   # "D" | "N"
}

POLLUTION_SCHEMA = {
    "station_id":  int,
    "station_name": str,
    "lat":         float,
    "lon":         float,
    "parameter":   str,   # "pm25" | "no2" | "o3"
    "value":       float, # µg/m³
    "unit":        str,
    "timestamp":   str,   # ISO 8601
    "country_code": str,
}