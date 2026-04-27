import requests
import json
from dotenv import load_dotenv
import os
load_dotenv()

# ── 1. USGS ─────────────────────────────────────────────────────────────────
print("=== USGS — dernière heure ===")
r = requests.get(
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    timeout=10
)
data = r.json()
events = data["features"]
print(f"{len(events)} séismes dans la dernière heure")
if events:
    e = events[0]["properties"]
    coords = events[0]["geometry"]["coordinates"]
    print(f"  → Dernier : M{e['mag']} — {e['place']}")
    print(f"  → Profondeur : {coords[2]} km")
    print(f"  → Status : {e['status']}")

# ── 2. NASA FIRMS ───────────────────────────────────────────────────────────
# FIRMS nécessite une API key gratuite : https://firms.modaps.eosdis.nasa.gov/api/
# Pour l'instant on vérifie juste que l'endpoint répond
print("\n=== NASA FIRMS — endpoint check ===")
# Sans key : on lit le CSV public "world" 24h en accès limité
r2 = requests.get(
    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv",
    timeout=15,
    stream=True
)
# Juste les premières lignes
lines = []
for line in r2.iter_lines():
    lines.append(line.decode())
    if len(lines) > 3:
        break
print(f"  → Status HTTP : {r2.status_code}")
print(f"  → Colonnes : {lines[0]}")
print(f"  → Exemple : {lines[1]}")

# ── 3. OpenAQ ───────────────────────────────────────────────────────────────
r3 = requests.get(
    "https://api.openaq.org/v3/locations",
    params={"countries_id": 22, "limit": 3},  # 22 = France ✅
    headers={
        "Accept": "application/json",
        "X-API-Key": os.getenv("OPENAQ_API_KEY")
    },
    timeout=10
)
oaq = r3.json()
print(f"  → {oaq['meta']['found']} stations trouvées en France")
for loc in oaq["results"][:3]:
    print(f"  → {loc['name']} — {loc['locality']}")

print(f"  → Status HTTP : {r3.status_code}")
print(json.dumps(r3.json(), indent=2)[:800])  # les 800 premiers chars