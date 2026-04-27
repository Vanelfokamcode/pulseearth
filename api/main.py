# api/main.py

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from clickhouse_driver import Client
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

load_dotenv()


CH_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT_NATIVE", "9000"))
CH_DB       = os.getenv("CLICKHOUSE_DB", "pulseearth")
CH_USER     = os.getenv("CLICKHOUSE_USER", "pulse")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "earth2026")


# ── ClickHouse client ────────────────────────────────────────────────────────

def get_ch_client() -> Client:
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASSWORD,
    )


# Dans api/main.py, remplace la fonction query() par celle-ci
# et ajoute un helper pour les filtres temporels

def query(sql: str, params: dict = {}) -> list[dict]:
    client = get_ch_client()
    rows, columns = client.execute(sql, params, with_column_types=True)
    col_names = [c[0] for c in columns]
    result = []
    for row in rows:
        d = {}
        for i, col in enumerate(col_names):
            v = row[i]
            # Convertit les datetime Python en string ISO
            if hasattr(v, 'isoformat'):
                d[col] = v.isoformat()
            else:
                d[col] = v
        result.append(d)
    return result

# ── WebSocket Connection Manager ─────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()

# Timestamp du dernier événement pushé — pour détecter les nouveaux
_last_eq_time:  str = "1970-01-01 00:00:00"
_last_wf_time:  str = "1970-01-01 00:00:00"
_last_pol_time: str = "1970-01-01 00:00:00"


async def poll_and_broadcast():
    """Background task — poll ClickHouse toutes les 10s, push les nouveaux events."""
    global _last_eq_time, _last_wf_time, _last_pol_time

    while True:
        await asyncio.sleep(10)

        if not manager.active:
            continue  # personne connecté, inutile de requêter

        try:
            # Nouveaux séismes
            new_eq = query(
                """
                SELECT id, mag, place, lat, lon, depth_km, alert,
                       tsunami, sig, status, toString(event_time) AS event_time
                FROM earthquakes
                WHERE toDateTime(event_time) > toDateTime(%(last)s)
                ORDER BY event_time ASC
                LIMIT 50
                """,
                {"last": _last_eq_time},
            )
            for eq in new_eq:
                eq["_type"] = "earthquake"
                await manager.broadcast(eq)
            if new_eq:
                _last_eq_time = new_eq[-1]["event_time"]

            # Nouveaux feux
            new_wf = query(
                """
                SELECT lat, lon, brightness, frp, confidence,
                       satellite, daynight,
                       toString(event_time) AS event_time
                FROM wildfires
                WHERE toDateTime(event_time) > toDateTime(%(last)s)
                ORDER BY event_time ASC
                LIMIT 100
                """,
                {"last": _last_wf_time},
            )
            for wf in new_wf:
                wf["_type"] = "wildfire"
                await manager.broadcast(wf)
            if new_wf:
                _last_wf_time = new_wf[-1]["event_time"]

            # Nouvelle pollution
            new_pol = query(
                """
                SELECT location_id, parameter, value, lat, lon,
                       country, toString(event_time) AS event_time
                FROM pollution
                WHERE toDateTime(event_time) > toDateTime(%(last)s)
                ORDER BY event_time ASC
                LIMIT 100
                """,
                {"last": _last_pol_time},
            )
            for pol in new_pol:
                pol["_type"] = "pollution"
                await manager.broadcast(pol)
            if new_pol:
                _last_pol_time = new_pol[-1]["event_time"]

        except Exception as e:
            print(f"[broadcast error] {e}")


# ── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Démarre le background broadcaster au démarrage de l'app
    task = asyncio.create_task(poll_and_broadcast())
    yield
    task.cancel()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="PulseEarth API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")


@app.get("/")
def serve_globe():
    return FileResponse("frontend/index.html")


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        result = query("SELECT 1")
        ch_ok  = result[0]["1"] == 1
    except Exception:
        ch_ok = False

    return {
        "status":      "ok" if ch_ok else "degraded",
        "clickhouse":  ch_ok,
        "ws_clients":  len(manager.active),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


# ── REST — snapshot initial ───────────────────────────────────────────────────

@app.get("/earthquakes/recent")
def earthquakes_recent(hours: int = 24, min_mag: float = 0.0):
    """Séismes des dernières N heures, magnitude >= min_mag."""
    rows = query(
        """
        SELECT id, mag, place, lat, lon, depth_km, alert,
               tsunami, sig, status,
               toString(event_time) AS event_time
        FROM earthquakes
        WHERE toDateTime(event_time) >= now() - INTERVAL %(hours)s HOUR
          AND mag >= %(min_mag)s
        ORDER BY event_time DESC
        LIMIT 500
        """,
        {"hours": hours, "min_mag": min_mag},
    )
    return {"count": len(rows), "data": rows}


@app.get("/wildfires/active")
def wildfires_active(hours: int = 24):
    """Feux détectés dans les dernières N heures."""
    rows = query(
        """
        SELECT lat, lon, brightness, frp, confidence,
               satellite, daynight,
               toString(event_time) AS event_time
        FROM wildfires
        WHERE toDateTime(event_time) >= now() - INTERVAL %(hours)s HOUR
        ORDER BY frp DESC
        LIMIT 1000
        """,
        {"hours": hours},
    )
    return {"count": len(rows), "data": rows}


@app.get("/pollution/latest")
def pollution_latest(parameter: str = "pm25"):
    """Dernière mesure par station pour un paramètre donné."""
    rows = query(
        """
        SELECT location_id, parameter, value, lat, lon, country,
               toString(max(event_time)) AS event_time
        FROM pollution
        WHERE parameter = %(param)s
          AND event_time >= now() - INTERVAL 48 HOUR
        GROUP BY location_id, parameter, value, lat, lon, country
        ORDER BY value DESC
        LIMIT 500
        """,
        {"param": parameter},
    )
    return {"count": len(rows), "data": rows}


@app.get("/risk-zones")
def risk_zones():
    """Zones à risque depuis dbt mart_risk_zones."""
    rows = query(
        """
        SELECT zone_lat, zone_lon, eq_count, max_mag,
               fire_count, max_frp, avg_pm25,
               risk_score_pct, risk_level,
               toString(last_activity) AS last_activity
        FROM mart_risk_zones
        ORDER BY risk_score_pct DESC
        LIMIT 200
        """
    )
    return {"count": len(rows), "data": rows}


@app.get("/stats/global")
def global_stats():
    """Statistiques globales pour le header du globe."""
    eq_count = query(
        "SELECT count(*) AS n FROM earthquakes "
        "WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR"
    )[0]["n"]

    wf_count = query(
        "SELECT count(*) AS n FROM wildfires "
        "WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR"
    )[0]["n"]

    pol_count = query(
        "SELECT count(*) AS n FROM pollution "
        "WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR"
    )[0]["n"]

    max_mag = query(
        "SELECT max(mag) AS m FROM earthquakes "
        "WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR"
    )[0]["m"]

    return {
        "earthquakes_24h": eq_count,
        "wildfires_24h":   wf_count,
        "pollution_24h":   pol_count,
        "max_mag_24h":     max_mag,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    client_ip = ws.client.host
    print(f"[WS] Client connecté : {client_ip} — {len(manager.active)} total")

    try:
        # Envoie le snapshot initial au client qui vient de se connecter
        recent_eq = query(
            """
            SELECT id, mag, place, lat, lon, depth_km, alert,
                   toString(event_time) AS event_time
            FROM earthquakes
            WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR
            ORDER BY event_time DESC
            LIMIT 200
            """
        )
        await ws.send_json({
            "_type":  "initial_snapshot",
            "earthquakes": recent_eq,
        })

        # Garde la connexion ouverte — attend les pings du client
        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(ws)
        print(f"[WS] Client déconnecté : {client_ip} — {len(manager.active)} restants")