# ml/explain.py

import os
import json
import joblib
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # pas de display GUI — sauvegarde en fichier
import matplotlib.pyplot as plt
import shap

from clickhouse_driver import Client
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

CH_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT_NATIVE", "9000"))
CH_DB       = os.getenv("CLICKHOUSE_DB", "pulseearth")
CH_USER     = os.getenv("CLICKHOUSE_USER", "pulse")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "earth2026")

MODEL_PATH  = "ml/model.joblib"
META_PATH   = "ml/model_meta.json"
OUTPUT_DIR  = "ml/shap_plots"

FEATURES = [
    "zone_lat", "zone_lon", "abs_lat",
    "eq_count_7d", "max_mag_7d", "avg_mag_7d",
    "shallow_eq_ratio", "hours_since_last_eq",
    "fire_count_7d", "max_frp_7d", "avg_frp_7d",
    "avg_pm25_48h", "max_pm25_48h", "avg_no2_48h",
]


def get_client() -> Client:
    return Client(
        host=CH_HOST, port=CH_PORT,
        database=CH_DB, user=CH_USER, password=CH_PASSWORD,
    )


def fetch_features() -> pd.DataFrame:
    """Même query que train.py — reconstruit le dataset."""
    client = get_client()

    eq_rows, eq_cols = client.execute("""
        SELECT
            round(lat)                          AS zone_lat,
            round(lon)                          AS zone_lon,
            count(*)                            AS eq_count_7d,
            max(mag)                            AS max_mag_7d,
            avg(mag)                            AS avg_mag_7d,
            countIf(depth_km < 20) / count(*)  AS shallow_eq_ratio,
            dateDiff('hour', max(toDateTime(event_time)), now()) AS hours_since_last_eq
        FROM earthquakes
        WHERE toDateTime(event_time) >= now() - INTERVAL 7 DAY
        GROUP BY zone_lat, zone_lon
    """, with_column_types=True)
    df_eq = pd.DataFrame(eq_rows, columns=[c[0] for c in eq_cols])

    wf_rows, wf_cols = client.execute("""
        SELECT
            round(lat)  AS zone_lat,
            round(lon)  AS zone_lon,
            count(*)    AS fire_count_7d,
            max(frp)    AS max_frp_7d,
            avg(frp)    AS avg_frp_7d
        FROM wildfires
        WHERE toDateTime(event_time) >= now() - INTERVAL 7 DAY
        GROUP BY zone_lat, zone_lon
    """, with_column_types=True)
    df_wf = pd.DataFrame(wf_rows, columns=[c[0] for c in wf_cols])

    pol_rows, pol_cols = client.execute("""
        SELECT
            round(lat)                           AS zone_lat,
            round(lon)                           AS zone_lon,
            avgIf(value, parameter = 'pm25')     AS avg_pm25_48h,
            maxIf(value, parameter = 'pm25')     AS max_pm25_48h,
            avgIf(value, parameter = 'no2')      AS avg_no2_48h
        FROM pollution
        WHERE toDateTime(event_time) >= now() - INTERVAL 48 HOUR
        GROUP BY zone_lat, zone_lon
    """, with_column_types=True)
    df_pol = pd.DataFrame(pol_rows, columns=[c[0] for c in pol_cols])

    df = df_eq.merge(df_wf,  on=["zone_lat", "zone_lon"], how="left")
    df = df.merge(df_pol,    on=["zone_lat", "zone_lon"], how="left")
    df["abs_lat"] = df["zone_lat"].abs()
    df = df.fillna(0)
    return df


def explain(model, df: pd.DataFrame):
    X = df[FEATURES]
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Calcul des valeurs SHAP...")
    explainer   = shap.TreeExplainer(model)
    shap_values = explainer(X)

    # ── 1. Beeswarm plot — vue globale ───────────────────────────────────────
    print("Génération beeswarm plot...")
    plt.figure(figsize=(10, 6))
    shap.plots.beeswarm(shap_values, show=False, max_display=14)
    plt.title("PulseEarth — SHAP Feature Impact (toutes zones)", pad=12)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/beeswarm.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUTPUT_DIR}/beeswarm.png")

    # ── 2. Bar plot — importance globale ─────────────────────────────────────
    print("Génération bar plot...")
    plt.figure(figsize=(8, 5))
    shap.plots.bar(shap_values, show=False, max_display=14)
    plt.title("PulseEarth — SHAP Feature Importance (mean |SHAP|)", pad=12)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/importance_bar.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUTPUT_DIR}/importance_bar.png")

    # ── 3. Waterfall plots — top 3 zones à risque ────────────────────────────
    print("Génération waterfall plots (top 3 zones)...")

    # Récupère les prédictions pour identifier le top 3
    proba       = model.predict_proba(X)[:, 1]
    df          = df.copy()
    df["score"] = proba
    top3_idx    = df.nlargest(3, "score").index.tolist()

    for rank, idx in enumerate(top3_idx):
        zone_lat = df.loc[idx, "zone_lat"]
        zone_lon = df.loc[idx, "zone_lon"]
        score    = df.loc[idx, "score"]

        plt.figure(figsize=(9, 5))
        shap.plots.waterfall(shap_values[idx], show=False, max_display=10)
        plt.title(
            f"Zone {zone_lat:+.0f}°N {zone_lon:+.0f}°E — "
            f"Risk Score: {score:.3f}",
            pad=12
        )
        plt.tight_layout()
        fname = f"{OUTPUT_DIR}/waterfall_rank{rank+1}.png"
        plt.savefig(fname, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  → {fname}  (score={score:.3f})")

    # ── 4. Export JSON pour l'API ─────────────────────────────────────────────
    print("\nExport SHAP JSON pour FastAPI...")
    shap_json = []
    for i, row in df.iterrows():
        sv   = shap_values[i].values.tolist()
        base = float(shap_values[i].base_values)
        shap_json.append({
            "zone_lat":    float(row["zone_lat"]),
            "zone_lon":    float(row["zone_lon"]),
            "score":       float(row["score"]),
            "base_value":  base,
            "shap_values": {feat: float(sv[j]) for j, feat in enumerate(FEATURES)},
            "top_drivers": sorted(
                {feat: float(sv[j]) for j, feat in enumerate(FEATURES)}.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )[:3],
        })

    with open("ml/shap_explanations.json", "w") as f:
        json.dump(shap_json, f, indent=2)
    print(f"  → ml/shap_explanations.json ({len(shap_json)} zones)")

    # ── 5. Summary console ────────────────────────────────────────────────────
    print("\n── SHAP Top drivers par zone (top 3) ──────────────────────────")
    for entry in sorted(shap_json, key=lambda x: x["score"], reverse=True)[:3]:
        print(f"\n  Zone {entry['zone_lat']:+.0f}°N {entry['zone_lon']:+.0f}°E "
              f"— score {entry['score']:.3f}")
        for feat, val in entry["top_drivers"]:
            direction = "↑" if val > 0 else "↓"
            print(f"    {direction} {feat:<25} SHAP={val:+.4f}")


def main():
    print("═" * 50)
    print("PulseEarth — SHAP Explanations")
    print("═" * 50)

    if not os.path.exists(MODEL_PATH):
        print(f"❌ Modèle introuvable : {MODEL_PATH}")
        print("   Lance d'abord : python ml/train.py")
        return

    model = joblib.load(MODEL_PATH)
    print(f"✅ Modèle chargé : {MODEL_PATH}")

    df = fetch_features()
    print(f"✅ Dataset : {len(df)} zones\n")

    explain(model, df)
    print("\n✅ SHAP terminé — plots dans ml/shap_plots/")


if __name__ == "__main__":
    main()