# ml/train.py

import os
import json
import joblib
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone

from clickhouse_driver import Client
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, roc_auc_score,
    precision_recall_curve, average_precision_score
)
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

warnings.filterwarnings("ignore")
load_dotenv()

CH_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT_NATIVE", "9000"))
CH_DB       = os.getenv("CLICKHOUSE_DB", "pulseearth")
CH_USER     = os.getenv("CLICKHOUSE_USER", "pulse")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "earth2026")

MODEL_PATH  = "ml/model.joblib"
META_PATH   = "ml/model_meta.json"


def get_client() -> Client:
    return Client(
        host=CH_HOST, port=CH_PORT,
        database=CH_DB, user=CH_USER, password=CH_PASSWORD,
    )


def fetch_features() -> pd.DataFrame:
    """Construit le dataset depuis ClickHouse."""
    client = get_client()

    print("Chargement features sismiques...")
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

    print("Chargement features feux...")
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

    print("Chargement features pollution...")
    pol_rows, pol_cols = client.execute("""
        SELECT
            round(lat)              AS zone_lat,
            round(lon)              AS zone_lon,
            avgIf(value, parameter = 'pm25') AS avg_pm25_48h,
            maxIf(value, parameter = 'pm25') AS max_pm25_48h,
            avgIf(value, parameter = 'no2')  AS avg_no2_48h
        FROM pollution
        WHERE toDateTime(event_time) >= now() - INTERVAL 48 HOUR
        GROUP BY zone_lat, zone_lon
    """, with_column_types=True)
    df_pol = pd.DataFrame(pol_rows, columns=[c[0] for c in pol_cols])

    print("Construction labels...")
    # Label : séisme M4+ OU feu FRP>100 dans les 24h
    # On utilise les données récentes comme proxy du "futur"
    label_rows, label_cols = client.execute("""
        SELECT
            round(lat) AS zone_lat,
            round(lon) AS zone_lon,
            1          AS significant_event
        FROM earthquakes
        WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR
          AND mag >= 4.0
        UNION ALL
        SELECT
            round(lat) AS zone_lat,
            round(lon) AS zone_lon,
            1          AS significant_event
        FROM wildfires
        WHERE toDateTime(event_time) >= now() - INTERVAL 24 HOUR
          AND frp > 100
    """, with_column_types=True)
    df_labels = pd.DataFrame(
        label_rows, columns=[c[0] for c in label_cols]
    ).drop_duplicates(["zone_lat", "zone_lon"])

    # Merge
    print("Merge des features...")
    df = df_eq.merge(df_wf,  on=["zone_lat", "zone_lon"], how="left")
    df = df.merge(df_pol,    on=["zone_lat", "zone_lon"], how="left")
    df = df.merge(df_labels, on=["zone_lat", "zone_lon"], how="left")

    # Label : 1 si événement significatif, 0 sinon
    df["significant_event"] = df["significant_event"].fillna(0).astype(int)

    # Features géo
    df["abs_lat"] = df["zone_lat"].abs()

    # Fill NaN
    df = df.fillna(0)

    print(f"Dataset : {len(df)} zones | {df['significant_event'].sum()} zones avec événement significatif")
    return df


FEATURES = [
    "zone_lat", "zone_lon", "abs_lat",
    "eq_count_7d", "max_mag_7d", "avg_mag_7d",
    "shallow_eq_ratio", "hours_since_last_eq",
    "fire_count_7d", "max_frp_7d", "avg_frp_7d",
    "avg_pm25_48h", "max_pm25_48h", "avg_no2_48h",
]


def train(df: pd.DataFrame):
    X = df[FEATURES]
    y = df["significant_event"]

    print(f"\nDistribution labels : {y.value_counts().to_dict()}")

    # Split temporel simulé — 80/20
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.sum() > 5 else None
    )

    # Gestion du déséquilibre de classes
    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    scale_pos = neg / pos if pos > 0 else 1
    print(f"Scale pos weight : {scale_pos:.2f}")

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos,   # compense le déséquilibre
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # Évaluation
    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("\n── Métriques ───────────────────────────────")
    print(classification_report(y_test, y_pred, zero_division=0))

    if y_test.sum() > 0:
        auc = roc_auc_score(y_test, y_proba)
        ap  = average_precision_score(y_test, y_proba)
        print(f"ROC-AUC          : {auc:.4f}")
        print(f"Average Precision: {ap:.4f}")

    # Feature importance
    importance = dict(zip(FEATURES, model.feature_importances_))
    importance = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    print("\n── Feature importance (top 5) ──────────────")
    for feat, score in list(importance.items())[:5]:
        bar = "█" * int(score * 50)
        print(f"  {feat:<25} {bar} {score:.4f}")

    return model, importance


def predict_all_zones(model, df: pd.DataFrame) -> pd.DataFrame:
    """Génère les scores de risque ML pour toutes les zones."""
    X = df[FEATURES]
    df = df.copy()
    df["ml_risk_score"]    = model.predict_proba(X)[:, 1]
    df["ml_risk_label"]    = df["ml_risk_score"].apply(
        lambda s: "critical" if s > 0.7 else "high" if s > 0.4 else "moderate" if s > 0.2 else "low"
    )
    df["predicted_at"] = datetime.now(timezone.utc).isoformat()
    return df[["zone_lat", "zone_lon", "ml_risk_score", "ml_risk_label", "predicted_at"]]


def save_predictions_to_clickhouse(predictions: pd.DataFrame):
    """Écrit les prédictions dans ClickHouse."""
    client = get_client()

    # Crée la table si elle n'existe pas
    client.execute("""
        CREATE TABLE IF NOT EXISTS ml_risk_predictions (
            zone_lat       Float32,
            zone_lon       Float32,
            ml_risk_score  Float32,
            ml_risk_label  String,
            predicted_at   String
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (zone_lat, zone_lon)
    """)

    rows = list(predictions.itertuples(index=False, name=None))
    client.execute(
        "INSERT INTO ml_risk_predictions VALUES",
        rows,
    )
    print(f"\n✅ {len(rows)} prédictions écrites dans ClickHouse")


def main():
    print("═" * 50)
    print("PulseEarth — ML Training Pipeline")
    print("═" * 50)

    df = fetch_features()

    if len(df) < 10:
        print("⚠️  Pas assez de données pour entraîner (< 10 zones).")
        print("   Lance les producers quelques minutes et réessaie.")
        return

    model, importance = train(df)

    # Sauvegarde du modèle
    os.makedirs("ml", exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"\n✅ Modèle sauvegardé → {MODEL_PATH}")

    # Métadonnées
    # Dans main(), remplace la section métadonnées par :
    meta = {
        "trained_at":       datetime.now(timezone.utc).isoformat(),
        "n_zones":          int(len(df)),
        "n_features":       len(FEATURES),
        "features":         FEATURES,
        "feature_importance": {k: float(v) for k, v in importance.items()},
        "positive_rate":    float(df["significant_event"].mean()),
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"✅ Métadonnées sauvegardées → {META_PATH}")

    # Prédictions sur toutes les zones
    predictions = predict_all_zones(model, df)
    save_predictions_to_clickhouse(predictions)

    print("\n── Top zones à risque ──────────────────────")
    top = predictions.sort_values("ml_risk_score", ascending=False).head(5)
    for _, row in top.iterrows():
        print(
            f"  {row['zone_lat']:+.0f}°N {row['zone_lon']:+.0f}°E  "
            f"→ {row['ml_risk_label'].upper()} ({row['ml_risk_score']:.3f})"
        )


if __name__ == "__main__":
    main()