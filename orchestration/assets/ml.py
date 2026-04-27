# orchestration/assets/ml.py

import os
import subprocess
from dagster import asset, get_dagster_logger


@asset(
    group_name="ml",
    description="Re-entraîne XGBoost et écrit les prédictions dans ClickHouse",
    deps=["dbt_models"],
)
def ml_training():
    log    = get_dagster_logger()
    result = subprocess.run(
        ["python", "ml/train.py"],
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception("ml/train.py échoué")
    log.info("ML training terminé ✅")
    return {"returncode": result.returncode}


@asset(
    group_name="ml",
    description="Génère les explications SHAP pour toutes les zones",
    deps=["ml_training"],
)
def shap_explanations():
    log    = get_dagster_logger()
    result = subprocess.run(
        ["python", "ml/explain.py"],
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception("ml/explain.py échoué")
    log.info("SHAP terminé ✅")
    return {"returncode": result.returncode}