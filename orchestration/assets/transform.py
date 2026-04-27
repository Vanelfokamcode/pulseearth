# orchestration/assets/transform.py

import os
import subprocess
from dagster import asset, get_dagster_logger

DBT_DIR = os.path.join(os.path.dirname(__file__), "../../transform")


@asset(
    group_name="transform",
    description="Lance dbt run — matérialise stg_, int_, mart_ dans ClickHouse",
    deps=["usgs_earthquakes", "firms_wildfires", "openaq_pollution"],
)
def dbt_models():
    log = get_dagster_logger()
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    log.info(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"dbt run échoué (code {result.returncode})")
    log.info("dbt run terminé ✅")
    return {"returncode": result.returncode}


@asset(
    group_name="transform",
    description="Lance dbt test — vérifie la qualité des données",
    deps=["dbt_models"],
)
def dbt_tests():
    log = get_dagster_logger()
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    log.info(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
    if result.returncode != 0:
        log.warning("dbt test a des échecs — voir logs")
    return {"returncode": result.returncode}