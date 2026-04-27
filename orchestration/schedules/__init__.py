# orchestration/schedules/__init__.py

from dagster import ScheduleDefinition
from orchestration.jobs import (
    ingestion_job,
    transform_job,
    ml_job,
)

# USGS toutes les heures
usgs_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 * * * *",
    name="hourly_ingestion",
)

# dbt toutes les heures (après ingestion)
transform_schedule = ScheduleDefinition(
    job=transform_job,
    cron_schedule="30 * * * *",
    name="hourly_transform",
)

# ML re-training tous les jours à 3h du matin
ml_schedule = ScheduleDefinition(
    job=ml_job,
    cron_schedule="0 3 * * *",
    name="daily_ml_training",
)