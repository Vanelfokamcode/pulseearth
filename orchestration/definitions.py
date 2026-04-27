# orchestration/definitions.py

from dagster import Definitions, load_assets_from_modules

from orchestration.assets import ingestion, transform, ml
from orchestration.jobs import (
    ingestion_job, transform_job, ml_job, full_pipeline_job
)
from orchestration.schedules import (
    usgs_schedule, transform_schedule, ml_schedule
)

all_assets = load_assets_from_modules([ingestion, transform, ml])

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job, transform_job, ml_job, full_pipeline_job],
    schedules=[usgs_schedule, transform_schedule, ml_schedule],
)