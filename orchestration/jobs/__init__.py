# orchestration/jobs/__init__.py

from dagster import define_asset_job, AssetSelection

ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.groups("ingestion"),
)

transform_job = define_asset_job(
    name="transform_job",
    selection=AssetSelection.groups("transform"),
)

ml_job = define_asset_job(
    name="ml_job",
    selection=AssetSelection.groups("ml"),
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
)