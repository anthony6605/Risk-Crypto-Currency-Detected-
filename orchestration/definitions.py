import subprocess
import sys
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DBT_DIR = PROJECT_ROOT / "transform"

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()

RETRY = dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL)

# The producer polls CoinGecko every 10 minutes, so the newest bar is normally up to about
# 15 minutes old. Older than 25 minutes means the producer, Kafka or the consumer has stopped.
MAX_BAR_LAG_SECONDS = 1500


# ---------------------------------------------------------------- streaming tables
# Written continuously by the Docker services, not by Dagster. Declaring them makes
# them appear in the lineage graph as the upstream of the dbt models.
stream_tables = [
    dg.AssetSpec(
        key=["riskcoin", table],
        group_name="streaming",
        description=f"public.{table}, written continuously by the {writer}.",
        kinds={"postgres"},
    )
    for table, writer in [
        ("price_bars_1m", "Kafka consumer"),
        ("live_risk", "live risk job"),
        ("pipeline_audit", "pipeline services"),
    ]
]


# ---------------------------------------------------------------- batch assets
@dg.asset(
    key=["riskcoin", "coin_catalog"],
    group_name="ingestion",
    retry_policy=RETRY,
    kinds={"python", "postgres"},
    description="Symbol to CoinGecko id mapping from the top 1,000 coins.",
)
def coin_catalog(context: dg.AssetExecutionContext):
    from src.database.connection import get_engine
    from src.database.schema import create_tables
    from src.ingestion.coingecko import CoinGeckoClient
    from src.jobs.refresh_coin_catalog import run_once

    engine = get_engine()
    create_tables(engine)
    run_once(engine, CoinGeckoClient())
    with engine.connect() as connection:
        symbols = connection.execute(text("SELECT COUNT(*) FROM coin_catalog")).scalar_one()
    return dg.MaterializeResult(metadata={"symbols": symbols})


@dg.asset(
    group_name="ingestion",
    retry_policy=RETRY,
    kinds={"python"},
    description="CoinGecko history for the top coins (main.py, watermark-based).",
)
def coingecko_history(context: dg.AssetExecutionContext):
    result = subprocess.run(
        [sys.executable, "main.py"], cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    context.log.info(result.stdout[-5000:])
    if result.returncode != 0:
        raise dg.Failure(
            description="main.py exited with an error",
            metadata={"stderr": dg.MetadataValue.text(result.stderr[-5000:])},
        )


@dbt_assets(manifest=dbt_project.manifest_path)
def riskcoin_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ---------------------------------------------------------------- checks
@dg.asset_check(asset=dg.AssetKey(["riskcoin", "price_bars_1m"]), blocking=False)
def price_bars_are_fresh():
    from src.database.connection import get_engine

    with get_engine().connect() as connection:
        lag = connection.execute(
            text("SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - MAX(bar_start)) FROM price_bars_1m")
        ).scalar()
    lag = None if lag is None else float(lag)
    return dg.AssetCheckResult(
        passed=lag is not None and lag < MAX_BAR_LAG_SECONDS,
        metadata={"lag_seconds": lag if lag is not None else -1.0},
    )


# ---------------------------------------------------------------- jobs and schedules
catalog_job = dg.define_asset_job(
    "daily_catalog",
    selection=dg.AssetSelection.assets(coin_catalog).downstream(),
)
dbt_job = dg.define_asset_job("hourly_dbt", selection=dg.AssetSelection.assets(riskcoin_dbt))
history_job = dg.define_asset_job("history_backfill", selection=dg.AssetSelection.assets(coingecko_history))
health_job = dg.define_asset_job("stream_health", selection=dg.AssetSelection.checks(price_bars_are_fresh))

schedules = [
    dg.ScheduleDefinition(job=catalog_job, cron_schedule="15 3 * * *", execution_timezone="UTC"),
    dg.ScheduleDefinition(job=dbt_job, cron_schedule="10 * * * *", execution_timezone="UTC"),
    # Once a day: each run makes about 100 CoinGecko calls (one per coin), and the free
    # plan's 10,000 calls a month are shared with the producer's polling
    dg.ScheduleDefinition(job=history_job, cron_schedule="5 4 * * *", execution_timezone="UTC"),
    dg.ScheduleDefinition(job=health_job, cron_schedule="*/15 * * * *", execution_timezone="UTC"),
]

defs = dg.Definitions(
    assets=[*stream_tables, coin_catalog, coingecko_history, riskcoin_dbt],
    asset_checks=[price_bars_are_fresh],
    jobs=[catalog_job, dbt_job, history_job, health_job],
    schedules=schedules,
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
