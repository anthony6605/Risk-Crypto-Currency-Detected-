import time
from datetime import datetime, timezone

import pandas as pd

from config.configs import (
    COINGECKO_BRONZE_DIR,
    COINGECKO_HISTORY_BRONZE_DIR,
    CURRENT_RISK_GOLD_DIR,
    HISTORY_SILVER_DIR,
    MARKET_SILVER_DIR,
    RISK_GOLD_DIR,
    RISK_HISTORY_GOLD_DIR,
    create_directories,
)
from src.config.coins import (
    get_tracked_coins,
)
from src.database.connection import (
    get_engine,
)
from src.database.control import (
    close_abandoned_audits,
    finish_audit,
    get_watermark,
    start_audit,
    update_watermark,
)
from src.database.loaders import (
    upsert_current_risk,
)
from src.database.schema import (
    create_tables,
)
from src.ingestion.coingecko import (
    CoinGeckoClient,
)
from src.pipeline.incremental import (
    compute_extraction_window,
    keep_new_rows,
)
from src.quality.feature_checks import (
    validate_risk_features,
)
from src.quality.history_checks import (
    validate_market_history,
)
from src.quality.market_checks import (
    validate_market_data,
)
from src.storage.local_storage import (
    merge_into_silver,
    write_json,
    write_parquet,
)
from src.transformation.features import (
    engineer_risk_features,
)
from src.transformation.history import (
    transform_market_history,
)
from src.transformation.market import (
    transform_market_data,
)
from src.transformation.risk_score import (
    calculate_risk_score,
)

# CoinGecko's free Demo plan allows about 30 requests per minute.
# If an audit row ever shows "429 Too Many Requests", raise this.
REQUEST_DELAY_SECONDS = 2.5


def get_silver_history_path(coin_id):
    # One Silver file per coin, appended to on every run
    return HISTORY_SILVER_DIR / coin_id / "history.parquet"


def main():

    create_directories()

    engine = get_engine()

    create_tables(
        engine
    )

    # Runs killed mid-way (Mac asleep, Ctrl+C) leave audit rows at RUNNING
    abandoned = close_abandoned_audits(
        engine
    )

    if abandoned:
        print(
            f"Closed {abandoned} abandoned audit row(s)."
        )

    print(
        "PostgreSQL connection ready."
    )

    print("Starting CoinGecko ingestion...")

    client = CoinGeckoClient()

    # ==================================================
    # CURRENT MARKET DATA
    # ==================================================

    # EXTRACT

    market_data = client.get_market_data(
        currency="usd",
        limit=100,
    )

    print(
        f"Extracted {len(market_data)} assets."
    )

    

    bronze_path = write_json(
        data=market_data,
        directory=COINGECKO_BRONZE_DIR,
        prefix="market",
    )

    print(
        f"Bronze written: {bronze_path}"
    )

    

    silver_df = transform_market_data(
        market_data
    )

    

    validate_market_data(
        silver_df
    )

    print(
        "Current market data quality checks passed."
    )

    

    timestamp = (
        datetime
        .now(timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
    )

    

    silver_path = write_parquet(
        df=silver_df,
        directory=MARKET_SILVER_DIR,
        filename=f"market_{timestamp}.parquet",
    )

    print(
        f"Silver written: {silver_path}"
    )

   
    # The coin list lives in config/coins.yaml
    coins = get_tracked_coins(
        client
    )

    print(
        f"\nTracking {len(coins)} coins"
    )

    failed_coins = []

    # ==================================================
    # INCREMENTAL HISTORY EXTRACTION
    # ==================================================

    for index, coin_id in enumerate(coins):

        # Pause between coins to stay under the rate limit. This sits at the
        # top of the loop because the SKIPPED path below uses `continue`,
        # which would jump over a pause placed at the end.
        if index > 0:
            time.sleep(
                REQUEST_DELAY_SECONDS
            )

        print(
            f"\nProcessing historical data for {coin_id}..."
        )

        audit_id = start_audit(
            engine,
            coin_id,
        )

        try:

            # WATERMARK + EXTRACTION WINDOW

            watermark = get_watermark(
                engine,
                coin_id,
            )

            start, end = compute_extraction_window(
                watermark
            )

            print(
                f"[{coin_id}] watermark={watermark}  "
                f"window={start:%Y-%m-%d %H:%M} -> "
                f"{end:%Y-%m-%d %H:%M}"
            )

            # EXTRACT

            history_data = client.get_market_history_range(
                coin_id=coin_id,
                currency="usd",
                start=start,
                end=end,
            )

            # BRONZE

            bronze_history_path = write_json(
                data=history_data,
                directory=(
                    COINGECKO_HISTORY_BRONZE_DIR
                    / coin_id
                ),
                prefix="history",
            )

            print(
                f"Historical Bronze written: "
                f"{bronze_history_path}"
            )

            # TRANSFORM + QUALITY

            history_df = transform_market_history(
                data=history_data,
                coin_id=coin_id,
            )

            validate_market_history(
                history_df
            )

            rows_extracted = len(history_df)

            # KEEP ONLY ROWS NEWER THAN THE WATERMARK

            new_rows = keep_new_rows(
                history_df,
                watermark,
            )

            if new_rows.empty:

                finish_audit(
                    engine,
                    audit_id,
                    "SKIPPED",
                    rows_extracted,
                    0,
                )

                print(
                    f"[{coin_id}] no new rows"
                )

                continue

            # SILVER: APPEND + DE-DUPLICATE

            silver_history_path = get_silver_history_path(
                coin_id
            )

            rows_loaded = merge_into_silver(
                new_df=new_rows,
                silver_path=silver_history_path,
            )

            print(
                f"Historical Silver merged: "
                f"{silver_history_path}"
            )

            # MOVE THE WATERMARK ONLY AFTER SILVER IS WRITTEN

            new_watermark = (
                new_rows["timestamp"]
                .max()
                .to_pydatetime()
            )

            update_watermark(
                engine,
                coin_id,
                new_watermark,
            )

            finish_audit(
                engine,
                audit_id,
                "SUCCESS",
                rows_extracted,
                rows_loaded,
            )

            print(
                f"[{coin_id}] extracted={rows_extracted} "
                f"loaded={rows_loaded} "
                f"watermark -> {new_watermark}"
            )

        except Exception as exc:

            finish_audit(
                engine,
                audit_id,
                "FAILED",
                error_message=str(exc)[:2000],
            )

            failed_coins.append(
                coin_id
            )

            print(
                f"[{coin_id}] FAILED: {exc}"
            )

    # ==================================================
    # GOLD: REBUILT FROM THE FULL SILVER HISTORY
    # ==================================================

    latest_feature_rows = []

    for coin_id in coins:

        silver_history_path = get_silver_history_path(
            coin_id
        )

        if not silver_history_path.exists():

            print(
                f"\n{coin_id}: no Silver history yet, "
                f"skipping Gold."
            )

            continue

        history_df = pd.read_parquet(
            silver_history_path
        )

        validate_market_history(
            history_df
        )

        print(
            f"\n{coin_id}: "
            f"{len(history_df)} "
            f"Silver history rows validated."
        )

        feature_df = engineer_risk_features(
            history_df
        )

        validate_risk_features(
            feature_df
        )

        print(
            f"{coin_id}: "
            f"risk features calculated."
        )

        gold_path = write_parquet(
            df=feature_df,
            directory=(
                RISK_GOLD_DIR
                / coin_id
            ),
            filename=(
                f"risk_features_"
                f"{timestamp}.parquet"
            ),
        )

        print(
            f"Gold risk features written: "
            f"{gold_path}"
        )

        # HISTORICAL RISK

        historical_risk_df = (
            calculate_risk_score(
                feature_df
            )
        )

        historical_risk_path = write_parquet(
            df=historical_risk_df,
            directory=(
                RISK_HISTORY_GOLD_DIR
                / coin_id
            ),
            filename=(
                f"risk_history_"
                f"{timestamp}.parquet"
            ),
        )

        print(
            f"Historical risk written: "
            f"{historical_risk_path}"
        )

        # KEEP LATEST FEATURE ROW

        latest_feature = (
            feature_df
            .dropna(
                subset=[
                    "volatility_24",
                    "momentum_24",
                    "drawdown_168",
                ]
            )
            .tail(1)
            .copy()
        )

        latest_feature_rows.append(
            latest_feature
        )

    if not latest_feature_rows:

        raise RuntimeError(
            "No Silver history for any coin; nothing to score."
        )

    
    # CURRENT MARKET RISK
    

    latest_features_df = pd.concat(
        latest_feature_rows,
        ignore_index=True,
    )

    current_risk_df = calculate_risk_score(
        latest_features_df
    )

    # KEEP ONLY USEFUL CURRENT-RISK COLUMNS

    current_risk_df = current_risk_df[
        [
            "coin_id",
            "timestamp",
            "price",
            "volatility_24",
            "momentum_24",
            "drawdown_168",
            "turnover_ratio",
            "volatility_score",
            "drawdown_score",
            "momentum_score",
            "liquidity_score",
            "risk_score",
            "risk_level",
        ]
    ]

    # SAVE CURRENT RISK

    current_risk_path = write_parquet(
        df=current_risk_df,
        directory=CURRENT_RISK_GOLD_DIR,
        filename=(
            f"current_risk_{timestamp}.parquet"
        ),
    )

    print(
        f"\nCurrent risk snapshot written: "
        f"{current_risk_path}"
    )

    latest_risk_path = write_parquet(
        df=current_risk_df,
        directory=CURRENT_RISK_GOLD_DIR,
        filename="latest_risk.parquet",
    )

    print(
        f"Latest risk written: "
        f"{latest_risk_path}"
    )

    # LOAD CURRENT RISK INTO POSTGRESQL

    upsert_current_risk(
        df=current_risk_df,
        engine=engine,
    )

    print(
        "Current risk loaded into PostgreSQL."
    )

    print(
        current_risk_df[
            [
                "coin_id",
                "risk_score",
                "risk_level",
            ]
        ].to_string(index=False)
    )

    # Exit with an error code so a scheduler can tell
    # that one or more coins failed
    if failed_coins:

        print(
            f"\nCoinGecko pipeline finished with failures: "
            f"{', '.join(failed_coins)} (see pipeline_audit)."
        )

        raise SystemExit(1)

    print(
        "\nCoinGecko pipeline completed successfully."
    )

if __name__ == "__main__":
    main()