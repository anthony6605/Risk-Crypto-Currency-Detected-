import pandas as pd
from datetime import datetime, timezone

from config.configs import (
    COINGECKO_BRONZE_DIR,
    COINGECKO_HISTORY_BRONZE_DIR,
    MARKET_SILVER_DIR,
    HISTORY_SILVER_DIR,
    create_directories,
    RISK_GOLD_DIR,
    RISK_HISTORY_GOLD_DIR,
    CURRENT_RISK_GOLD_DIR,
)
from src.transformation.risk_score import (
    calculate_risk_score,
)

from src.quality.feature_checks import (
    validate_risk_features,
)

from src.transformation.features import (
    engineer_risk_features,
)
from src.ingestion.coingecko import (
    CoinGeckoClient,
)

from src.storage.local_storage import (
    write_json,
    write_parquet,
)

from src.transformation.market import (
    transform_market_data,
)

from src.transformation.history import (
    transform_market_history,
)

from src.quality.market_checks import (
    validate_market_data,
)

from src.quality.history_checks import (
    validate_market_history,
)


def main():

    create_directories()

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

   
    coins = [
        "bitcoin",
        "ethereum",
        "solana",
    ]

    latest_feature_rows = []

    for coin_id in coins:

        print(
            f"\nProcessing historical data for {coin_id}..."
        )

        

        history_data = client.get_market_history(
            coin_id=coin_id,
            currency="usd",
            days=30,
        )

        

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

        

        history_df = transform_market_history(
            data=history_data,
            coin_id=coin_id,
        )

        

        validate_market_history(
            history_df
        )

        print(
            f"{coin_id}: "
            f"{len(history_df)} "
            f"historical records validated."
        )

        

        silver_history_path = write_parquet(    
            df=history_df,
            directory=(
                HISTORY_SILVER_DIR
                / coin_id
            ),
            filename=(
                f"history_{timestamp}.parquet"
            ),
        )

        print(
            f"Historical Silver written: "
            f"{silver_history_path}"
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

    print(
        current_risk_df[
            [
                "coin_id",
                "risk_score",
                "risk_level",
            ]
        ].to_string(index=False)
    )

    print(
        "\nCoinGecko pipeline completed successfully."
    )

if __name__ == "__main__":
    main()