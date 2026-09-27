from datetime import datetime, timezone

from config.configs import (
    COINGECKO_BRONZE_DIR,
    MARKET_SILVER_DIR,
    create_directories,
    COINGECKO_HISTORY_BRONZE_DIR,
    HISTORY_SILVER_DIR,
    
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

from src.quality.market_checks import (
    validate_market_data,
)

from src.transformation.history import (
    transform_market_history,
)

from src.quality.history_checks import (
    validate_market_history,
)


def main():

    create_directories()

    print("Starting CoinGecko ingestion...")

    #
    # EXTRACT
    
    client = CoinGeckoClient()

    market_data = client.get_market_data(
        currency="usd",
        limit=100,
    )

    print(
        f"Extracted {len(market_data)} assets."
    )

    
    # BRONZE
    

    bronze_path = write_json(
        data=market_data,
        directory=COINGECKO_BRONZE_DIR,
        prefix="market",
    )

    print(
        f"Bronze written: {bronze_path}"
    )

    
    # TRANSFORM
    

    silver_df = transform_market_data(
        market_data
    )

    
    # DATA QUALITY
    

    validate_market_data(
        silver_df
    )

    print(
        "Data quality checks passed."
    )

    
    # SILVER
  

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
            f"Historical Bronze written: {bronze_history_path}"
        )

        history_df = (
            transform_market_history(
                history_data,
                coin_id, 
            )
        )

        validate_market_history(
            history_df
        )

        print(
            f"{coin_id}: "
            f"{len(history_df)}"
            f"historical records validated."

        )

        silver_history_path = (
        write_parquet(
            df=history_df,
            directory=(
                HISTORY_SILVER_DIR
                / coin_id
            ),
            filename=(
                f"history_{timestamp}"
                f".parquet"
            ),
        )
    )

    print(
        f"Historical Silver written: "
        f"{silver_history_path}"
    )

    

if __name__ == "__main__":
    main()
