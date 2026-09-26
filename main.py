from datetime import datetime, timezone

from config.configs import (
    COINGECKO_BRONZE_DIR,
    MARKET_SILVER_DIR,
    create_directories,
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


if __name__ == "__main__":
    main()
