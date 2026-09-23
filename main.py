from config.configs import (
    COINGECKO_BRONZE_DIR,
    create_directories,
)

from src.ingestion.coingecko import CoinGeckoAPI
from src.storage.local_storage import write_json


def main():

    create_directories()

    print("Starting CoinGecko ingestion...")

    client = CoinGeckoAPI()

    market_data = client.get_market_data(
        currency="usd",
        limit=100,
    )

    path = write_json(
        data=market_data,
        directory=COINGECKO_BRONZE_DIR,
        prefix="market",
    )

    print(
        f"Extracted {len(market_data)} cryptocurrencies"
    )

    print(
        f"Bronze data saved to: {path}"
    )


if __name__ == "__main__":
    main()
