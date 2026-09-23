from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIRECTORY = PROJECT_ROOT / "data"
BRONZE_DIRECTORY = DATA_DIRECTORY / "bronze"
SILVER_DIRECTORY = DATA_DIRECTORY / "silver"
GOLD_DIRECTORY = DATA_DIRECTORY / "gold"
COINGECKO_BRONZE_DIR = BRONZE_DIRECTORY / "coingecko"
BINANCE_BRONZE_DIR = BRONZE_DIRECTORY / "binance"


def create_directories():
    directories = [
        BRONZE_DIRECTORY,
        SILVER_DIRECTORY,
        GOLD_DIRECTORY,
        COINGECKO_BRONZE_DIR,
        BINANCE_BRONZE_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)