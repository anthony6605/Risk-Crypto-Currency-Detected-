import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

COINGECKO_BASE_URL = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3",
).strip().rstrip("/")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()

DATA_DIRECTORY = PROJECT_ROOT / "data"
BRONZE_DIRECTORY = DATA_DIRECTORY / "bronze"
SILVER_DIRECTORY = DATA_DIRECTORY / "silver"
GOLD_DIRECTORY = DATA_DIRECTORY / "gold"
COINGECKO_BRONZE_DIR = BRONZE_DIRECTORY / "coingecko"
BINANCE_BRONZE_DIR = BRONZE_DIRECTORY / "binance"
MARKET_SILVER_DIR = SILVER_DIRECTORY / "market"
COINGECKO_HISTORY_BRONZE_DIR = COINGECKO_BRONZE_DIR / "history"
HISTORY_SILVER_DIR = SILVER_DIRECTORY / "history"
RISK_GOLD_DIR = GOLD_DIRECTORY / "risk_features"
RISK_SCORE_GOLD_DIR = GOLD_DIRECTORY / "risk_scores"


def create_directories():
    directories = [
        BRONZE_DIRECTORY,
        SILVER_DIRECTORY,
        GOLD_DIRECTORY,
        COINGECKO_BRONZE_DIR,
        BINANCE_BRONZE_DIR,
        MARKET_SILVER_DIR,
        RISK_GOLD_DIR, 
        RISK_SCORE_GOLD_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
