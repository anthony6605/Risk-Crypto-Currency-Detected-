import argparse
import time
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import text

from src.database.connection import get_engine
from src.database.control import close_abandoned_audits, finish_audit, start_audit
from src.database.schema import create_tables
from src.ingestion.coingecko import CoinGeckoClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OVERRIDES_PATH = PROJECT_ROOT / "config" / "symbol_overrides.yaml"

PIPELINE_NAME = "coin_catalog"
PAGES = 4
PER_PAGE = 250
REQUEST_DELAY_SECONDS = 2.5

UPSERT_CATALOG = text(
    """
    INSERT INTO coin_catalog (symbol, coin_id, name, market_cap, market_cap_rank, updated_at)
    VALUES (:symbol, :coin_id, :name, :market_cap, :market_cap_rank, CURRENT_TIMESTAMP)
    ON CONFLICT (symbol) DO UPDATE SET
        coin_id = EXCLUDED.coin_id,
        name = EXCLUDED.name,
        market_cap = EXCLUDED.market_cap,
        market_cap_rank = EXCLUDED.market_cap_rank,
        updated_at = CURRENT_TIMESTAMP
    """
)


def fetch_markets(client):
    rows = []
    for page in range(1, PAGES + 1):
        rows.extend(client.get_market_data(currency="usd", limit=PER_PAGE, page=page))
        time.sleep(REQUEST_DELAY_SECONDS)
    return rows


def load_overrides(path=OVERRIDES_PATH):
    if not Path(path).exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return {k.lower(): v for k, v in (data.get("overrides") or {}).items()}


def build_catalog(markets, overrides):
    """One row per symbol. Highest market cap wins, then manual overrides apply."""
    df = pd.DataFrame(markets)[["id", "symbol", "name", "market_cap", "market_cap_rank"]]
    df = df.rename(columns={"id": "coin_id"})
    # The ranking can shift between page requests, so a coin may appear on two pages
    df = df.drop_duplicates(subset="coin_id")
    df["symbol"] = df["symbol"].str.lower()
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

    # Keep every fetched coin for override lookups, before duplicates are dropped
    by_id = df.set_index("coin_id")

    df = (
        df.sort_values("market_cap", ascending=False, na_position="last")
        .drop_duplicates(subset="symbol", keep="first")
    )
    for symbol, coin_id in overrides.items():
        if coin_id in by_id.index:
            row = by_id.loc[coin_id]
            df = df[df["symbol"] != symbol]
            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        [{
                            "coin_id": coin_id,
                            "symbol": symbol,
                            "name": row["name"],
                            "market_cap": row["market_cap"],
                            "market_cap_rank": row["market_cap_rank"],
                        }]
                    ),
                ],
                ignore_index=True,
            )
        else:
            print(f"Override {symbol} -> {coin_id} skipped: id not in the top {PAGES * PER_PAGE}")

    return df.reset_index(drop=True)


def upsert_catalog(engine, catalog):
    rows = [
        {
            "symbol": r.symbol,
            "coin_id": r.coin_id,
            "name": r.name,
            "market_cap": None if pd.isna(r.market_cap) else float(r.market_cap),
            "market_cap_rank": None if pd.isna(r.market_cap_rank) else int(r.market_cap_rank),
        }
        for r in catalog.itertuples(index=False)
    ]
    with engine.begin() as connection:
        connection.execute(UPSERT_CATALOG, rows)
    return len(rows)


def run_once(engine, client):
    close_abandoned_audits(engine, pipeline_name=PIPELINE_NAME)
    audit_id = start_audit(engine, coin_id=None, pipeline_name=PIPELINE_NAME)
    try:
        markets = fetch_markets(client)
        catalog = build_catalog(markets, load_overrides())
        loaded = upsert_catalog(engine, catalog)
        finish_audit(engine, audit_id, "SUCCESS", rows_extracted=len(markets), rows_loaded=loaded)
        print(f"Coin catalog refreshed: {len(markets)} coins fetched, {loaded} symbols upserted.")
    except Exception as exc:
        finish_audit(engine, audit_id, "FAILED", error_message=str(exc)[:2000])
        print(f"Coin catalog refresh FAILED: {exc}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Refresh the coin catalog from CoinGecko")
    parser.add_argument("--every", type=int, default=None,
                        help="Repeat every N seconds (used when running in Docker)")
    args = parser.parse_args()

    engine = get_engine()
    create_tables(engine)
    client = CoinGeckoClient()

    while True:
        try:
            run_once(engine, client)
        except Exception:
            if args.every is None:
                raise SystemExit(1)
        if args.every is None:
            break
        time.sleep(args.every)


if __name__ == "__main__":
    main()
