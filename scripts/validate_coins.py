import sys
from pathlib import Path

# Make the project root importable when run as `python scripts/validate_coins.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config.coins import get_tracked_coins  # noqa: E402
from src.ingestion.coingecko import CoinGeckoClient  # noqa: E402

client = CoinGeckoClient()

all_coins = client.get_coin_list()
valid_ids = {c["id"] for c in all_coins}

ids_by_symbol = {}
for c in all_coins:
    ids_by_symbol.setdefault(c["symbol"].lower(), []).append(c["id"])

tracked = get_tracked_coins(client)
print(f"Tracking {len(tracked)} coins: {tracked}")

unknown = [c for c in tracked if c not in valid_ids]

if not unknown:
    print("All coin ids are valid.")
else:
    for coin in unknown:
        candidates = ids_by_symbol.get(coin, [])
        # Many tokens share a symbol, so rank them by market cap to put the real coin first
        market = client.get_simple_prices(candidates) if candidates else {}
        candidates.sort(
            key=lambda coin_id: market.get(coin_id, {}).get("usd_market_cap") or 0,
            reverse=True,
        )
        print(f"UNKNOWN id '{coin}'. Did you mean one of: {candidates[:5]}")
    raise SystemExit(1)
