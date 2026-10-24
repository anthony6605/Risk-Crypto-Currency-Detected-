from pathlib import Path

import yaml


# parents[2] walks up from src/config/coins.py to the project root,
# so this works no matter which folder you run from
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "coins.yaml"


def load_coin_config(
    path=CONFIG_PATH,
):
    with open(path) as f:
        return yaml.safe_load(f)


def get_tracked_coins(
    client=None,
    path=CONFIG_PATH,
):
    """Return the list of CoinGecko ids the pipeline should track."""

    config = load_coin_config(path)
    mode = config.get("mode", "list")

    if mode == "list":
        coins = config["coins"]

    elif mode == "top_n":
        if client is None:
            raise ValueError("top_n mode needs a CoinGecko client")

        settings = config["top_n"]
        size = settings["size"]
        exclude = set(settings.get("exclude", []))

        # Ask for extra so we still have `size` coins after removing excluded ones
        markets = client.get_market_data(
            currency=config.get("currency", "usd"),
            limit=size + len(exclude),
        )
        coins = [m["id"] for m in markets if m["id"] not in exclude][:size]

    else:
        raise ValueError(f"Unknown coin mode: {mode}")

    # Normalise and remove duplicates, keeping order
    return list(dict.fromkeys(c.strip().lower() for c in coins))
