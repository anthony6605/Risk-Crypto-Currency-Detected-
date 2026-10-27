import numpy as np
import pandas as pd

MINUTES_PER_YEAR = 525_600
MIN_BARS = 60

WEIGHTS = {
    "vol_24h": 0.40,
    "vol_1h": 0.20,
    "drawdown": 0.25,
    "abs_return": 0.15,
}

FEATURE_COLUMNS = [
    "base_asset", "price", "return_24h", "vol_1h", "vol_24h", "max_drawdown_24h", "bars_24h",
]


def compute_features(bars, min_bars=MIN_BARS):
    """bars: base_asset, bar_start, close. Returns one row of features per coin."""
    rows = []

    for base_asset, group in bars.sort_values("bar_start").groupby("base_asset"):
        # bars_24h counts minutes that had a price update (one per poll)
        if len(group) < min_bars:
            continue

        # Bars only exist for minutes with a price update: one every 10 minutes when
        # polling CoinGecko. Put the closes on a regular 1-minute grid (a minute
        # without an update keeps the last price) so every return covers exactly one
        # minute. Otherwise a 10-minute move counts as a 1-minute move, and every
        # coin looks far more volatile than it is.
        closes = (
            group.set_index("bar_start")["close"]
            .astype(float)
            .resample("1min")
            .last()
            .ffill()
            .reset_index(drop=True)
        )

        log_returns = np.log(closes).diff().dropna()
        running_max = closes.cummax()

        rows.append(
            {
                "base_asset": base_asset,
                "price": closes.iloc[-1],
                "return_24h": closes.iloc[-1] / closes.iloc[0] - 1,
                "vol_1h": log_returns.tail(60).std() * np.sqrt(MINUTES_PER_YEAR),
                "vol_24h": log_returns.std() * np.sqrt(MINUTES_PER_YEAR),
                "max_drawdown_24h": (closes / running_max - 1).min(),
                "bars_24h": len(group),
            }
        )

    return pd.DataFrame(rows, columns=FEATURE_COLUMNS)


def score_risk(features):
    """Add risk_score (0-100) and risk_level, ranking coins against each other."""
    scored = features.copy()
    if scored.empty:
        scored["risk_score"] = pd.Series(dtype=float)
        scored["risk_level"] = pd.Series(dtype=str)
        return scored

    components = {
        "vol_24h": scored["vol_24h"],
        "vol_1h": scored["vol_1h"],
        "drawdown": -scored["max_drawdown_24h"],
        "abs_return": scored["return_24h"].abs(),
    }

    score = sum(WEIGHTS[name] * values.rank(pct=True) for name, values in components.items())
    scored["risk_score"] = (score * 100).round(1)
    scored["risk_level"] = pd.cut(
        scored["risk_score"],
        bins=[0, 25, 50, 75, 100],
        labels=["LOW", "MEDIUM", "HIGH", "EXTREME"],
        include_lowest=True,
    ).astype(str)
    return scored
