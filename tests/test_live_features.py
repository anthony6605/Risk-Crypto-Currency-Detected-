from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from src.risk.live_features import compute_features, score_risk

START = datetime(2026, 10, 25, tzinfo=timezone.utc)


def bars_for(base, closes):
    return pd.DataFrame({
        "base_asset": base,
        "bar_start": [START + timedelta(minutes=i) for i in range(len(closes))],
        "close": closes,
    })


def test_coins_with_too_few_bars_are_skipped():
    features = compute_features(bars_for("NEW", [1.0] * 10), min_bars=60)
    assert features.empty


def test_flat_price_has_zero_volatility_and_drawdown():
    f = compute_features(bars_for("FLAT", [10.0] * 120)).iloc[0]
    assert f.vol_24h == 0
    assert f.max_drawdown_24h == 0
    assert f.return_24h == 0


def test_drawdown_measures_fall_from_peak():
    closes = [100.0] * 30 + [120.0] * 30 + [90.0] * 30
    f = compute_features(bars_for("DD", closes)).iloc[0]
    assert np.isclose(f.max_drawdown_24h, 90 / 120 - 1)   # -25%
    assert np.isclose(f.return_24h, 90 / 100 - 1)         # -10%


def test_riskier_coin_gets_higher_score():
    rng = np.random.default_rng(42)
    calm = 100 * np.exp(np.cumsum(rng.normal(0, 0.0005, 200)))
    wild = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, 200)))
    bars = pd.concat([bars_for("CALM", calm), bars_for("WILD", wild)])

    scored = score_risk(compute_features(bars)).set_index("base_asset")
    assert scored.loc["WILD", "risk_score"] > scored.loc["CALM", "risk_score"]
    assert set(scored["risk_level"]) <= {"LOW", "MEDIUM", "HIGH", "EXTREME"}


def test_score_risk_handles_no_coins():
    scored = score_risk(compute_features(bars_for("X", [1.0])))
    assert scored.empty
    assert "risk_score" in scored.columns


def test_quiet_coin_is_not_made_to_look_more_volatile():
    # Same price path; one coin trades every minute, the other every 10 minutes
    rng = np.random.default_rng(42)
    closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, 1440)))
    dense = bars_for("DENSE", closes)
    sparse = dense.iloc[::10].assign(base_asset="SPARSE")

    f = compute_features(pd.concat([dense, sparse])).set_index("base_asset")
    assert np.isclose(f.loc["SPARSE", "vol_24h"], f.loc["DENSE", "vol_24h"], rtol=0.2)
    assert f.loc["SPARSE", "bars_24h"] == 144   # minutes that actually had trades
