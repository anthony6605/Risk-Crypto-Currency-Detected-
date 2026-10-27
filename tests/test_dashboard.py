import pandas as pd

from app.dashboard import PRICE_STALE_AFTER_S, format_lag, pipeline_status


def health(**overrides):
    row = {"price_lag_s": 300.0, "risk_lag_s": 120.0, "coins_tracked": 100, "coins_scored": 40,
           "best_bars_24h": 144, "rejected_1h": 2, "failed_1h": 0}
    row.update(overrides)
    return pd.Series(row)


def test_fresh_data_is_healthy():
    assert pipeline_status(health())[0] == "ok"


def test_no_scores_yet_is_warming_up_not_an_error():
    assert pipeline_status(health(coins_scored=0))[0] == "warming"


def test_stale_prices_need_attention():
    state, message = pipeline_status(health(price_lag_s=PRICE_STALE_AFTER_S + 60))
    assert state == "attention"
    assert "newest price" in message


def test_missing_data_and_failed_runs_need_attention():
    assert pipeline_status(health(price_lag_s=float("nan")))[0] == "attention"
    assert pipeline_status(health(risk_lag_s=None))[0] == "attention"
    assert pipeline_status(health(failed_1h=1))[0] == "attention"


def test_freshness_is_shown_in_minutes():
    assert format_lag(30) == "under 1 min"
    assert format_lag(9 * 60) == "9 min"
    assert format_lag(3 * 3600) == "3.0 h"
    assert format_lag(None) == "no data"
