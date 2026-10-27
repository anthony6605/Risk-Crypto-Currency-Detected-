import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from src.database.connection import get_engine  # noqa: E402
from src.risk.live_features import MIN_BARS  # noqa: E402

LEVELS = ["LOW", "MEDIUM", "HIGH", "EXTREME"]
REFRESH_SECONDS = 60

# The producer polls CoinGecko every poll_interval_seconds (10 minutes), so the newest
# price is normally a few minutes old. It counts as stale after two and a half polls.
STREAM_CONFIG = yaml.safe_load((PROJECT_ROOT / "config" / "stream.yaml").read_text())
POLL_SECONDS = STREAM_CONFIG["poll_interval_seconds"]
PRICE_STALE_AFTER_S = 2.5 * POLL_SECONDS
# The live-risk job runs every 5 minutes (docker-compose.yml): stale after two missed runs
RISK_STALE_AFTER_S = 600

# Risk freshness comes from the live-risk job's last successful run, not from the
# scores, so it still works while no coin has enough data to be scored.
HEALTH_SQL = """
SELECT
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - (SELECT MAX(event_time) FROM live_prices))  AS price_lag_s,
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - (SELECT MAX(ended_at) FROM pipeline_audit
        WHERE pipeline_name = 'live_risk' AND status = 'SUCCESS'))                     AS risk_lag_s,
    (SELECT COUNT(*) FROM live_prices)                                                AS coins_tracked,
    (SELECT COUNT(*) FROM live_risk)                                                  AS coins_scored,
    (SELECT COALESCE(MAX(bars), 0) FROM (
        SELECT COUNT(*) AS bars FROM price_bars_1m
        WHERE bar_start >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        GROUP BY base_asset) AS per_coin)                                             AS best_bars_24h,
    (SELECT COALESCE(SUM(rows_rejected), 0) FROM pipeline_audit
      WHERE pipeline_name = 'price_stream'
        AND started_at > CURRENT_TIMESTAMP - INTERVAL '1 hour')                       AS rejected_1h,
    (SELECT COUNT(*) FROM pipeline_audit
      WHERE status = 'FAILED'
        AND started_at > CURRENT_TIMESTAMP - INTERVAL '1 hour')                       AS failed_1h
"""

RISK_SQL = """
SELECT
    r.base_asset,
    COALESCE(r.name, r.base_asset) AS name,
    r.price,
    r.return_24h * 100       AS return_24h_pct,
    r.vol_24h * 100          AS vol_24h_pct,
    r.max_drawdown_24h * 100 AS max_drawdown_pct,
    r.risk_score,
    r.risk_level,
    p.volume_24h_quote,
    c.market_cap
FROM live_risk r
LEFT JOIN live_prices p USING (base_asset)
LEFT JOIN coin_catalog c ON c.symbol = LOWER(r.base_asset)
ORDER BY r.risk_score DESC
"""

# Every coin with a live price, biggest first, for the Coin detail picker
COINS_SQL = """
SELECT base_asset, COALESCE(name, base_asset) AS name, price, change_24h_pct
FROM live_prices_enriched
ORDER BY market_cap DESC NULLS LAST, base_asset
"""

BARS_SQL = """
SELECT bar_start, close
FROM price_bars_1m
WHERE base_asset = :base_asset
  AND bar_start >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY bar_start
"""

THROUGHPUT_SQL = """
SELECT
    to_timestamp(floor(EXTRACT(EPOCH FROM started_at) / 600) * 600) AS period,
    SUM(rows_loaded)   AS events_loaded,
    SUM(rows_rejected) AS events_rejected
FROM pipeline_audit
WHERE pipeline_name = 'price_stream'
  AND started_at > CURRENT_TIMESTAMP - INTERVAL '6 hours'
GROUP BY 1
ORDER BY 1
"""

# live_risk runs every 5 minutes, so it would push the daily jobs out of the
# recent-runs list. This shows the newest run of every job instead.
LATEST_RUNS_SQL = """
SELECT DISTINCT ON (pipeline_name)
       pipeline_name, status, started_at,
       EXTRACT(EPOCH FROM ended_at - started_at) AS duration_s,
       rows_loaded, error_message
FROM pipeline_audit
WHERE pipeline_name <> 'price_stream'
ORDER BY pipeline_name, audit_id DESC
"""

RUNS_SQL = """
SELECT pipeline_name, coin_id, status, rows_extracted, rows_loaded,
       started_at, EXTRACT(EPOCH FROM ended_at - started_at) AS duration_s, error_message
FROM pipeline_audit
WHERE pipeline_name <> 'price_stream'
ORDER BY audit_id DESC
LIMIT 50
"""

BATCH_SQL = "SELECT * FROM current_risk ORDER BY risk_score DESC"


@st.cache_resource
def engine():
    return get_engine()


@st.cache_data(ttl=REFRESH_SECONDS)
def query(sql, **params):
    with engine().connect() as connection:
        return pd.read_sql(text(sql), connection, params=params)


def format_lag(seconds):
    """Freshness in minutes (in hours once it's over 2 hours)."""
    if seconds is None or pd.isna(seconds):
        return "no data"
    minutes = seconds / 60
    if minutes < 1:
        return "under 1 min"
    if minutes < 120:
        return f"{minutes:.0f} min"
    return f"{minutes / 60:.1f} h"


def format_price(price):
    return f"{price:,.2f}" if price >= 1 else f"{price:.6g}"


def format_pct(value, digits=2):
    return "n/a" if pd.isna(value) else f"{value:.{digits}f}%"


def pipeline_status(health):
    """("ok" | "warming" | "attention", message) for the health row. No I/O, so it's unit tested."""
    problems = []
    if pd.isna(health["price_lag_s"]):
        problems.append("no prices yet")
    elif health["price_lag_s"] > PRICE_STALE_AFTER_S:
        problems.append(f"the newest price is {format_lag(health['price_lag_s'])} old")
    if pd.isna(health["risk_lag_s"]):
        problems.append("the live-risk job hasn't run yet")
    elif health["risk_lag_s"] > RISK_STALE_AFTER_S:
        problems.append(f"the live-risk job last ran {format_lag(health['risk_lag_s'])} ago")
    if health["failed_1h"] > 0:
        failed = int(health["failed_1h"])
        problems.append(f"{failed} failed run{'s' if failed != 1 else ''} in the last hour")

    if problems:
        return "attention", "Pipeline needs attention: " + "; ".join(problems) + "."
    if health["coins_scored"] == 0:
        return "warming", "Prices are up to date. Waiting for enough data to score coins (see Live risk)."
    return "ok", "Pipeline healthy: prices and risk scores are up to date."


def health_section():
    health = query(HEALTH_SQL).iloc[0]
    state, message = pipeline_status(health)
    if state == "ok":
        st.success(message)
    elif state == "warming":
        st.info(message)
    else:
        st.error(message)

    cols = st.columns(6)
    cols[0].metric("Price freshness", format_lag(health.price_lag_s),
                   help=f"Prices are polled every {POLL_SECONDS // 60} minutes")
    cols[1].metric("Risk freshness", format_lag(health.risk_lag_s))
    cols[2].metric("Coins tracked", int(health.coins_tracked))
    cols[3].metric("Coins scored", int(health.coins_scored))
    cols[4].metric("Rejected (1 h)", int(health.rejected_1h))
    cols[5].metric("Failed runs (1 h)", int(health.failed_1h))
    return health


def waiting_for_data(best_bars):
    hours = MIN_BARS * POLL_SECONDS / 3600
    st.info(
        f"Waiting for enough data. A coin is scored once it has {MIN_BARS} price updates in the "
        f"last 24 hours: about {hours:.0f} hours of polling at one poll every {POLL_SECONDS // 60} minutes."
    )
    st.progress(min(best_bars / MIN_BARS, 1.0), text=f"Closest coin: {best_bars} of {MIN_BARS} updates")


def live_risk_tab(health, levels, min_volume, search):
    risk = query(RISK_SQL)
    if risk.empty:
        waiting_for_data(int(health.best_bars_24h))
        return

    filtered = risk[
        risk["risk_level"].isin(levels)
        & (risk["volume_24h_quote"].fillna(0) >= min_volume)
    ]
    if search:
        term = search.lower()
        filtered = filtered[
            filtered["base_asset"].str.lower().str.contains(term, regex=False)
            | filtered["name"].str.lower().str.contains(term, regex=False)
        ]

    left, right = st.columns([3, 1])
    with left:
        st.dataframe(
            filtered,
            hide_index=True,
            width="stretch",
            column_config={
                "base_asset": "Symbol",
                "name": "Name",
                "price": st.column_config.NumberColumn("Price (USD)", format="%.6g"),
                "return_24h_pct": st.column_config.NumberColumn("24h return", format="%.2f%%"),
                "vol_24h_pct": st.column_config.NumberColumn("24h volatility", format="%.0f%%"),
                "max_drawdown_pct": st.column_config.NumberColumn("Max drawdown", format="%.2f%%"),
                "risk_score": st.column_config.ProgressColumn(
                    "Risk score", min_value=0, max_value=100, format="%.1f"
                ),
                "risk_level": "Level",
                "volume_24h_quote": st.column_config.NumberColumn("24h volume (USD)", format="compact"),
                "market_cap": st.column_config.NumberColumn("Market cap (USD)", format="compact"),
            },
        )
    with right:
        st.caption(f"{len(filtered)} of {len(risk)} scored coins")
        counts = risk["risk_level"].value_counts().reindex(LEVELS, fill_value=0)
        st.bar_chart(counts, sort=False)


def coin_detail_tab():
    coins = query(COINS_SQL)
    if coins.empty:
        st.info("No prices yet.")
        return

    choice = st.selectbox("Coin", coins["base_asset"] + " · " + coins["name"])
    base_asset = choice.split(" · ")[0]
    coin = coins[coins["base_asset"] == base_asset].iloc[0]
    risk = query(RISK_SQL)
    scored = risk[risk["base_asset"] == base_asset]
    bars = query(BARS_SQL, base_asset=base_asset)

    cols = st.columns(5)
    cols[0].metric("Price (USD)", format_price(coin.price))
    if scored.empty:
        cols[1].metric("Risk score", "not yet")
        cols[2].metric("24h change", format_pct(coin.change_24h_pct))
        cols[3].metric("Updates (24h)", f"{len(bars)} of {MIN_BARS}")
    else:
        row = scored.iloc[0]
        cols[1].metric("Risk score", f"{row.risk_score:.1f}", row.risk_level,
                       delta_color="off", delta_arrow="off")
        cols[2].metric("24h return", format_pct(row.return_24h_pct))
        cols[3].metric("24h volatility", format_pct(row.vol_24h_pct, digits=0))
        cols[4].metric("Max drawdown", format_pct(row.max_drawdown_pct))

    if bars.empty:
        st.info("No price updates in the last 24 hours.")
    else:
        st.line_chart(bars.set_index("bar_start")["close"])


def pipeline_tab():
    st.subheader("Stream throughput (events per 10 minutes, last 6 hours)")
    throughput = query(THROUGHPUT_SQL)
    if throughput.empty:
        st.info("No stream batches recorded yet.")
    else:
        st.bar_chart(throughput.set_index("period")[["events_loaded", "events_rejected"]])

    duration = {"duration_s": st.column_config.NumberColumn("Duration", format="%.1f s")}
    st.subheader("Latest run of each job")
    st.dataframe(query(LATEST_RUNS_SQL), hide_index=True, width="stretch", column_config=duration)
    st.subheader("Recent job runs")
    st.dataframe(query(RUNS_SQL), hide_index=True, width="stretch", column_config=duration)


def batch_tab():
    st.caption("Risk from the daily CoinGecko history batch (main.py), computed from hourly prices.")
    try:
        st.dataframe(query(BATCH_SQL), hide_index=True, width="stretch")
    except Exception as exc:
        st.warning(f"Batch table not available: {exc}")


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def live_page(levels, min_volume, search):
    health = health_section()
    tab_risk, tab_coin, tab_pipeline, tab_batch = st.tabs(
        ["Live risk", "Coin detail", "Pipeline", "Batch history"]
    )
    with tab_risk:
        live_risk_tab(health, levels, min_volume, search)
    with tab_coin:
        coin_detail_tab()
    with tab_pipeline:
        pipeline_tab()
    with tab_batch:
        batch_tab()


def main():
    st.set_page_config(page_title="RiskCoin", layout="wide")
    st.title("RiskCoin")
    st.caption(
        f"Crypto risk from CoinGecko prices: polled every {POLL_SECONDS // 60} minutes, "
        "sent through Kafka and scored every 5 minutes."
    )

    with st.sidebar:
        st.header("Filters")
        selected_levels = st.multiselect("Risk level", LEVELS, default=LEVELS)
        min_volume_m = st.slider("Minimum 24h volume (million USD)", 0, 100, 0)
        search_text = st.text_input("Search symbol or name")

    live_page(selected_levels, min_volume_m * 1_000_000, search_text)


# Streamlit runs this file as __main__; tests import it without drawing the page
if __name__ == "__main__":
    main()
