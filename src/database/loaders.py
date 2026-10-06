import pandas as pd

from sqlalchemy import text


def upsert_current_risk(
    df: pd.DataFrame,
    engine,
):

    query = text(
        """
        INSERT INTO current_risk (

            coin_id,
            timestamp,
            price,

            volatility_24,
            momentum_24,
            drawdown_168,
            turnover_ratio,

            volatility_score,
            drawdown_score,
            momentum_score,
            liquidity_score,

            risk_score,
            risk_level

        )

        VALUES (

            :coin_id,
            :timestamp,
            :price,

            :volatility_24,
            :momentum_24,
            :drawdown_168,
            :turnover_ratio,

            :volatility_score,
            :drawdown_score,
            :momentum_score,
            :liquidity_score,

            :risk_score,
            :risk_level

        )

        ON CONFLICT (coin_id)

        DO UPDATE SET

            timestamp =
                EXCLUDED.timestamp,

            price =
                EXCLUDED.price,

            volatility_24 =
                EXCLUDED.volatility_24,

            momentum_24 =
                EXCLUDED.momentum_24,

            drawdown_168 =
                EXCLUDED.drawdown_168,

            turnover_ratio =
                EXCLUDED.turnover_ratio,

            volatility_score =
                EXCLUDED.volatility_score,

            drawdown_score =
                EXCLUDED.drawdown_score,

            momentum_score =
                EXCLUDED.momentum_score,

            liquidity_score =
                EXCLUDED.liquidity_score,

            risk_score =
                EXCLUDED.risk_score,

            risk_level =
                EXCLUDED.risk_level,

            updated_at =
                CURRENT_TIMESTAMP;
        """
    )

    records = (
        df
        .copy()
        .astype(
            {
                "risk_level": "string",
            }
        )
        .to_dict(
            orient="records"
        )
    )

    with engine.begin() as connection:

        connection.execute(
            query,
            records,
        )