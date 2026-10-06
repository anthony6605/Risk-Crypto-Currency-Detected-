from sqlalchemy import text


def create_tables(engine):

    create_current_risk = text(
        """
        CREATE TABLE IF NOT EXISTS current_risk (

            coin_id VARCHAR(100) PRIMARY KEY,

            timestamp TIMESTAMPTZ NOT NULL,

            price DOUBLE PRECISION,

            volatility_24 DOUBLE PRECISION,

            momentum_24 DOUBLE PRECISION,

            drawdown_168 DOUBLE PRECISION,

            turnover_ratio DOUBLE PRECISION,

            volatility_score DOUBLE PRECISION,

            drawdown_score DOUBLE PRECISION,

            momentum_score DOUBLE PRECISION,

            liquidity_score DOUBLE PRECISION,

            risk_score DOUBLE PRECISION,

            risk_level VARCHAR(20),

            updated_at TIMESTAMPTZ
                DEFAULT CURRENT_TIMESTAMP

        );
        """
    )

    with engine.begin() as connection:

        connection.execute(
            create_current_risk
        )