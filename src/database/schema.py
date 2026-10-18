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

    create_watermarks = text(
        """
        CREATE TABLE IF NOT EXISTS pipeline_watermarks (

            pipeline_name VARCHAR(100) NOT NULL,

            coin_id VARCHAR(100) NOT NULL,

            last_processed_timestamp TIMESTAMPTZ,

            updated_at TIMESTAMPTZ
                DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (
                pipeline_name,
                coin_id
            )
        );
        """
    )
    create_pipeline_audit = text(
        """
        CREATE TABLE IF NOT EXISTS pipeline_audit (

            audit_id BIGSERIAL PRIMARY KEY,

            pipeline_name VARCHAR(100) NOT NULL,

            coin_id VARCHAR(100),

            status VARCHAR(20) NOT NULL,

            started_at TIMESTAMPTZ NOT NULL,

            ended_at TIMESTAMPTZ,

            rows_extracted INTEGER DEFAULT 0,

            rows_loaded INTEGER DEFAULT 0,

            error_message TEXT

        );
        """
    )

    with engine.begin() as connection:

        connection.execute(
            create_current_risk
        )
        connection.execute(
            create_watermarks
        )
        connection.execute(
            create_pipeline_audit
        )