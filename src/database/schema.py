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

    # Migration (Step 40): safe to run every time
    add_rows_rejected = text(
        "ALTER TABLE pipeline_audit ADD COLUMN IF NOT EXISTS rows_rejected INTEGER DEFAULT 0;"
    )

    # ==================================================
    # STREAMING TABLES (Steps 37-39)
    # ==================================================

    # Latest live price per coin, from the CoinGecko stream
    create_live_prices = text(
        """
        CREATE TABLE IF NOT EXISTS live_prices (
            base_asset        VARCHAR(30) PRIMARY KEY,
            exchange          VARCHAR(30) NOT NULL,
            pair              VARCHAR(40) NOT NULL,
            price             DOUBLE PRECISION NOT NULL,
            open_24h          DOUBLE PRECISION,
            high_24h          DOUBLE PRECISION,
            low_24h           DOUBLE PRECISION,
            volume_24h_quote  DOUBLE PRECISION,
            change_24h_pct    DOUBLE PRECISION,
            event_time        TIMESTAMPTZ NOT NULL,
            updated_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Exchange symbol (btc) -> CoinGecko id (bitcoin), refreshed daily
    create_coin_catalog = text(
        """
        CREATE TABLE IF NOT EXISTS coin_catalog (
            symbol           VARCHAR(30) PRIMARY KEY,
            coin_id          VARCHAR(100) NOT NULL,
            name             VARCHAR(200),
            market_cap       DOUBLE PRECISION,
            market_cap_rank  INTEGER,
            updated_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Needs live_prices and coin_catalog to exist first
    create_live_prices_enriched = text(
        """
        CREATE OR REPLACE VIEW live_prices_enriched AS
        SELECT
            lp.*,
            cc.coin_id,
            cc.name,
            cc.market_cap,
            cc.market_cap_rank
        FROM live_prices lp
        LEFT JOIN coin_catalog cc
            ON cc.symbol = LOWER(lp.base_asset);
        """
    )

    # 1-minute OHLC bars built by the consumer
    create_price_bars = text(
        """
        CREATE TABLE IF NOT EXISTS price_bars_1m (
            base_asset        VARCHAR(30) NOT NULL,
            bar_start         TIMESTAMPTZ NOT NULL,
            open              DOUBLE PRECISION NOT NULL,
            high              DOUBLE PRECISION NOT NULL,
            low               DOUBLE PRECISION NOT NULL,
            close             DOUBLE PRECISION NOT NULL,
            first_event_time  TIMESTAMPTZ NOT NULL,
            last_event_time   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (base_asset, bar_start)
        );
        """
    )

    create_price_bars_index = text(
        "CREATE INDEX IF NOT EXISTS idx_price_bars_1m_bar_start ON price_bars_1m (bar_start);"
    )

    # Live risk score per coin, recomputed every 5 minutes from the bars
    create_live_risk = text(
        """
        CREATE TABLE IF NOT EXISTS live_risk (
            base_asset        VARCHAR(30) PRIMARY KEY,
            coin_id           VARCHAR(100),
            name              VARCHAR(200),
            price             DOUBLE PRECISION,
            return_24h        DOUBLE PRECISION,
            vol_1h            DOUBLE PRECISION,
            vol_24h           DOUBLE PRECISION,
            max_drawdown_24h  DOUBLE PRECISION,
            bars_24h          INTEGER,
            risk_score        DOUBLE PRECISION,
            risk_level        VARCHAR(20),
            computed_at       TIMESTAMPTZ NOT NULL
        );
        """
    )

    with engine.begin() as connection:

        # Several programs call create_tables when they start (the batch, the
        # consumer, the jobs). This lock makes them take turns, because two
        # CREATE OR REPLACE VIEW statements at the same moment can fail.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(4207)")
        )

        connection.execute(
            create_current_risk
        )
        connection.execute(
            create_watermarks
        )
        connection.execute(
            create_pipeline_audit
        )
        connection.execute(
            add_rows_rejected
        )
        connection.execute(
            create_live_prices
        )
        connection.execute(
            create_coin_catalog
        )
        connection.execute(
            create_live_prices_enriched
        )
        connection.execute(
            create_price_bars
        )
        connection.execute(
            create_price_bars_index
        )
        connection.execute(
            create_live_risk
        )