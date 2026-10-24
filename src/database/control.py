from sqlalchemy import text


PIPELINE_NAME = "crypto_history"


def get_watermark(
    engine,
    coin_id,
    pipeline_name=PIPELINE_NAME,
):
    """Return the last processed timestamp for a coin, or None on the first run."""

    query = text(
        """
        SELECT last_processed_timestamp
        FROM pipeline_watermarks
        WHERE pipeline_name = :pipeline_name
          AND coin_id = :coin_id
        """
    )

    with engine.connect() as connection:

        return connection.execute(
            query,
            {
                "pipeline_name": pipeline_name,
                "coin_id": coin_id,
            },
        ).scalar_one_or_none()


def update_watermark(
    engine,
    coin_id,
    last_processed_timestamp,
    pipeline_name=PIPELINE_NAME,
):
    """Insert or move the watermark forward. It never moves backwards."""

    query = text(
        """
        INSERT INTO pipeline_watermarks (
            pipeline_name,
            coin_id,
            last_processed_timestamp,
            updated_at
        )
        VALUES (
            :pipeline_name,
            :coin_id,
            :last_processed_timestamp,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (pipeline_name, coin_id)
        DO UPDATE SET
            last_processed_timestamp = GREATEST(
                pipeline_watermarks.last_processed_timestamp,
                EXCLUDED.last_processed_timestamp
            ),
            updated_at = CURRENT_TIMESTAMP
        """
    )

    with engine.begin() as connection:

        connection.execute(
            query,
            {
                "pipeline_name": pipeline_name,
                "coin_id": coin_id,
                "last_processed_timestamp": last_processed_timestamp,
            },
        )


def start_audit(
    engine,
    coin_id,
    pipeline_name=PIPELINE_NAME,
):
    """Open an audit row with status RUNNING and return its audit_id."""

    query = text(
        """
        INSERT INTO pipeline_audit (
            pipeline_name,
            coin_id,
            status,
            started_at
        )
        VALUES (
            :pipeline_name,
            :coin_id,
            'RUNNING',
            CURRENT_TIMESTAMP
        )
        RETURNING audit_id
        """
    )

    with engine.begin() as connection:

        return connection.execute(
            query,
            {
                "pipeline_name": pipeline_name,
                "coin_id": coin_id,
            },
        ).scalar_one()


def finish_audit(
    engine,
    audit_id,
    status,
    rows_extracted=0,
    rows_loaded=0,
    error_message=None,
):
    """Close an audit row with its final status and counts."""

    query = text(
        """
        UPDATE pipeline_audit
        SET status = :status,
            ended_at = CURRENT_TIMESTAMP,
            rows_extracted = :rows_extracted,
            rows_loaded = :rows_loaded,
            error_message = :error_message
        WHERE audit_id = :audit_id
        """
    )

    with engine.begin() as connection:

        connection.execute(
            query,
            {
                "audit_id": audit_id,
                "status": status,
                "rows_extracted": int(rows_extracted),
                "rows_loaded": int(rows_loaded),
                "error_message": error_message,
            },
        )


def close_abandoned_audits(
    engine,
    older_than_hours=2,
    pipeline_name=PIPELINE_NAME,
):
    """Mark RUNNING rows that never finished as FAILED."""

    query = text(
        """
        UPDATE pipeline_audit
        SET status = 'FAILED',
            ended_at = CURRENT_TIMESTAMP,
            error_message = 'Abandoned: run never finished'
        WHERE pipeline_name = :pipeline_name
          AND status = 'RUNNING'
          AND started_at < CURRENT_TIMESTAMP - make_interval(hours => :hours)
        """
    )

    with engine.begin() as connection:

        result = connection.execute(
            query,
            {
                "pipeline_name": pipeline_name,
                "hours": older_than_hours,
            },
        )

        return result.rowcount
