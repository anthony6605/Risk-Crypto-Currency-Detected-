"""Query the Iceberg table with DuckDB: python -m src.lakehouse.explore [BASE_ASSET]"""

import sys

from src.lakehouse.catalog import PRICE_EVENTS, get_catalog


def main(base_asset="BTC"):
    table = get_catalog().load_table(PRICE_EVENTS)

    # 1. Plan the scan in Iceberg (partition and file pruning), then hand the rows to DuckDB.
    scan = table.scan(row_filter=f"base_asset == '{base_asset}'")
    print(f"files read for {base_asset}: {len(list(scan.plan_files()))}")
    con = scan.to_duckdb(table_name="events")
    con.execute("SET TimeZone = 'UTC'")  # like the rest of the project, not the computer's time zone
    con.sql(
        """
        SELECT date_trunc('hour', event_time) AS hour,
               count(*)                      AS events,
               round(min(price), 2)          AS low,
               round(max(price), 2)          AS high,
               round(arg_max(price, event_time), 2) AS close
        FROM events
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 24
        """
    ).show()

    # 2. Every write is a snapshot.
    snapshots = table.inspect.snapshots().to_pandas()
    snapshots["hour"] = snapshots["summary"].map(lambda s: dict(s).get("riskcoin.hour"))
    print(snapshots[["committed_at", "snapshot_id", "operation", "hour"]].tail(10).to_string(index=False))

    # 3. Time travel: read the table as it was after the first write.
    first = table.snapshots()[0]
    then = table.scan(snapshot_id=first.snapshot_id).to_arrow().num_rows
    now = table.scan().to_arrow().num_rows
    print(f"rows after the first snapshot: {then:,}   rows now: {now:,}")


if __name__ == "__main__":
    main(*sys.argv[1:2])
