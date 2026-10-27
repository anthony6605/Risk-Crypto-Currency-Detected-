def build_bars(df):
    """
    Turn price events into 1-minute OHLC bars.
    df needs: event_id, base_asset, price, event_time (tz-aware datetime).
    """
    df = df.drop_duplicates(subset="event_id")
    df = df.assign(bar_start=df["event_time"].dt.floor("min")).sort_values("event_time")

    bars = (
        df.groupby(["base_asset", "bar_start"])
        .agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            first_event_time=("event_time", "min"),
            last_event_time=("event_time", "max"),
        )
        .reset_index()
    )
    return bars
