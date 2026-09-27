import pandas as pd


def transform_market_history(
    data: dict,
    coin_id: str,
) -> pd.DataFrame:

    prices = pd.DataFrame(
        data["prices"],
        columns=[
            "timestamp",
            "price",
        ],
    )

    market_caps = pd.DataFrame(
        data["market_caps"],
        columns=[
            "timestamp",
            "market_cap",
        ],
    )

    volumes = pd.DataFrame(
        data["total_volumes"],
        columns=[
            "timestamp",
            "total_volume",
        ],
    )

    # Join the three datasets
    df = prices.merge(
        market_caps,
        on="timestamp",
        how="outer",
    )

    df = df.merge(
        volumes,
        on="timestamp",
        how="outer",
    )

    # Convert milliseconds to timestamp
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        unit="ms",
        utc=True,
    )

    # Add asset identifier
    df["coin_id"] = coin_id

    # Sort chronologically
    df = df.sort_values(
        "timestamp"
    )

    # Remove duplicates
    df = df.drop_duplicates(
        subset=[
            "coin_id",
            "timestamp",
        ],
        keep="last",
    )

    return df