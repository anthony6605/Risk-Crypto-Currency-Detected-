import pandas as pd


def validate_market_history(
    df: pd.DataFrame,
) -> None:

    if df.empty:
        raise ValueError(
            "Historical dataset is empty."
        )

    required_columns = [
        "coin_id",
        "timestamp",
        "price",
        "market_cap",
        "total_volume",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns: "
            f"{missing_columns}"
        )

    if df["timestamp"].isnull().any():
        raise ValueError(
            "Null timestamps detected."
        )

    if (
        df["price"]
        .dropna()
        .lt(0)
        .any()
    ):
        raise ValueError(
            "Negative prices detected."
        )

    if df.duplicated(
        subset=[
            "coin_id",
            "timestamp",
        ]
    ).any():

        raise ValueError(
            "Duplicate historical records."
        )