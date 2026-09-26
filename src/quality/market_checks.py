import pandas as pd


def validate_market_data(
    df: pd.DataFrame,
) -> None:

    if df.empty:
        raise ValueError(
            "Market dataset is empty."
        )

    required_columns = [
        "id",
        "symbol",
        "current_price",
        "market_cap",
        "last_updated",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: "
            f"{missing_columns}"
        )

    if df["id"].isnull().any():
        raise ValueError(
            "Null asset IDs detected."
        )

    if df["symbol"].isnull().any():
        raise ValueError(
            "Null symbols detected."
        )

    if (
        df["current_price"]
        .dropna()
        .lt(0)
        .any()
    ):
        raise ValueError(
            "Negative prices detected."
        )

    if df["id"].duplicated().any():
        raise ValueError(
            "Duplicate assets detected."
        )