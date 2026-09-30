import pandas as pd


def validate_risk_features(
    df: pd.DataFrame,
) -> None:

    if df.empty:
        raise ValueError(
            "Risk feature dataset is empty."
        )

    required_columns = [
        "coin_id",
        "timestamp",
        "price",
        "return",
        "volatility_24",
        "momentum_24",
        "drawdown_168",
        "volume_change_24",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing risk features: "
            f"{missing_columns}"
        )

    if df["coin_id"].isnull().any():
        raise ValueError(
            "Null coin IDs detected."
        )

    if df["timestamp"].isnull().any():
        raise ValueError(
            "Null timestamps detected."
        )