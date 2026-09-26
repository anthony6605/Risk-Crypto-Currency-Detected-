import pandas as pd


def transform_market_data(data: list[dict]) -> pd.DataFrame:

    df = pd.DataFrame(data)

    columns = [
        "id",
        "symbol",
        "name",

        "current_price",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",

        "market_cap",
        "market_cap_rank",
        "market_cap_change_24h",
        "market_cap_change_percentage_24h",
        "fully_diluted_valuation",
        "total_volume",

        "circulating_supply",
        "total_supply",
        "max_supply",

        "ath",
        "ath_change_percentage",
        "ath_date",

        "atl",
        "atl_change_percentage",
        "atl_date",

        "last_updated",
    ]

    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    df = df[existing_columns].copy()

    # -------------------------
    # STANDARDIZE SYMBOL
    # -------------------------

    df["symbol"] = (
        df["symbol"]
        .astype(str)
        .str.upper()
    )

    # -------------------------
    # TIMESTAMPS
    # -------------------------

    timestamp_columns = [
        "last_updated",
        "ath_date",
        "atl_date",
    ]

    for column in timestamp_columns:

        if column in df.columns:

            df[column] = pd.to_datetime(
                df[column],
                utc=True,
                errors="coerce",
            )

    # -------------------------
    # REMOVE DUPLICATES
    # -------------------------

    df = df.drop_duplicates(
        subset=["id"],
        keep="last",
    )

    return df
