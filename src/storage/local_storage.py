import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def write_json(
    data: Any,
    directory: Path,
    prefix: str,
) -> Path:

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    filename = f"{prefix}_{timestamp}.json"

    filepath = directory / filename

    with filepath.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            data,
            file,
            indent=2,
        )

    return filepath

def read_json(filepath: Path) -> Any:
    with filepath.open(
        "r",
        encoding="utf-8",
    ) as file:
        data = json.load(file)
    return data




def write_parquet(
    df: pd.DataFrame,
    directory: Path,
    filename: str,
) -> Path:

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    filepath = directory / filename

    df.to_parquet(
        filepath,
        index=False,
    )

    return filepath


def merge_into_silver(
    new_df: pd.DataFrame,
    silver_path: Path,
) -> int:
    """
    Append new rows to the Silver Parquet file, de-duplicate on (coin_id, timestamp),
    and return how many rows were genuinely new.
    """

    silver_path = Path(silver_path)

    silver_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if silver_path.exists():
        existing = pd.read_parquet(silver_path)
        rows_before = len(existing)
        combined = pd.concat(
            [existing, new_df],
            ignore_index=True,
        )
    else:
        rows_before = 0
        combined = new_df

    combined = (
        combined
        .drop_duplicates(
            subset=["coin_id", "timestamp"],
            keep="last",
        )
        .sort_values(["coin_id", "timestamp"])
        .reset_index(drop=True)
    )

    # Write to a temp file first so a crash mid-write can't corrupt Silver
    tmp_path = silver_path.with_suffix(".parquet.tmp")

    combined.to_parquet(
        tmp_path,
        index=False,
    )

    tmp_path.replace(silver_path)

    return len(combined) - rows_before