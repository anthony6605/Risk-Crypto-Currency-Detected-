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