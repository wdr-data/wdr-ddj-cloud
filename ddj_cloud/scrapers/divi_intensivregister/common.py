from collections.abc import Generator
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


def load_data(url: str) -> pd.DataFrame:
    cached_file = Path(__file__).parent / "cache" / url.split("/")[-1]

    if cached_file.exists():
        print("Using cached file:", cached_file)
        return pd.read_csv(cached_file, low_memory=False)

    print("Downloading data:", url)
    r = requests.get(url)
    r.raise_for_status()

    try:
        cached_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cached_file, "w") as f:
            f.write(r.text)
    except Exception:
        # Can't write here on Lambda
        pass

    return pd.read_csv(StringIO(r.text), low_memory=False)


def filter_by_latest_date(df: pd.DataFrame) -> pd.DataFrame:
    latest_date = df["datum"].max()
    return df[df["datum"] == latest_date]


def make_latest_date_single(
    df: pd.DataFrame, drop_columns: list[str], rename_columns: dict[str, str]
) -> pd.DataFrame:
    df = filter_by_latest_date(df).copy()
    df.reset_index(drop=True, inplace=True)
    df.drop(columns=drop_columns, inplace=True)
    df = df.melt(var_name="column", value_name="value")
    df.insert(
        1, "label", df["column"].apply(lambda col_name: rename_columns.get(col_name, col_name))
    )

    return df


def filter_by_id_column(
    df: pd.DataFrame, column_name: str
) -> Generator[tuple[int, pd.DataFrame], None, None]:
    for id_value in sorted(df[column_name].unique()):
        yield int(id_value), df[df[column_name] == id_value]


def filter_by_bundesland(df: pd.DataFrame) -> Generator[tuple[int, pd.DataFrame], None, None]:
    yield from filter_by_id_column(df, "bundesland_id")


def filter_by_landkreis(df: pd.DataFrame) -> Generator[tuple[int, pd.DataFrame], None, None]:
    yield from filter_by_id_column(df, "landkreis_id")
