from collections.abc import Generator
from io import StringIO

import pandas as pd
import requests


def load_data(url: str) -> pd.DataFrame:
    r = requests.get(url)
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text), low_memory=False)


def filter_by_latest_date(df: pd.DataFrame) -> pd.DataFrame:
    latest_date = df["datum"].max()
    return df[df["datum"] == latest_date]


def filter_by_id_column(
    df: pd.DataFrame, column_name: str
) -> Generator[tuple[int, pd.DataFrame], None, None]:
    for id_value in sorted(df[column_name].unique()):
        yield int(id_value), df[df[column_name] == id_value]


def filter_by_bundesland(df: pd.DataFrame) -> Generator[tuple[int, pd.DataFrame], None, None]:
    yield from filter_by_id_column(df, "bundesland_id")


def filter_by_landkreis(df: pd.DataFrame) -> Generator[tuple[int, pd.DataFrame], None, None]:
    yield from filter_by_id_column(df, "landkreis_id")
