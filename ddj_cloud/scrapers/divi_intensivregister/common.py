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


def add_rows_for_missing_dates(
    df: pd.DataFrame,
    copy_columns: list[str],
    *,
    max_date: str | None = None,
) -> pd.DataFrame:
    copy_columns_set = set(copy_columns)

    if max_date is None:
        max_date = df["datum"].max()

    min_date = df["datum"].min()

    dates_in_df = set(pd.to_datetime(df["datum"]).to_list())
    dates_in_range = set(pd.date_range(min_date, max_date))
    missing_dates = dates_in_range - dates_in_df

    missing_date_dfs = []

    for date in sorted(missing_dates):
        date_iso = date.strftime("%Y-%m-%d")

        # Get the previous row
        previous_day = date - pd.Timedelta(days=1)
        previous_day_iso = previous_day.strftime("%Y-%m-%d")
        previous_row = df[df["datum"] == previous_day_iso]

        # If the previous row is missing, we can't add a row for this date
        if previous_row.size == 0:
            continue

        new_row: pd.DataFrame = previous_row.copy()
        new_row["datum"] = date_iso

        # NAs for columns that are not in the copy_columns list
        for column in new_row.columns:
            if column not in copy_columns_set and column != "datum":
                new_row[column] = pd.NA

        # Insert the new row
        missing_date_dfs.append(new_row)
        print(f"Added row for {date_iso}")
        print(new_row)

    df = pd.concat([df, *missing_date_dfs])
    return df.sort_values(by=["datum"])
