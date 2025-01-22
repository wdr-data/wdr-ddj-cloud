from collections.abc import Mapping
from functools import partial, reduce

import pandas as pd

from ddj_cloud.scrapers.divi_intensivregister.common import filter_by_latest_date, load_data
from ddj_cloud.utils.storage import upload_dataframe

url = "https://github.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/raw/refs/heads/main/Intensivregister_Deutschland_Kapazitaeten.csv"


def expand_column(
    df: pd.DataFrame,
    target_column: str,
    value: str,
    static_columns: list[str],
) -> pd.DataFrame:
    df_expanded = df[df[target_column] == value].copy()
    df_expanded.drop(columns=[target_column], inplace=True)

    rename_map = {
        column: f"{column}__{value}" for column in df.columns if column not in static_columns
    }

    df_expanded.rename(columns=rename_map, inplace=True)
    return df_expanded


def expand_df(
    df: pd.DataFrame,
    *,
    target_column: str,
    static_columns: list[str],
    aggregate_columns: Mapping[str, str] | str | None = None,
) -> pd.DataFrame:
    if aggregate_columns is None:
        aggregate_columns = "sum"

    df = df.groupby([*static_columns, target_column], as_index=False).aggregate(aggregate_columns)

    expanded_dfs = [
        expand_column(df, target_column, value, static_columns)
        for value in df[target_column].unique()
    ]
    return reduce(
        partial(
            pd.merge,
            how="outer",
            on=static_columns,
            copy=False,  # type: ignore
            validate="one_to_one",
        ),
        expanded_dfs,
    )


def run():
    df = load_data(url)
    upload_dataframe(df, "divi_intensivregister/deutschland_kapazitaeten/history_original.csv")

    static_columns = [
        "datum",
        "bundesland_id",
        "bundesland_name",
    ]
    df_expanded_behandlungsgruppe = expand_df(
        df.drop(columns=["behandlungsgruppe_level_2"]),
        target_column="behandlungsgruppe",
        static_columns=static_columns,
    )
    upload_dataframe(
        df_expanded_behandlungsgruppe,
        "divi_intensivregister/deutschland_kapazitaeten/history_by_behandlungsgruppe.csv",
    )
    upload_dataframe(
        filter_by_latest_date(df_expanded_behandlungsgruppe),
        "divi_intensivregister/deutschland_kapazitaeten/latest_by_behandlungsgruppe.csv",
    )

    df_expanded_behandlungsgruppe_level_2 = expand_df(
        df.drop(columns=["behandlungsgruppe"]),
        target_column="behandlungsgruppe_level_2",
        static_columns=static_columns,
    )
    upload_dataframe(
        df_expanded_behandlungsgruppe_level_2,
        "divi_intensivregister/deutschland_kapazitaeten/history_by_behandlungsgruppe_level_2.csv",
    )
    upload_dataframe(
        filter_by_latest_date(df_expanded_behandlungsgruppe_level_2),
        "divi_intensivregister/deutschland_kapazitaeten/latest_by_behandlungsgruppe_level_2.csv",
    )
