import datetime as dt
import json
import os
from functools import partial, reduce

import numpy as np
import pandas as pd
from google.cloud import bigquery

from ddj_cloud.utils import bigquery as bigquery_utils
from ddj_cloud.utils.date_and_time import local_now, local_today
from ddj_cloud.utils.storage import upload_dataframe

PROJECT = "swr-datalab-prod"
DATASET = bigquery.DatasetReference(PROJECT, "bundeskartellamt_trusted")
TABLE_TAGESWERTE = "markttransparenzstelle_tageswerte"
TABLE_AUFLOESUNG = "markttransparenzstelle_aufloesung"


def load_tageswerte(client: bigquery.Client):
    query = "SELECT * FROM `@table_name` WHERE ags = @ags ORDER BY meldedatum DESC, type ASC"
    query = bigquery_utils.insert_table_name(query, TABLE_TAGESWERTE, "@table_name")

    job_config = bigquery.QueryJobConfig(
        default_dataset=DATASET,
        query_parameters=[
            bigquery.ScalarQueryParameter("ags", "STRING", "05"),
        ],
    )

    def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
        # Convert to date
        df.drop(
            columns=[
                "tages_mittel_min_10",
                "tages_mittel_min_25",
                "tages_mittel_min_50",
                "tages_mittel_min_75",
                "tages_mittel_max_10",
                "tages_mittel_max_25",
                "tages_mittel_max_50",
                "tages_mittel_max_75",
                "n_unique_dates",
                "n_preise",
                "ags",
            ],
            inplace=True,
        )
        df["type"] = df["type"].map({1: "octane95", 2: "e10", 3: "diesel"})
        df.drop_duplicates(subset=["type", "meldedatum"], inplace=True, ignore_index=True)
        df.reset_index(drop=True)

        # Backwards compatibility
        df.rename(columns={"meldedatum": "day"}, inplace=True)

        return df.replace({np.nan: None})

    yield from bigquery_utils.iter_results(
        client,
        query,
        job_config,
        df_cleaner,
    )


def load_aufloesung(client: bigquery.Client):
    query = "SELECT * FROM `@table_name` WHERE ags = @ags AND datenstand BETWEEN @datenstand_start AND @datenstand_end ORDER BY datenstand DESC, type ASC"
    query = bigquery_utils.insert_table_name(query, TABLE_AUFLOESUNG, "@table_name")
    now = local_now()
    start = now - dt.timedelta(hours=48)
    end = now + dt.timedelta(days=1)

    job_config = bigquery.QueryJobConfig(
        default_dataset=DATASET,
        query_parameters=[
            bigquery.ScalarQueryParameter("ags", "STRING", "05"),
            bigquery.ScalarQueryParameter("datenstand_start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("datenstand_end", "TIMESTAMP", end),
        ],
    )

    def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
        df.drop(
            columns=[
                "auflsg_mittel_min_10",
                "auflsg_mittel_min_25",
                "auflsg_mittel_min_50",
                "auflsg_mittel_min_75",
                "auflsg_mittel_max_10",
                "auflsg_mittel_max_25",
                "auflsg_mittel_max_50",
                "auflsg_mittel_max_75",
                "n_unique_dates",
                "n_preise",
                "ags",
                "meldedatum",
            ],
            inplace=True,
        )
        df["type"] = df["type"].map({1: "octane95", 2: "e10", 3: "diesel"})
        df.reset_index(drop=True)

        return df.replace({np.nan: None})

    yield from bigquery_utils.iter_results(
        client,
        query,
        job_config,
        df_cleaner,
    )


def expand_column(
    df: pd.DataFrame,
    target_column: str,
    value: str,
    ignore_columns: list[str],
) -> pd.DataFrame:
    df_expanded = df[df[target_column] == value].copy()
    df_expanded.drop(columns=[target_column], inplace=True)

    rename_map = {
        column: f"{value}_{column}" for column in df.columns if column not in ignore_columns
    }

    df_expanded.rename(columns=rename_map, inplace=True)
    return df_expanded


def run():
    # Set up Google BigQuery access
    SERVICE_ACCOUNT_ENV_VAR = "SWR_BENZINPREISE_SERVICE_ACCOUNT"

    if SERVICE_ACCOUNT_ENV_VAR in os.environ:
        service_account_info = json.loads(os.environ[SERVICE_ACCOUNT_ENV_VAR])
        bigquery_client = bigquery_utils.make_client(service_account_info, location="europe-west3")
    else:
        print("Service account not found in environment, BigQuery client could not be created")
        print(f"Please set the environment variable {SERVICE_ACCOUNT_ENV_VAR}")
        return

    # Load data
    df_tageswerte = pd.DataFrame(load_tageswerte(bigquery_client))  # Full history since 2022-01-01
    upload_dataframe(df_tageswerte, "swr_benzinpreise/history_original.csv")

    # Latest data in some kind of dashboard format
    df_latest = df_tageswerte[df_tageswerte["day"] == df_tageswerte["day"].max()].copy()
    df_latest.replace(
        {
            "octane95": "Super E5",
            "e10": "Super E10",
            "diesel": "Diesel",
        },
        inplace=True,
    )

    upload_dataframe(df_latest, "swr_benzinpreise/latest.csv")

    # Expand data
    tageswerte_expanded_dfs = [
        expand_column(df_tageswerte, "type", fuel_type, ["day"])
        for fuel_type in ["octane95", "e10", "diesel"]
    ]
    df_tageswerte_expanded = reduce(
        partial(
            pd.merge,
            how="outer",
            on="day",
            copy=False,
            validate="one_to_one",
        ),
        tageswerte_expanded_dfs,
    )
    upload_dataframe(df_tageswerte_expanded, "swr_benzinpreise/history.csv")

    df_tageswerte_30_days_expanded = df_tageswerte_expanded[
        df_tageswerte_expanded["day"] >= (local_today() - dt.timedelta(days=30))
    ]
    upload_dataframe(df_tageswerte_30_days_expanded, "swr_benzinpreise/history_30_days.csv")

    # Load data
    df_aufloesung = pd.DataFrame(load_aufloesung(bigquery_client))

    # Expand data
    aufloesung_expanded_dfs = [
        expand_column(df_aufloesung, "type", fuel_type, ["datenstand"])
        for fuel_type in ["octane95", "e10", "diesel"]
    ]

    df_aufloesung_expanded = reduce(
        partial(
            pd.merge,
            how="outer",
            on="datenstand",
            copy=False,
            validate="one_to_one",
        ),
        aufloesung_expanded_dfs,
    )
    upload_dataframe(df_aufloesung_expanded, "swr_benzinpreise/history_48_hours.csv")

    df_aufloesung_expanded_24_hours = df_aufloesung_expanded[
        df_aufloesung_expanded["datenstand"]
        >= (df_aufloesung_expanded["datenstand"].max() - dt.timedelta(hours=24))
    ]
    upload_dataframe(df_aufloesung_expanded_24_hours, "swr_benzinpreise/history_24_hours.csv")
