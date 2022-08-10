import json
import os
from functools import reduce, partial
import datetime as dt

from google.cloud import bigquery
import numpy as np
import pandas as pd

from ddj_cloud.utils import bigquery as bigquery_utils
from ddj_cloud.utils.date_and_time import local_today
from ddj_cloud.utils.storage import upload_dataframe

PROJECT = "swr-data-1"
DATASET = bigquery.DatasetReference(PROJECT, "123Tanken")
TABLE_TAGESWERTE = "tageswerte"
TABLE_AUFLOESUNG = "aufloesung"


def load_tageswerte(client: bigquery.Client):
    query = "SELECT * FROM `@table_name` WHERE ags = @ags ORDER BY day DESC, type ASC"
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
        df.drop_duplicates(subset=["type", "day"], inplace=True, ignore_index=True)
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
        bigquery_client = bigquery_utils.make_client(service_account_info)
    else:
        print("Service account not found in environment, BigQuery client could not be created")
        print(f"Please set the environment variable {SERVICE_ACCOUNT_ENV_VAR}")
        return

    # Load data
    df_tageswerte = pd.DataFrame(load_tageswerte(bigquery_client))  # Full history since 2022-01-01

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

    df_tageswerte_30_days_expanded = df_tageswerte_expanded[
        df_tageswerte_expanded["day"] >= (local_today() - dt.timedelta(days=30))
    ]

    # Upload to S3
    upload_dataframe(df_tageswerte_expanded, "swr_benzinpreise/history.csv")
    upload_dataframe(df_tageswerte_30_days_expanded, "swr_benzinpreise/history_30_days.csv")
    upload_dataframe(df_tageswerte, "swr_benzinpreise/history_original.csv")
    upload_dataframe(df_latest, "swr_benzinpreise/latest.csv")
