"""Utility functions ."""
import re
from typing import Callable, Generator

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.job.query import QueryJobConfig
from google.oauth2 import service_account


def make_client(service_account_info: dict, **kwargs) -> bigquery.Client:
    """
    Make a BigQuery client from a parsed service account JSON file, provided as a dict.

    Args:
        service_account_info (dict): The parsed service account JSON file
        **kwargs: Additional keyword arguments to pass to the bigquery.Client constructor

    Returns:
        bigquery.Client: A BigQuery client
    """
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    )

    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
        **kwargs,
    )


def insert_table_name(
    query: str,
    table_name: str,
    placeholder: str = "@table_name",
) -> str:
    """
    Replace @table_name with the actual table name
    BigQuery doesn't support parameterized table names :(
    Sanitizes the suffix, lol

    Args:
        query (str): The query to insert the table name into
        table_prefix (str): The table prefix
        table_suffix (str): The table suffix
        placeholder (str): The placeholder to replace
    """
    table_name = re.sub(r"[^a-zA-Z0-9_]", "", table_name)
    return query.replace(placeholder, f"{table_name}")


def iter_results(
    client: bigquery.Client,
    query: str,
    job_config: QueryJobConfig,
    df_cleaner: Callable[[pd.DataFrame], pd.DataFrame] = None,
) -> Generator[pd.Series, None, None]:
    """
    Page through the results of a query and yield each row as a pandas Series

    Args:
        query (str): The query to run
        job_config (QueryJobConfig): The BigQuery job config
        df_cleaner (Callable[[pd.DataFrame], pd.DataFrame]): A function to clean the dataframe

    Returns:
        Generator[pd.Series, None, None]: A generator of pandas Series
    """

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    # Get reference to destination table
    destination = client.get_table(query_job.destination)

    rows = client.list_rows(destination, page_size=10000)

    dfs = rows.to_dataframe_iterable()

    for df in dfs:
        if df_cleaner is not None:
            df = df_cleaner(df)

        for index, row in df.iterrows():
            yield row
