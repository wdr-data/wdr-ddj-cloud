"""Utility functions ."""

import re
from collections.abc import Callable, Generator

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
    df_cleaner: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
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
    assert query_job.destination is not None
    destination = client.get_table(query_job.destination)

    rows = client.list_rows(destination, page_size=10000)

    # HACK: To avoid having to ship `pyarrow` (too big for Lambda), disable the
    # import check in google-cloud-bigquery
    from google.cloud.bigquery import _pandas_helpers

    imports_verifier_orig = _pandas_helpers.verify_pandas_imports
    _pandas_helpers.verify_pandas_imports = lambda: None
    dfs = rows.to_dataframe_iterable()
    _pandas_helpers.verify_pandas_imports = imports_verifier_orig

    for df in dfs:
        df_cleaned = df
        if df_cleaner is not None:
            df_cleaned = df_cleaner(df)

        for _, row in df_cleaned.iterrows():
            yield row
