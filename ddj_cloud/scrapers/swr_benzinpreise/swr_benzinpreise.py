import datetime as dt
import json
import os
from functools import partial, reduce

import pandas as pd
from google.cloud import bigquery

from ddj_cloud.data.gemeinden_nrw import gemeinden_nrw
from ddj_cloud.utils import bigquery as bigquery_utils
from ddj_cloud.utils.date_and_time import BERLIN, UTC, local_now, local_today
from ddj_cloud.utils.storage import upload_dataframe

PROJECT = "swr-datalab-prod"
DATASET_BUNDESKARTELLAMT = bigquery.DatasetReference(PROJECT, "spritpreise_refined")
TABLE_TAGESWERTE = "stats_daily_by_fuel_type"
TABLE_TAGESWERTE_REGIO = "stats_daily_regio_by_fuel_type"
TABLE_AUFLOESUNG = "resolution_by_fuel_type"

DATASET_SPRITPREISE_TRUSTED = bigquery.DatasetReference(PROJECT, "spritpreise_trusted")
# Schema: ['openPrice:FLOAT', 'closePrice:FLOAT', 'lowPrice:FLOAT', 'highPrice:FLOAT', 'cumulativeVolumeUnit:FLOAT', 'meldedatum:DATE', 'datenstand:DATETIME', 'abrufdatum:TIMESTAMP']
TABLE_ROHOEL = "crude_oil_time_series"

LABELS = {"cost_category": "wdr", "triggered_by": "wdr-ddj-cloud"}

FUEL_TYPES = {1: "octane95", 2: "e10", 3: "diesel"}
STATS = ["min", "max", "mittel", "median", "percentil_10", "percentil_90"]
# Source suffix -> our suffix (backwards compatibility)
STATS_RENAME = {"mean": "mittel"}
# Source delivers full float precision, exports keep 5 decimals
DECIMALS = 5


def _rename_stats(df: pd.DataFrame, source_prefix: str, target_prefix: str) -> pd.DataFrame:
    """Rename `{source_prefix}_{stat}` to `{target_prefix}_{stat}`, drop other stats columns, round."""
    rename_map = {}
    for column in df.columns:
        if not column.startswith(f"{source_prefix}_"):
            continue
        stat = column.removeprefix(f"{source_prefix}_")
        stat = STATS_RENAME.get(stat, stat)
        if stat in STATS:
            rename_map[column] = f"{target_prefix}_{stat}"
    df = df[[*rename_map, *(c for c in df.columns if not c.startswith(f"{source_prefix}_"))]]
    df = df.rename(columns=rename_map)
    df[list(rename_map.values())] = df[list(rename_map.values())].round(DECIMALS)
    return df


def _clean_tageswerte(df: pd.DataFrame, id_columns: list[str]) -> pd.DataFrame:
    df = _rename_stats(df, "daily", "tages")
    df["type"] = df["type"].map(FUEL_TYPES)
    df = df.drop_duplicates(subset=[*id_columns, "type", "meldedatum"], ignore_index=True)

    # Backwards compatibility
    df = df.rename(columns={"meldedatum": "day"})
    df["datenstand"] = df["datenstand"].dt.date

    # Tag datetimes
    df["abrufdatum"] = df["abrufdatum"].dt.floor("s").dt.tz_localize(UTC)

    df = df[
        [
            *id_columns,
            "type",
            "day",
            *(f"tages_{stat}" for stat in STATS),
            "abrufdatum",
            "datenstand",
        ]
    ]
    return df


def load_tageswerte(client: bigquery.Client):
    query = "SELECT * FROM `@table_name` WHERE ags = @ags ORDER BY meldedatum DESC, type ASC"
    query = bigquery_utils.insert_table_name(query, TABLE_TAGESWERTE, "@table_name")

    job_config = bigquery.QueryJobConfig(
        default_dataset=DATASET_BUNDESKARTELLAMT,
        query_parameters=[
            bigquery.ScalarQueryParameter("ags", "STRING", "05"),
        ],
        labels=LABELS,
    )

    yield from bigquery_utils.iter_results(
        client,
        query,
        job_config,
        partial(_clean_tageswerte, id_columns=[]),
    )


def load_tageswerte_regio_latest(client: bigquery.Client):
    """Latest daily stats per NRW municipality (8-digit AGS)."""
    # Table is partitioned by `meldedatum` but not clustered by `ags`,
    # so the date window and column selection keep scanned bytes low
    query = """
        SELECT
            meldedatum,
            datenstand,
            abrufdatum,
            ags,
            type,
            daily_min,
            daily_max,
            daily_mean,
            daily_median,
            daily_percentil_10,
            daily_percentil_90
        FROM `@table_name`
        WHERE STARTS_WITH(ags, @ags_prefix) AND meldedatum >= @meldedatum_start
        QUALIFY meldedatum = MAX(meldedatum) OVER ()
        ORDER BY ags ASC, type ASC
    """
    query = bigquery_utils.insert_table_name(query, TABLE_TAGESWERTE_REGIO, "@table_name")

    job_config = bigquery.QueryJobConfig(
        default_dataset=DATASET_BUNDESKARTELLAMT,
        query_parameters=[
            bigquery.ScalarQueryParameter("ags_prefix", "STRING", "05"),
            bigquery.ScalarQueryParameter(
                "meldedatum_start", "DATE", local_today() - dt.timedelta(days=7)
            ),
        ],
        labels=LABELS,
    )

    yield from bigquery_utils.iter_results(
        client,
        query,
        job_config,
        partial(_clean_tageswerte, id_columns=["ags"]),
    )


def load_aufloesung(client: bigquery.Client):
    query = "SELECT * FROM `@table_name` WHERE ags = @ags AND datenstand BETWEEN @datenstand_start AND @datenstand_end ORDER BY datenstand DESC, type ASC"
    query = bigquery_utils.insert_table_name(query, TABLE_AUFLOESUNG, "@table_name")
    now = local_now()
    start = now - dt.timedelta(hours=48)
    end = now + dt.timedelta(days=1)

    job_config = bigquery.QueryJobConfig(
        default_dataset=DATASET_BUNDESKARTELLAMT,
        query_parameters=[
            bigquery.ScalarQueryParameter("ags", "STRING", "05"),
            bigquery.ScalarQueryParameter("datenstand_start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("datenstand_end", "TIMESTAMP", end),
        ],
        labels=LABELS,
    )

    def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
        df = _rename_stats(df, "resolution", "auflsg")
        df["type"] = df["type"].map(FUEL_TYPES)
        df = df.drop_duplicates(subset=["type", "datenstand"], ignore_index=True)

        # Tag datetimes
        df["abrufdatum"] = df["abrufdatum"].dt.floor("s").dt.tz_localize(UTC)

        df = df[["type", "datenstand", *(f"auflsg_{stat}" for stat in STATS), "abrufdatum"]]
        return df

    yield from bigquery_utils.iter_results(
        client,
        query,
        job_config,
        df_cleaner,
    )


def load_rohoel(client: bigquery.Client):
    # `lowPrice` and `highPrice` are the same as `closePrice` before 2026-03-05
    # `openPrice` not really interesting
    # `cumulativeVolumeUnit` seems to be always 0
    query = """
        SELECT
            closePrice,
            meldedatum,
            datenstand,
            abrufdatum
        FROM `@table_name`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY meldedatum
            ORDER BY datenstand DESC, abrufdatum DESC
        ) = 1
        ORDER BY meldedatum DESC
    """
    query = bigquery_utils.insert_table_name(query, TABLE_ROHOEL, "@table_name")

    job_config = bigquery.QueryJobConfig(
        default_dataset=DATASET_SPRITPREISE_TRUSTED,
        labels=LABELS,
    )

    def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
        rohoel_datenstand_tz = df["datenstand"].dt.tz
        rohoel_abrufdatum_tz = df["abrufdatum"].dt.tz
        df["datenstand"] = df["datenstand"].dt.floor("s")
        df["abrufdatum"] = df["abrufdatum"].dt.floor("s")

        df.rename(
            columns={
                "meldedatum": "day",
                "closePrice": "rohoel_close_price",
                "datenstand": "rohoel_datenstand",
                "abrufdatum": "rohoel_abrufdatum",
            },
            inplace=True,
        )

        # Naive `datenstand` is a plain date (midnight) since the source moved to `spritpreise_trusted`
        if rohoel_datenstand_tz is None:
            df["rohoel_datenstand"] = df["rohoel_datenstand"].dt.tz_localize(BERLIN)
        else:
            df["rohoel_datenstand"] = df["rohoel_datenstand"].dt.tz_convert(UTC)

        if rohoel_abrufdatum_tz is None:
            df["rohoel_abrufdatum"] = df["rohoel_abrufdatum"].dt.tz_localize(UTC)
        else:
            df["rohoel_abrufdatum"] = df["rohoel_abrufdatum"].dt.tz_convert(UTC)

        df.drop_duplicates(subset=["day"], inplace=True, ignore_index=True)
        df.reset_index(drop=True)

        return df

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

    upload_dataframe(
        df_latest,
        "swr_benzinpreise/latest.csv",
        datawrapper_datetimes=True,
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
            validate="one_to_one",
        ),
        tageswerte_expanded_dfs,
    )

    df_rohoel = pd.DataFrame(load_rohoel(bigquery_client))

    rohoel_value_columns = [
        "rohoel_close_price",
    ]

    # Duplicate columns so we can render a second, dotted line in the Datawrapper chart
    # that shows the transition over weekends/holidays
    for column in rohoel_value_columns:
        df_rohoel.insert(
            list(df_rohoel.columns).index(column),
            f"{column}_gestrichelt",
            df_rohoel[column],
        )

    df_rohoel_history = df_rohoel[df_rohoel[rohoel_value_columns].notna().any(axis=1)].copy()
    upload_dataframe(
        df_rohoel_history,
        "swr_benzinpreise/history_rohoel.csv",
        datawrapper_datetimes=True,
    )

    df_daily_history = pd.merge(
        df_tageswerte_expanded,
        df_rohoel,
        how="outer",
        on="day",
        validate="one_to_one",
    )
    upload_dataframe(
        df_daily_history,
        "swr_benzinpreise/history.csv",
        datawrapper_datetimes=True,
    )

    df_daily_30_days_expanded = df_daily_history[
        df_daily_history["day"] >= (local_today() - dt.timedelta(days=30))
    ]
    upload_dataframe(df_daily_30_days_expanded, "swr_benzinpreise/history_30_days.csv")

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
            validate="one_to_one",
        ),
        aufloesung_expanded_dfs,
    )
    upload_dataframe(
        df_aufloesung_expanded.copy(),
        "swr_benzinpreise/history_48_hours.csv",
        datawrapper_datetimes=True,
    )

    df_aufloesung_expanded_24_hours = df_aufloesung_expanded[
        df_aufloesung_expanded["datenstand"]
        >= (df_aufloesung_expanded["datenstand"].max() - dt.timedelta(hours=24))
    ].copy()
    upload_dataframe(
        df_aufloesung_expanded_24_hours,
        "swr_benzinpreise/history_24_hours.csv",
        datawrapper_datetimes=True,
    )

    # Latest per municipality, last so it can't block the other exports
    # Keep the schema even when the query returns no rows.
    df_latest_regio = pd.DataFrame(
        load_tageswerte_regio_latest(bigquery_client),
        columns=[
            "ags",
            "type",
            "day",
            *(f"tages_{stat}" for stat in STATS),
            "abrufdatum",
            "datenstand",
        ],
    )

    # Expand data
    latest_regio_expanded_dfs = [
        expand_column(df_latest_regio, "type", fuel_type, ["ags", "day"])
        for fuel_type in ["octane95", "e10", "diesel"]
    ]
    df_latest_regio = reduce(
        partial(
            pd.merge,
            how="outer",
            on=["ags", "day"],
            validate="one_to_one",
        ),
        latest_regio_expanded_dfs,
    )

    # Outer join so every municipality gets a map row and unknown AGS still show up
    df_gemeinden = (
        pd.DataFrame.from_dict(gemeinden_nrw, orient="index").rename_axis("ags").reset_index()
    )
    df_latest_regio = pd.merge(
        df_gemeinden,
        df_latest_regio,
        how="outer",
        on="ags",
        validate="one_to_one",
    )
    df_latest_regio = df_latest_regio.sort_values("ags", ignore_index=True)
    # Preserve datetime dtypes so upload_dataframe can convert them to Berlin time.
    # Missing values are written as empty CSV fields by pandas.

    upload_dataframe(
        df_latest_regio,
        "swr_benzinpreise/latest_regio.csv",
        datawrapper_datetimes=True,
    )
