from io import BytesIO

import pandas as pd
import datetime as dt
import sentry_sdk

from ddj_cloud.utils.storage import (
    DownloadFailedException,
    upload_dataframe,
    upload_file,
    download_file,
)
from .common import Federation, Exporter, ReservoirMeta, to_parquet_bio


IGNORE_LIST = [
    "Rurtalsperre Gesamt",
]


def _cleanup_old_data(df: pd.DataFrame) -> pd.DataFrame:
    ### CLEANUP ###
    # Remove extra column from past runs
    df.drop(columns=["fill_ratio"], errors="ignore", inplace=True)

    # Rename reservoirs
    from .federations.wupper import WupperFederation

    df["name"] = df["name"].replace(WupperFederation.renames)

    return df


def _get_base_dataset():
    # Download existing data
    df_db = None
    try:
        bio = download_file("talsperren/data.parquet.gzip")
        df_db = pd.read_parquet(bio, engine="fastparquet")
        df_db = _cleanup_old_data(df_db)

    except DownloadFailedException:
        pass

    is_first_run = df_db is None

    start = dt.datetime(1971, 1, 1) if is_first_run else None

    # Instantiate all federation classes
    federation_classes = Federation.__subclasses__()
    federations = [cls() for cls in federation_classes]  # type: ignore

    # Get data from all federations
    data = []
    for federation in federations:
        try:
            data.extend(federation.get_data(start=start))
        except Exception as e:
            print("Skipping federation due to error:")
            print(e)
            sentry_sdk.capture_exception(e)

    # Parse into data frame
    df_new = pd.DataFrame(data)

    # Cast ts_measured to datetime
    df_new["ts_measured"] = pd.to_datetime(df_new["ts_measured"], utc=True)

    # Add timestamp
    df_new["ts_scraped"] = dt.datetime.now(dt.timezone.utc)

    # Merge with existing data
    if not is_first_run:
        df = pd.concat([df_db, df_new])
    else:
        df = df_new

    # Deduplicate
    df = df.drop_duplicates(subset=["federation_name", "name", "ts_measured"], keep="first")

    # Sort
    df = df.sort_values(["federation_name", "name", "ts_measured"])

    # Uploads

    # Parquet
    bio = to_parquet_bio(df, compression="gzip", index=False)
    bio.seek(0)
    upload_file(bio.read(), "talsperren/data.parquet.gzip")

    # CSV
    upload_dataframe(df, "talsperren/data.csv")

    # Add additional columns used for exporting

    # Calculate fill ratio
    df["fill_ratio"] = df["content_mio_m3"] / df["capacity_mio_m3"]

    # Add metadata from federation classes
    metas = {
        (federation.name, reservoir_name): meta
        for federation in federations
        for reservoir_name, meta in federation.reservoirs.items()
    }

    for column in ReservoirMeta.__annotations__:
        if column in df.columns:
            continue

        df[column] = df.apply(
            lambda row: metas[(row["federation_name"], row["name"])][column],
            axis=1,
        )

    return df


def run():
    df_base = _get_base_dataset()

    ## For testing
    #
    # bio = to_parquet_bio(df_base, compression="gzip", index=False)
    # bio.seek(0)
    # upload_file(bio.read(), "talsperren/base.parquet.gzip")
    #
    # df_base = pd.read_parquet("local_storage/talsperren/base.parquet.gzip", engine="fastparquet")

    # Filter out reservoirs in ignore list
    df_base = df_base[~df_base["name"].isin(IGNORE_LIST)]

    # Exporters
    exporter_classes = Exporter.__subclasses__()
    exporters = [cls() for cls in exporter_classes]  # type: ignore

    for exporter in exporters:
        try:
            df_export = exporter.run(df_base.copy())
            upload_dataframe(df_export, f"talsperren/{exporter.filename}.csv")
        except Exception as e:
            print("Skipping exporter due to error:")
            print(e)
            sentry_sdk.capture_exception(e)
