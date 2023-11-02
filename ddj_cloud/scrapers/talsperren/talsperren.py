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
from .common import ReservoirRecord, Federation


IGNORE_LIST = [
    "Rurtalsperre Gesamt",
]


def run():
    # Download existing data
    df_db = None
    try:
        bio = download_file("talsperren/data.parquet.gzip")
        df_db = pd.read_parquet(bio)
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

    # Calculate fill ratio
    df_new["fill_ratio"] = df_new["content_mio_m3"] / df_new["capacity_mio_m3"]

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
    bio = BytesIO()
    df.to_parquet(bio, compression="gzip")
    bio.seek(0)
    upload_file(bio.read(), "talsperren/data.parquet.gzip")

    # CSV
    upload_dataframe(df, "talsperren/data.csv")
