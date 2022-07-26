import os
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, List, Optional, Union
from uuid import uuid4

import pandas as pd

from botocore.exceptions import ClientError
from boto3 import client
import sentry_sdk

from ddj_cloud.utils.date_and_time import local_today

USE_LOCAL_FILESYSTEM = os.environ.get("USE_LOCAL_FILESYSTEM", False)

if USE_LOCAL_FILESYSTEM:
    LOCAL_FILESYSTEM_ROOT = Path(__file__).parent.parent.parent / "local_filesystem"
else:
    try:
        BUCKET_NAME = os.environ["BUCKET_NAME"]
        s3 = client("s3")
    except KeyError:
        BUCKET_NAME = None
        s3 = None
        print("Warning: S3 client not created")

    try:
        CLOUDFRONT_ID = os.environ["CLOUDFRONT_ID"]
        cloudfront = client("cloudfront")
    except KeyError:
        CLOUDFRONT_ID = None
        cloudfront = None
        print("Warning: CloudFront client not created")


def simple_compare(old: Any, new: Any) -> bool:
    return old == new


def make_df_compare_fn(*, ignore_columns: Union[str, list[str], None] = None) -> Callable:
    def is_equal(old, new):
        old = pd.read_csv(BytesIO(old), dtype="str")
        new = pd.read_csv(BytesIO(new), dtype="str")

        if ignore_columns is not None:
            old = old.drop(columns=ignore_columns, errors="ignore")
            new = new.drop(columns=ignore_columns, errors="ignore")

        return old.equals(new)

    return is_equal


class DownloadFailedException(Exception):
    pass


def download_file(filename: str) -> BytesIO:
    try:
        if USE_LOCAL_FILESYSTEM:
            with open(LOCAL_FILESYSTEM_ROOT / filename, "rb") as fp:
                bio = BytesIO(fp.read())
        else:
            bio = BytesIO()
            s3.download_fileobj(BUCKET_NAME, filename, bio)
    except:
        raise DownloadFailedException(f"Failed to download file {filename}")

    bio.seek(0)
    return bio


def upload_file(
    bio: BytesIO,
    filename: str,
    *,
    acl: Optional[str] = "public-read",
    content_type: Optional[str] = None,
):
    if USE_LOCAL_FILESYSTEM:
        # Ensure path exists
        (LOCAL_FILESYSTEM_ROOT / filename).parent.mkdir(parents=True, exist_ok=True)

        with open(LOCAL_FILESYSTEM_ROOT / filename, "wb") as fp:
            fp.write(bio.getbuffer())

    else:
        # Upload file with ACL and content type
        s3.upload_fileobj(
            bio,
            BUCKET_NAME,
            filename,
            ExtraArgs={
                "ACL": acl,
                "ContentType": content_type,
            },
        )


def _create_cloudfront_invalidation(
    filenames=Union[str, List[str]],
    *,
    caller_reference: Optional[str] = None,
) -> Any:
    if isinstance(filenames, str):
        filenames = [filenames]

    caller_reference = caller_reference or str(uuid4())

    if cloudfront:
        return cloudfront.create_invalidation(
            DistributionId=CLOUDFRONT_ID,
            InvalidationBatch={
                "Paths": {
                    "Quantity": len(filenames),
                    "Items": filenames,
                },
                "CallerReference": caller_reference,
            },
        )


def upload_dataframe(
    df: pd.DataFrame,
    filename: str,
    *,
    create_cloudfront_invalidation: bool = True,
    change_notification: Optional[str] = None,
    compare: Optional[Callable] = None,
):

    if compare is None:
        compare = simple_compare

    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False).encode("utf-8")

    # Read old file-like object to check for differences
    bio_old = BytesIO()
    file_exists = True

    try:
        s3.download_fileobj(BUCKET_NAME, filename, bio_old)
    except ClientError:
        file_exists = False

    bio_old.seek(0)

    if not file_exists or not compare(bio_old.read(), write):

        # Create new file-like object for upload
        bio_new = BytesIO(write)

        # Upload file with ACL and content type
        upload_file(bio_new, filename, content_type="text/plain; charset=utf-8")

        # Upload file again into timestamped folder
        bio_new = BytesIO(write)
        timestamp = local_today().isoformat()
        upload_file(bio_new, f"{timestamp}/{filename}", content_type="text/plain; charset=utf-8")

        # Create CloudFront invalidation
        if create_cloudfront_invalidation:
            _create_cloudfront_invalidation([filename, f"{timestamp}/{filename}"])

        # Notify
        if change_notification:
            sentry_sdk.capture_message(change_notification)
