import os
from collections.abc import Callable
from io import BytesIO
from os.path import commonprefix as common_prefix
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import sentry_sdk
from boto3 import client

from ddj_cloud.utils.date_and_time import local_today

USE_LOCAL_STORAGE = os.environ.get("USE_LOCAL_STORAGE", False)
STORAGE_EVENTS = []

CLOUDFRONT_INVALIDATIONS_TO_CREATE = []

if USE_LOCAL_STORAGE:
    LOCAL_STORAGE_ROOT = Path(__file__).parent.parent.parent / "local_storage"
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


def describe_events(*, clear: bool = True) -> list[str]:
    """Describe the events that have been recorded by the storage module.

    Args:
        clear (bool, optional): Clear the events after they have been described. Defaults to False.

    Returns:
        list[str]: List of events
    """

    def _describe(fs_event):  # noqa: PLR0911
        if fs_event["type"] == "download":
            if fs_event["success"]:
                return f'Downloaded file "{fs_event["filename"]}" from storage'
            else:
                return (
                    f'Attempted to download non-existing file "{fs_event["filename"]}" from storage'
                )

        elif fs_event["type"] == "upload":
            return f'Uploaded file "{fs_event["filename"]}" to storage'

        elif fs_event["type"] == "archive":
            return f'Archived file "{fs_event["original_filename"]}" to "{fs_event["archived_filename"]}"'

        elif fs_event["type"] == "existed":
            return f'Attempted to upload a file "{fs_event["filename"]}" that was identical to the file in storage'

        elif fs_event["type"] == "invalidation":
            return f'Created CloudFront invalidation for "{fs_event["path"]}"'

        else:
            return f'Unknown event type "{fs_event["type"]}"'

    descriptions = [_describe(fs_event) for fs_event in STORAGE_EVENTS]

    if clear:
        STORAGE_EVENTS.clear()

    return descriptions


def simple_compare(old: Any, new: Any) -> bool:
    return old == new


def make_df_compare_fn(
    *, ignore_columns: str | list[str] | None = None
) -> Callable[[bytes, bytes], bool]:
    """Create a function that can be used as the ``compare_fn`` argument to ``upload_dataframe``
    to compare two pandas DataFrames by their contents while ignoring specified columns.

    This is useful if you have a timestamp column and you want to ignore it when comparing.

    Args:
        ignore_columns (Union[str, list[str], None]): Columns to ignore when comparing two DataFrames.

    Returns:
        Callable[[bytes, bytes], bool]: Function that can be used as the ``compare_fn`` argument to ``upload_dataframe``
    """

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


def _download_file(filename: str) -> BytesIO:
    """Internal file download function"""
    try:
        if USE_LOCAL_STORAGE:
            with open(LOCAL_STORAGE_ROOT / filename, "rb") as fp:
                bio = BytesIO(fp.read())
        else:
            assert s3 is not None
            bio = BytesIO()
            s3.download_fileobj(BUCKET_NAME, filename, bio)

    except Exception as err:
        msg = f"Failed to download file {filename}"
        raise DownloadFailedException(msg) from err

    bio.seek(0)
    return bio


def download_file(filename: str) -> BytesIO:
    """Download a file from storage.

    If the file was not found or some other error occured, a ``DownloadFailedException`` will be raised.

    Args:
        filename (str): Filename to download

    Returns:
        BytesIO: ``BytesIO`` object containing the file contents
    """
    try:
        bio = _download_file(filename)
    except DownloadFailedException:
        STORAGE_EVENTS.append({"type": "download", "filename": filename, "success": False})
        raise

    STORAGE_EVENTS.append({"type": "download", "filename": filename, "success": True})

    return bio


def __upload_file(
    content: bytes,
    filename: str,
    *,
    acl: str | None = None,
    content_type: str | None = None,
    content_encoding: str | None = None,
):
    bio = BytesIO(content)

    """Internal file upload function"""
    if USE_LOCAL_STORAGE:
        # Ensure path exists
        (LOCAL_STORAGE_ROOT / filename).parent.mkdir(parents=True, exist_ok=True)

        with open(LOCAL_STORAGE_ROOT / filename, "wb") as fp:
            fp.write(bio.getbuffer())

        bio.close()

    else:
        # Upload file with ACL and content type
        extra_args = {}

        if acl is not None:
            extra_args["ACL"] = acl

        if content_type is not None:
            extra_args["ContentType"] = content_type

        if content_encoding is not None:
            extra_args["ContentEncoding"] = content_encoding

        assert s3 is not None
        s3.upload_fileobj(
            bio,
            BUCKET_NAME,
            filename,
            ExtraArgs=extra_args,
        )


def _upload_file(
    content: bytes,
    filename: str,
    *,
    acl: str | None = None,
    content_type: str | None = None,
    content_encoding: str | None = None,
    archive: bool = True,
) -> list[str]:
    """Internal file upload function that performs optional achiving and storage event tracking"""
    filenames = [filename]

    # Upload file normally
    __upload_file(
        content,
        filename,
        acl=acl,
        content_type=content_type,
        content_encoding=content_encoding,
    )
    STORAGE_EVENTS.append({"type": "upload", "filename": filename})

    # Upload archived version
    if archive:
        timestamp = local_today().isoformat()
        filename_archive = f"archive/{timestamp}/{filename}"
        __upload_file(
            content,
            filename_archive,
            acl=acl,
            content_type=content_type,
            content_encoding=content_encoding,
        )
        STORAGE_EVENTS.append(
            {
                "type": "archive",
                "original_filename": filename,
                "archived_filename": filename_archive,
            }
        )
        filenames.append(filename_archive)

    return filenames


def upload_file(  # noqa: PLR0913
    content: bytes,
    filename: str,
    *,
    content_type: str | None = None,
    content_encoding: str | None = None,
    change_notification: str | None = None,
    compare_fn: Callable[[bytes, bytes], bool] = simple_compare,
    acl: str | None = "public-read",
    create_cloudfront_invalidation: bool = False,
    archive: bool = True,
):
    """Upload a file to storage.

    This function does a number of things:

    - Checks if the file already exists in storage and if so, compares the contents of the file with the new content.
        - If the file does not exist in storage, it is uploaded.
        - If the file exists in storage, but the contents are different, the file is uploaded.
        - If the file exists in storage, but the contents are the same, the file is not uploaded.
    - If the file is uploaded, an optional CloudFront invalidation is created.
    - Optionally sends a change notification via Sentry if the file was uploaded.

    By default, the comparison between the old and new file is done using simple_compare, which simply tests equality.
    This can be overridden by passing in a custom comparison function.
    The comparison function must take two arguments, the old and new file contents, and return a boolean indicating whether the files are equal.

    Args:
        content (bytes): File content
        filename (str): Filename to upload
        content_type (str, optional): Content type to use when uploading. Defaults to None.
        change_notification (str, optional): Notification text that should be sent to Sentry if the file was updated. Defaults to None.
        compare_fn (Callable[[bytes, bytes], bool], optional): Function to use to compare existing file with new file. Defaults to ``simple_compare``.
        acl (str, optional): ACL to use when uploading. Defaults to ``"public-read"``.
        create_cloudfront_invalidation (bool, optional): Whether to create a CloudFront invalidation. Defaults to False.
    """
    # Parameter validation
    filename = filename.lstrip("/")

    # Read old file-like object to check for differences
    try:
        bio_old = _download_file(filename)
        bio_old.seek(0)
        file_exists = True
    except DownloadFailedException:
        bio_old = BytesIO()
        file_exists = False

    # Compare if file exists and if it has changed
    if file_exists and compare_fn(bio_old.read(), content):
        STORAGE_EVENTS.append({"type": "existed", "filename": filename})
        return

    # Upload file with ACL and content type
    _upload_file(
        content,
        filename,
        acl=acl,
        content_type=content_type,
        archive=archive,
    )

    # Create CloudFront invalidation
    if create_cloudfront_invalidation:
        _queue_cloudfront_invalidation(filename)

    # Notify
    if change_notification:
        sentry_sdk.capture_message(change_notification)


def upload_dataframe(  # noqa: PLR0913
    df: pd.DataFrame,
    filename: str,
    *,
    change_notification: str | None = None,
    compare_fn: Callable[[bytes, bytes], bool] = simple_compare,
    acl: str | None = "public-read",
    create_cloudfront_invalidation: bool = False,
    datawrapper_datetimes: bool = False,
    archive: bool = True,
):
    """Upload a dataframe to storage.

    This is a convenience function that serializes the dataframe to CSV in a consistent manner and uploads it to storage.
    See ``upload_file`` for more details.

    Args:
        df (pd.DataFrame): Dataframe to upload
        filename (str): Filename to upload
        change_notification (str, optional): Notification text that should be sent to Sentry if the file was updated. Defaults to None.
        compare_fn (Callable, optional): Function to use to compare existing file with new file. Defaults to ``simple_compare``.
        acl (str, optional): ACL to use when uploading. Defaults to ``"public-read"``.
        create_cloudfront_invalidation (bool, optional): Whether to create a CloudFront invalidation. Defaults to False.
    """

    # Convert datetime for datawrapper (no timezone support madge)
    if datawrapper_datetimes:
        for col in df.columns:
            # There's some different types of datetime columns,
            # like datetime64[ns, Europe/Berlin] and datetime64[ns, UTC]
            if not str(df[col].dtype).startswith("datetime64"):
                continue

            # Convert to Berlin timezone
            df[col] = df[col].dt.tz_convert("Europe/Berlin")

            # Convert to string
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False).encode("utf-8")

    # Upload file
    upload_file(
        write,
        filename,
        content_type="text/csv",
        content_encoding="utf-8",
        acl=acl,
        compare_fn=compare_fn,
        create_cloudfront_invalidation=create_cloudfront_invalidation,
        change_notification=change_notification,
        archive=archive,
    )


def _queue_cloudfront_invalidation(filename: str) -> Any:
    """Internal function to create a CloudFront invalidation"""

    # Make sure paths start with /
    filename = "/" + filename.lstrip("/")
    CLOUDFRONT_INVALIDATIONS_TO_CREATE.append(filename)


def run_cloudfront_invalidations(*, caller_reference: str | None = None):
    """Run CloudFront invalidations"""

    caller_reference = caller_reference or str(uuid4())

    if not CLOUDFRONT_INVALIDATIONS_TO_CREATE:
        return

    invalidation_path = common_prefix(CLOUDFRONT_INVALIDATIONS_TO_CREATE) + "*"

    if "/" not in invalidation_path:
        msg = f"CloudFront invalidation path is too broad: {invalidation_path}"
        raise Exception(msg)

    if not USE_LOCAL_STORAGE and cloudfront:
        STORAGE_EVENTS.append({"type": "invalidation", "path": invalidation_path})
        cloudfront.create_invalidation(
            DistributionId=CLOUDFRONT_ID,
            InvalidationBatch={
                "Paths": {
                    "Quantity": 1,
                    "Items": invalidation_path,
                },
                "CallerReference": caller_reference,
            },
        )

    CLOUDFRONT_INVALIDATIONS_TO_CREATE.clear()
