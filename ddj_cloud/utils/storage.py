import os
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Optional, Union
from uuid import uuid4

import pandas as pd

from boto3 import client
import sentry_sdk

from ddj_cloud.utils.date_and_time import local_today

USE_LOCAL_STORAGE = os.environ.get("USE_LOCAL_STORAGE", False)
STORAGE_EVENTS = []

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


def simple_compare(old: Any, new: Any) -> bool:
    return old == new


def make_df_compare_fn(
    *, ignore_columns: Union[str, list[str], None] = None
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
            bio = BytesIO()
            s3.download_fileobj(BUCKET_NAME, filename, bio)

    except:
        raise DownloadFailedException(f"Failed to download file {filename}")

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
        STORAGE_EVENTS.append({"type": "download", "filename": filename, "found": False})
        raise

    STORAGE_EVENTS.append({"type": "download", "filename": filename, "found": True})

    return bio


def __upload_file(
    bio: BytesIO,
    filename: str,
    *,
    acl: Optional[str] = None,
    content_type: Optional[str] = None,
):
    """Internal file upload function"""
    if USE_LOCAL_STORAGE:
        # Ensure path exists
        (LOCAL_STORAGE_ROOT / filename).parent.mkdir(parents=True, exist_ok=True)

        with open(LOCAL_STORAGE_ROOT / filename, "wb") as fp:
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


def _upload_file(
    bio: BytesIO,
    filename: str,
    *,
    acl: Optional[str] = None,
    content_type: Optional[str] = None,
    archive: bool = True,
) -> list[str]:
    """Internal file upload function that performs optional achiving and storage event tracking"""
    filenames = [filename]

    # Upload file normally
    __upload_file(bio, filename, acl=acl, content_type=content_type)
    STORAGE_EVENTS.append({"type": "upload", "filename": filename})
    bio.seek(0)

    # Upload archived version
    if archive:
        timestamp = local_today().isoformat()
        filename_archive = f"archive/{timestamp}/{filename}"
        __upload_file(bio, filename_archive, acl=acl, content_type=content_type)
        STORAGE_EVENTS.append(
            {
                "type": "archive",
                "original_filename": filename,
                "archived_filename": filename_archive,
            }
        )
        filenames.append(filename_archive)

    return filenames


def upload_file(
    content: bytes,
    filename: str,
    *,
    content_type: Optional[str] = None,
    change_notification: Optional[str] = None,
    compare_fn: Callable[[bytes, bytes], bool] = simple_compare,
    acl: Optional[str] = "public-read",
    create_cloudfront_invalidation: bool = True,
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
        create_cloudfront_invalidation (bool, optional): Whether to create a CloudFront invalidation. Defaults to True.
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

    # Create new file-like object for upload
    bio_new = BytesIO(content)

    # Upload file with ACL and content type
    uploaded_files = _upload_file(
        bio_new,
        filename,
        acl=acl,
        content_type=content_type,
    )

    # Create CloudFront invalidation
    if create_cloudfront_invalidation:
        _create_cloudfront_invalidation(uploaded_files)

    # Notify
    if change_notification:
        sentry_sdk.capture_message(change_notification)


def upload_dataframe(
    df: pd.DataFrame,
    filename: str,
    *,
    change_notification: Optional[str] = None,
    compare_fn: Callable[[bytes, bytes], bool] = simple_compare,
    acl: Optional[str] = "public-read",
    create_cloudfront_invalidation: bool = True,
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
        create_cloudfront_invalidation (bool, optional): Whether to create a CloudFront invalidation. Defaults to True.
    """
    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False).encode("utf-8")

    # Upload file
    upload_file(
        write,
        filename,
        acl=acl,
        compare_fn=compare_fn,
        create_cloudfront_invalidation=create_cloudfront_invalidation,
        change_notification=change_notification,
    )


def _create_cloudfront_invalidation(
    filenames: Union[str, list[str]],
    *,
    caller_reference: Optional[str] = None,
) -> Any:
    """Internal function to create a CloudFront invalidation"""
    if isinstance(filenames, str):
        filenames = [filenames]

    # Make sure paths start with /
    filenames = ["/" + f.lstrip("/") for f in filenames]

    caller_reference = caller_reference or str(uuid4())

    if not USE_LOCAL_STORAGE and cloudfront:
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
