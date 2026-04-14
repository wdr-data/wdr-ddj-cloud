import csv
import json
import os
import shutil
from collections.abc import Callable
from io import BytesIO, UnsupportedOperation
from os.path import commonprefix as common_prefix
from pathlib import Path
from typing import Any, BinaryIO, overload
from uuid import uuid4

import pandas as pd
import sentry_sdk
from boto3 import client
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, ValidationError

from ddj_cloud.utils.date_and_time import local_today

USE_LOCAL_STORAGE = os.environ.get("USE_LOCAL_STORAGE", None)
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

        elif fs_event["type"] == "delete":
            return f'Deleted file "{fs_event["filename"]}" from storage'

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


class StorageMetadata(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    ident: str


def _download_into(filename: str, fileobj: BinaryIO) -> None:
    """Stream a file from storage into a caller-provided file-like object."""
    try:
        if USE_LOCAL_STORAGE:
            with open(LOCAL_STORAGE_ROOT / filename, "rb") as fp:
                shutil.copyfileobj(fp, fileobj)
        else:
            assert s3 is not None
            s3.download_fileobj(BUCKET_NAME, filename, fileobj)
    except Exception as err:
        msg = f"Failed to download file {filename}"
        raise DownloadFailedException(msg) from err


def _download_file(filename: str) -> BytesIO:
    """Internal file download function"""
    bio = BytesIO()
    _download_into(filename, bio)
    bio.seek(0)
    return bio


@overload
def download_file(filename: str) -> BytesIO: ...
@overload
def download_file(filename: str, fileobj: BinaryIO) -> None: ...


def download_file(filename: str, fileobj: BinaryIO | None = None) -> BytesIO | None:
    """Download a file from storage.

    If the file was not found or some other error occurred, a ``DownloadFailedException`` will be raised.

    Args:
        filename (str): Filename to download
        fileobj (BinaryIO, optional): If provided, the file contents are streamed directly
            into this file-like object. Use this for large files to avoid loading the full
            payload into memory. The caller owns the stream's position afterward.

    Returns:
        BytesIO | None: When ``fileobj`` is not provided, returns a ``BytesIO`` with the
        file contents (seek position 0). When ``fileobj`` is provided, returns ``None``.
    """
    try:
        if fileobj is not None:
            _download_into(filename, fileobj)
            result = None
        else:
            result = _download_file(filename)
    except DownloadFailedException:
        STORAGE_EVENTS.append({"type": "download", "filename": filename, "success": False})
        raise

    STORAGE_EVENTS.append({"type": "download", "filename": filename, "success": True})

    return result


def list_files(prefix: str) -> list[str]:
    """List files in storage under a given prefix.

    The returned filenames match the keys you'd pass to ``download_file`` or
    ``delete_file``. The local-storage metadata registry (``_metadata.json``) is
    excluded.

    ``prefix`` is required to reduce the likelihood of accidentally listing the whole
    bucket (which includes a potentially large archive). Pass an empty-ish prefix
    deliberately (e.g., ``"archive/"``) if that's really what you want.

    Args:
        prefix (str): Only return filenames that start with this prefix. A leading
            ``/`` is stripped. Must be non-empty.

    Returns:
        list[str]: Sorted list of filenames.
    """
    prefix = prefix.lstrip("/")
    if not prefix:
        msg = "list_files requires a non-empty prefix"
        raise ValueError(msg)

    if USE_LOCAL_STORAGE:
        prefix_path = LOCAL_STORAGE_ROOT / prefix
        # Narrow the rglob root to the deepest existing directory along the prefix so we
        # don't walk unrelated trees (e.g., the whole archive) when filtering a small slice.
        if prefix_path.is_dir():
            base = prefix_path
        elif prefix_path.parent.is_dir():
            base = prefix_path.parent
        else:
            return []

        files: list[str] = []
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(LOCAL_STORAGE_ROOT).as_posix()
            if rel == _LOCAL_METADATA_REGISTRY_NAME:
                continue
            if rel.startswith(prefix):
                files.append(rel)
        return sorted(files)

    assert s3 is not None
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return sorted(keys)


def delete_file(filename: str) -> None:
    """Delete a file from storage.

    The operation is idempotent — if the file doesn't exist, nothing happens. In
    local-storage mode, any associated metadata registry entry is also removed.

    Args:
        filename (str): Filename to delete.
    """
    filename = filename.lstrip("/")

    if USE_LOCAL_STORAGE:
        (LOCAL_STORAGE_ROOT / filename).unlink(missing_ok=True)
        _delete_local_metadata(filename)
    else:
        assert s3 is not None
        s3.delete_object(Bucket=BUCKET_NAME, Key=filename)

    STORAGE_EVENTS.append({"type": "delete", "filename": filename})


def _rewind_if_seekable(fileobj: BinaryIO) -> None:
    """Rewind seekable streams so uploads read the full payload by default."""
    try:
        if fileobj.seekable():
            fileobj.seek(0)
    except (AttributeError, OSError, UnsupportedOperation):
        pass


def _upload_file(  # noqa: PLR0913
    content: bytes | BinaryIO,
    filename: str,
    *,
    acl: str | None = None,
    content_type: str | None = None,
    metadata: StorageMetadata | None = None,
    rewind: bool = True,
):
    """Internal file upload function"""
    is_bytes = isinstance(content, (bytes, bytearray))
    source: BinaryIO = BytesIO(content) if is_bytes else content
    if rewind:
        _rewind_if_seekable(source)

    try:
        if USE_LOCAL_STORAGE:
            # Ensure path exists
            (LOCAL_STORAGE_ROOT / filename).parent.mkdir(parents=True, exist_ok=True)

            with open(LOCAL_STORAGE_ROOT / filename, "wb") as fp:
                shutil.copyfileobj(source, fp)

            # Persist object metadata in local storage so skip-if-unchanged works in dev.
            if metadata:
                _save_local_metadata(filename, metadata)
            else:
                _delete_local_metadata(filename)

        else:
            # Upload file with ACL, content type, and user metadata
            extra_args: dict[str, Any] = {}

            if acl is not None:
                extra_args["ACL"] = acl

            if content_type is not None:
                extra_args["ContentType"] = content_type

            if metadata:
                extra_args["Metadata"] = metadata.model_dump()

            assert s3 is not None
            s3.upload_fileobj(
                source,
                BUCKET_NAME,
                filename,
                ExtraArgs=extra_args,
            )
    finally:
        if is_bytes:
            source.close()


def _archive_file(source: str, dest: str, acl: str | None) -> None:
    """Copy an already-uploaded object to an archive location without re-reading the payload."""
    if USE_LOCAL_STORAGE:
        src_path = LOCAL_STORAGE_ROOT / source
        dest_path = LOCAL_STORAGE_ROOT / dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dest_path)

        metadata = _fetch_local_metadata(source)
        if metadata:
            _save_local_metadata(dest, metadata)
        else:
            _delete_local_metadata(dest)
        return

    assert s3 is not None
    extra_args: dict[str, Any] = {}
    if acl is not None:
        extra_args["ACL"] = acl
    # boto3's high-level s3.copy() handles multipart copy for objects >5GB automatically
    # and preserves user metadata (including ident) by default.
    s3.copy(
        CopySource={"Bucket": BUCKET_NAME, "Key": source},
        Bucket=BUCKET_NAME,
        Key=dest,
        ExtraArgs=extra_args,
    )


_LOCAL_METADATA_REGISTRY_NAME = "_metadata.json"


def _load_local_metadata() -> dict[str, StorageMetadata]:
    path = LOCAL_STORAGE_ROOT / _LOCAL_METADATA_REGISTRY_NAME
    try:
        raw_data: Any = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}

    if not isinstance(raw_data, dict):
        return {}

    metadata_by_filename: dict[str, StorageMetadata] = {}
    for filename, metadata in raw_data.items():
        if not isinstance(filename, str):
            continue

        try:
            metadata_by_filename[filename] = StorageMetadata.model_validate(metadata, strict=True)
        except ValidationError:
            continue

    return metadata_by_filename


def _write_local_metadata_store(data: dict[str, StorageMetadata]) -> None:
    LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    path = LOCAL_STORAGE_ROOT / _LOCAL_METADATA_REGISTRY_NAME
    serialized_data = {filename: metadata.model_dump() for filename, metadata in data.items()}
    path.write_text(json.dumps(serialized_data, indent=2, sort_keys=True))


def _save_local_metadata(filename: str, metadata: StorageMetadata) -> None:
    data = _load_local_metadata()
    data[filename] = metadata
    _write_local_metadata_store(data)


def _delete_local_metadata(filename: str) -> None:
    data = _load_local_metadata()
    if filename not in data:
        return

    del data[filename]
    _write_local_metadata_store(data)


def _fetch_local_metadata(filename: str) -> StorageMetadata | None:
    if not (LOCAL_STORAGE_ROOT / filename).exists():
        _delete_local_metadata(filename)
        return None

    return _load_local_metadata().get(filename)


def _fetch_metadata(filename: str) -> StorageMetadata | None:
    """Return the user metadata stored on an existing object, or None.

    For S3, reads user metadata via ``head_object``. For local storage, reads from a
    JSON registry at ``local_storage/_metadata.json`` and drops stale entries whose
    underlying file no longer exists.
    """
    if USE_LOCAL_STORAGE:
        return _fetch_local_metadata(filename)
    if s3 is None:
        return None
    try:
        response = s3.head_object(Bucket=BUCKET_NAME, Key=filename)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        error_code = error.get("Code")
        status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in {"404", "NoSuchKey", "NotFound"} or status_code == 404:
            return None
        raise

    metadata = response.get("Metadata")
    if not isinstance(metadata, dict):
        return None

    try:
        return StorageMetadata.model_validate(metadata, strict=True)
    except ValidationError:
        return None


def _fetch_ident(filename: str) -> str | None:
    metadata = _fetch_metadata(filename)
    if metadata is None:
        return None
    return metadata.ident


def _upload_and_archive_file(  # noqa: PLR0913
    content: bytes | BinaryIO,
    filename: str,
    *,
    acl: str | None = None,
    content_type: str | None = None,
    archive: bool = True,
    metadata: StorageMetadata | None = None,
    rewind: bool = True,
) -> list[str]:
    """Internal file upload function that performs optional archiving and storage event tracking."""
    filenames = [filename]

    # Upload file normally
    _upload_file(
        content,
        filename,
        acl=acl,
        content_type=content_type,
        metadata=metadata,
        rewind=rewind,
    )
    STORAGE_EVENTS.append({"type": "upload", "filename": filename})

    # Archive via server-side copy (S3) or filesystem copy (local)
    if archive:
        timestamp = local_today().isoformat()
        filename_archive = f"archive/{timestamp}/{filename}"
        _archive_file(filename, filename_archive, acl=acl)
        STORAGE_EVENTS.append(
            {
                "type": "archive",
                "original_filename": filename,
                "archived_filename": filename_archive,
            }
        )
        filenames.append(filename_archive)

    return filenames


@overload
def upload_file(
    content: bytes,
    filename: str,
    *,
    content_type: str | None = None,
    change_notification: str | None = None,
    compare_fn: Callable[[bytes, bytes], bool] = simple_compare,
    acl: str | None = "public-read",
    create_cloudfront_invalidation: bool = False,
    archive: bool = True,
) -> None: ...


@overload
def upload_file(
    content: BinaryIO,
    filename: str,
    *,
    content_type: str | None = None,
    change_notification: str | None = None,
    ident: str | None = None,
    acl: str | None = "public-read",
    create_cloudfront_invalidation: bool = False,
    archive: bool = True,
    rewind: bool = True,
) -> None: ...


def upload_file(  # noqa: PLR0913
    content: bytes | BinaryIO,
    filename: str,
    *,
    content_type: str | None = None,
    change_notification: str | None = None,
    compare_fn: Callable[[bytes, bytes], bool] = simple_compare,
    ident: str | None = None,
    acl: str | None = "public-read",
    create_cloudfront_invalidation: bool = False,
    archive: bool = True,
    rewind: bool = True,
):
    """Upload a file to storage.

    This function does a number of things:

    - Checks if the file already exists in storage and if so, compares against the new content.
        - If the file does not exist in storage, it is uploaded.
        - If the file exists in storage, but differs, the new version is uploaded.
        - If the file exists in storage and matches, the upload is skipped.
    - If the file is uploaded, an optional CloudFront invalidation is queued.
    - Optionally sends a change notification via Sentry if the file was uploaded.

    Two modes are supported:

    - **Bytes** (default): ``content`` is ``bytes``. Comparison uses ``compare_fn``
      (``simple_compare`` equality by default), which can be customized.
    - **Streaming**: ``content`` is a binary file-like object. Use this for large files
      (multi-GB) to avoid loading the payload into memory. Seekable streams are rewound
      to position 0 before upload (override with ``rewind=False`` if the caller has
      deliberately positioned the stream). Comparison uses an optional caller-provided
      ``ident`` string stored as S3 user metadata (``x-amz-meta-ident``): if the existing
      object's ``ident`` matches, the upload is skipped. If ``ident`` is not provided,
      the file is always uploaded. Local-storage mode tracks metadata in
      ``local_storage/_metadata.json`` so skip-if-unchanged works in dev.

    Args:
        content (bytes | BinaryIO): File content as bytes, or a readable binary stream.
        filename (str): Filename to upload.
        content_type (str, optional): Content type to use when uploading. Defaults to None.
        change_notification (str, optional): Notification text sent to Sentry if the file was updated.
        compare_fn (Callable[[bytes, bytes], bool], optional): Bytes-mode only — function to compare
            existing file with new file. Defaults to ``simple_compare``.
        ident (str, optional): Streaming-mode only — caller-provided identifier used to skip
            re-uploads. Stored as S3 user metadata. Defaults to None (always upload).
        acl (str, optional): ACL to use when uploading. Defaults to ``"public-read"``.
        create_cloudfront_invalidation (bool, optional): Whether to queue a CloudFront invalidation.
        archive (bool, optional): Whether to archive the file under ``archive/<date>/<filename>``.
            The archive is created via server-side copy (S3) or filesystem copy (local) —
            the payload is never re-uploaded. Defaults to True.
        rewind (bool, optional): Streaming-mode only — rewind seekable streams to position
            0 before upload. Set to False to upload from the stream's current position
            (e.g., to skip a header). Defaults to True.
    """
    # Parameter validation
    filename = filename.lstrip("/")
    is_bytes = isinstance(content, (bytes, bytearray))

    # Skip-if-unchanged check
    if is_bytes:
        try:
            bio_old = _download_file(filename)
            if compare_fn(bio_old.read(), content):
                STORAGE_EVENTS.append({"type": "existed", "filename": filename})
                return
        except DownloadFailedException:
            pass
    elif ident is not None and _fetch_ident(filename) == ident:
        STORAGE_EVENTS.append({"type": "existed", "filename": filename})
        return

    metadata = StorageMetadata(ident=ident) if (not is_bytes and ident is not None) else None

    # Upload file with ACL, content type, and optional ident metadata
    _upload_and_archive_file(
        content,
        filename,
        acl=acl,
        content_type=content_type,
        archive=archive,
        metadata=metadata,
        rewind=rewind,
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
            # But we ignore naive datetimes (apparently used for dates as well)
            if getattr(df[col].dtype, "tz", None) is None:
                continue

            # Convert to Berlin timezone
            df[col] = df[col].dt.tz_convert("Europe/Berlin")

            # Convert to string
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC).encode("utf-8")

    # Upload file
    upload_file(
        write,
        filename,
        content_type="text/csv",
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
