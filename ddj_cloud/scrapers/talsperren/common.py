from dataclasses import dataclass
import datetime as dt
from io import BytesIO
from typing import Callable, Generator, Iterable, Optional, TypeVar, Protocol, TypedDict
from zoneinfo import ZoneInfo
import pandas as pd

import sentry_sdk

TZ_UTC = ZoneInfo("UTC")
TZ_BERLIN = ZoneInfo("Europe/Berlin")


@dataclass
class ReservoirRecord:
    federation_name: str
    name: str
    ts_measured: dt.datetime
    capacity_mio_m3: float
    content_mio_m3: float


class ReservoirMeta(TypedDict):
    capacity_mio_m3: float
    lat: float
    lon: float


class Federation(Protocol):
    name: str

    reservoirs: dict[str, ReservoirMeta]

    def __init__(self) -> None:
        ...

    def get_data(
        self,
        *,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
    ) -> Iterable[ReservoirRecord]:
        ...


T1 = TypeVar("T1")
T2 = TypeVar("T2")


def apply_guarded(
    func: Callable[[T2], Optional[T1]],
    data: Iterable[T2],
) -> Generator[T1, None, None]:
    for item in data:
        try:
            result = func(item)
            if result is not None:
                yield result
        except Exception as e:
            print("Skipping due to error:")
            print(e)
            sentry_sdk.capture_exception(e)


def to_parquet_bio(df: pd.DataFrame, **kwargs) -> BytesIO:
    data: BytesIO = BytesIO()

    orig_close = data.close
    data.close = lambda: None
    try:
        df.to_parquet(data, engine="fastparquet", **kwargs)
    finally:
        data.close = orig_close

    return data


class Exporter(Protocol):
    filename: str

    def __init__(self) -> None:
        ...

    def run(self, df_base: pd.DataFrame) -> pd.DataFrame:
        ...
