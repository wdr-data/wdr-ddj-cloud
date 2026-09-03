import datetime as dt
import re
from collections.abc import Iterator
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import sentry_sdk
from bs4 import BeautifulSoup
from bs4.element import Tag
from pydantic import BaseModel, TypeAdapter

from ddj_cloud.utils.date_and_time import local_today
from ddj_cloud.utils.storage import upload_dataframe

CURRENT_DIR = Path(__file__).parent


@dataclass(frozen=True)
class IndexSource:
    """A DWD fire-danger index and where to scrape it from."""

    key: str  # column prefix in the output CSV, e.g. "waldbrand"
    label: str  # human-readable name, used in log/error messages
    url: str


# Both pages are public (no auth) and share the exact same station set, layout
# and forecast dates -- one table per Bundesland.
INDEX_SOURCES = [
    IndexSource(
        key="wald",
        label="Waldbrandgefahrenindex",
        url="https://www.wettergefahren.de/warnungen/indizes/waldbrand.html",
    ),
    IndexSource(
        key="grasland",
        label="Graslandfeuerindex",
        url="https://www.wettergefahren.de/warnungen/indizes/grasland.html",
    ),
]

# Station coordinates are cached in ``stations.json``. Regenerate with
# ``update_stations.py`` when new stations appear (see that file for details).
STATIONS_PATH = CURRENT_DIR / "stations.json"

# Maps each danger level (1-5) to its short label and color, baked into the CSV
# so the Datawrapper tooltips can show colored labels instead of bare numbers.
DANGER_LEVELS_PATH = CURRENT_DIR / "danger_levels.json"

# Matches the day/month in a column header like "Di 07.07."
DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.")

# Extracts the state from a figcaption like "... im Bundesland Nordrhein-Westfalen."
BUNDESLAND_RE = re.compile(r"im Bundesland (.+?)\.?$")

NRW = "Nordrhein-Westfalen"

# Forecast columns should stay close to today's date plus their table offset.
DATE_SANITY_THRESHOLD_DAYS = 7

# Up to this many stations without coordinates (e.g. newly added by DWD) are
# tolerated: the maps are still uploaded and Sentry is notified. Beyond it, the
# drift is large enough that we hard-fail instead of publishing broken maps.
# The check runs over the country-wide station set, so this is sized accordingly.
MAX_TOLERATED_UNKNOWN_STATIONS = 10


class StationInfo(BaseModel):
    stationskennung: str
    latitude: float
    longitude: float


_STATIONS_ADAPTER = TypeAdapter(dict[str, StationInfo])


def _load_stations() -> dict[str, StationInfo]:
    return _STATIONS_ADAPTER.validate_json(STATIONS_PATH.read_bytes())


class DangerLevel(BaseModel):
    color: str
    label: str


_DANGER_LEVELS_ADAPTER = TypeAdapter(dict[int, DangerLevel])


def _load_danger_levels() -> dict[int, DangerLevel]:
    return _DANGER_LEVELS_ADAPTER.validate_json(DANGER_LEVELS_PATH.read_bytes())


@dataclass
class StationForecast:
    """One station's daily index values for a single index type."""

    bundesland: str
    indices: list[int | None]  # aligned with the page's forecast days


@dataclass
class IndexPage:
    """Everything parsed from a single index page."""

    date_columns: list[tuple[str, dt.date]]
    forecasts: dict[str, StationForecast]  # keyed by station name


def _header_to_date(header: str, *, expected_date: dt.date) -> dt.date:
    """Turn a column header like 'Di 07.07.' into an absolute date.

    The DWD table omits the year, so we infer it from the current date and
    the column's forecast offset.
    """
    match = DATE_RE.search(header)
    if not match:
        msg = f"Could not parse date from column header {header!r}."
        raise ValueError(msg)

    day, month = int(match.group(1)), int(match.group(2))
    candidates = []
    for year in range(expected_date.year - 1, expected_date.year + 2):
        try:
            candidate = dt.date(year, month, day)
        except ValueError:
            continue
        candidates.append(candidate)

    if not candidates:
        msg = f"Could not build a valid date from column header {header!r}."
        raise ValueError(msg)

    date = min(candidates, key=lambda candidate: abs(candidate - expected_date))

    days_from_expected = abs(date - expected_date).days
    if days_from_expected > DATE_SANITY_THRESHOLD_DAYS:
        msg = (
            f"Parsed date {date.isoformat()} from column header {header!r}, "
            f"but expected a date near {expected_date.isoformat()}."
        )
        raise ValueError(msg)

    return date


def _iter_tables(soup: BeautifulSoup) -> Iterator[tuple[Tag, pd.DataFrame]]:
    """Yield each ``<table>`` element together with its parsed DataFrame."""
    for table in soup.find_all("table"):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except ValueError:
            continue
        yield table, df


def _bundesland_of(table: Tag) -> str:
    figure = table.find_parent("figure")
    caption = figure.find("figcaption") if figure else None
    match = BUNDESLAND_RE.search(caption.get_text(strip=True)) if caption else None
    if not match:
        msg = "Could not determine the Bundesland for a station table (page layout changed?)."
        raise ValueError(msg)
    return match.group(1).strip()


def _parse_index_page(html: str, *, today: dt.date) -> IndexPage:
    soup = BeautifulSoup(html, "lxml")

    date_columns: list[tuple[str, dt.date]] | None = None
    forecasts: dict[str, StationForecast] = {}

    for table, df in _iter_tables(soup):
        header = str(df.columns[0]).strip().lower()

        # Skip everything that isn't a per-Bundesland station table (e.g. the legend).
        if header != "stationsname":
            continue

        bundesland = _bundesland_of(table)
        station_col = df.columns[0]
        # pandas repeats the header row in the table footer; drop those artifacts.
        df_stations = df[df[station_col].astype(str).str.strip() != "Stationsname"]

        # All state tables share the same forecast dates; compute them once.
        if date_columns is None:
            date_columns = [
                (col, _header_to_date(str(col), expected_date=today + dt.timedelta(days=offset)))
                for offset, col in enumerate(df_stations.columns[1:])
            ]

        for _, row in df_stations.iterrows():
            name = str(row[station_col]).strip()
            indices = [
                int(value) if pd.notna(value := pd.to_numeric(row[col], errors="coerce")) else None
                for col, _ in date_columns
            ]
            forecasts[name] = StationForecast(bundesland=bundesland, indices=indices)

    if date_columns is None:
        msg = "Could not find any station tables on the index page (page layout changed?)."
        raise ValueError(msg)

    return IndexPage(date_columns=date_columns, forecasts=forecasts)


def _fetch(url: str) -> str:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def _check_unknown_stations(station_names: set[str], stations: dict[str, StationInfo]) -> None:
    unknown_stations = sorted(name for name in station_names if name not in stations)
    if len(unknown_stations) > MAX_TOLERATED_UNKNOWN_STATIONS:
        msg = (
            f"No coordinates found for {len(unknown_stations)} stations "
            f"{unknown_stations}. Run update_stations.py to refresh stations.json."
        )
        raise ValueError(msg)
    if unknown_stations:
        # A handful of newly added stations shouldn't block the whole map. Upload
        # the rest (these rows go out without coordinates) and flag it so someone
        # refreshes stations.json.
        msg = (
            f"No coordinates found for stations {unknown_stations}. "
            "Uploading maps without them; run update_stations.py to refresh stations.json."
        )
        print(msg)
        sentry_sdk.capture_message(msg, level="warning")


def _bundesland_for(name: str, pages: dict[str, IndexPage]) -> str | None:
    for source in INDEX_SOURCES:
        forecast = pages[source.key].forecasts.get(name)
        if forecast is not None:
            return forecast.bundesland
    return None


def _build_dataframe(
    pages: dict[str, IndexPage],
    stations: dict[str, StationInfo],
    danger_levels: dict[int, DangerLevel],
) -> pd.DataFrame:
    # Both pages share the same stations and forecast dates; use the first source
    # as the canonical reference for the forecast dates.
    date_columns = pages[INDEX_SOURCES[0].key].date_columns
    reference_dates = [date for _, date in date_columns]

    for source in INDEX_SOURCES[1:]:
        other_dates = [date for _, date in pages[source.key].date_columns]
        if other_dates != reference_dates:
            msg = f"Forecast dates for {source.label} do not match {INDEX_SOURCES[0].label}."
            raise ValueError(msg)

    station_names = sorted(
        {name for page in pages.values() for name in page.forecasts},
        key=lambda name: (_bundesland_for(name, pages) or "", name),
    )

    records = []
    for name in station_names:
        coords = stations.get(name)

        record: dict = {
            "bundesland": _bundesland_for(name, pages),
            "stationsname": name,
            "stationskennung": coords.stationskennung if coords else None,
            "latitude": coords.latitude if coords else None,
            "longitude": coords.longitude if coords else None,
        }

        for offset, (_, date) in enumerate(date_columns):
            record[f"datum_tag{offset}"] = date.strftime("%d.%m.%Y")

            for source in INDEX_SOURCES:
                forecast = pages[source.key].forecasts.get(name)
                index = forecast.indices[offset] if forecast else None
                level = danger_levels.get(index) if index is not None else None
                record[f"{source.key}_index_tag{offset}"] = index
                record[f"{source.key}_stufe_tag{offset}"] = level.label if level else None
                record[f"{source.key}_farbe_tag{offset}"] = level.color if level else None

        records.append(record)

    df = pd.DataFrame.from_records(records)

    # Nullable integer type so missing values stay empty (not 0.0) in the CSV.
    for source in INDEX_SOURCES:
        for offset in range(len(date_columns)):
            col = f"{source.key}_index_tag{offset}"
            df[col] = df[col].astype("Int64")

    return df


def run():
    today = local_today()
    stations = _load_stations()
    danger_levels = _load_danger_levels()

    pages = {
        source.key: _parse_index_page(_fetch(source.url), today=today) for source in INDEX_SOURCES
    }

    all_station_names = {name for page in pages.values() for name in page.forecasts}
    _check_unknown_stations(all_station_names, stations)

    df = _build_dataframe(pages, stations, danger_levels)
    df_nrw = df[df["bundesland"] == NRW]

    n_days = len(pages[INDEX_SOURCES[0].key].date_columns)
    print(
        f"Parsed {len(df)} stations ({len(df_nrw)} in NRW) across {n_days} forecast days "
        f"for {len(INDEX_SOURCES)} indices."
    )

    upload_dataframe(df, "dwd_brandgefahrenindizes/brandgefahrenindizes_de.csv")
    upload_dataframe(df_nrw, "dwd_brandgefahrenindizes/brandgefahrenindizes_nrw.csv")
