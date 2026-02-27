"""LANUK Pegelstände scraper.

Fetches current water level data from all LANUK stations in NRW.

Ported from pegelstaende-pipeline-nrw (simplified: no BigQuery, no intermediate stages).
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from pydantic import BaseModel, ValidationError, field_validator

from ddj_cloud.scrapers.lanuk_karte.common import WARNSTUFE_COLORS, StationRow
from ddj_cloud.utils.date_and_time import BERLIN, local_now

logger = logging.getLogger(__name__)

STATIONS_URL = "https://www.hochwasserportal.nrw/data/internet/layers/10/index.json"
BASE_URL = "https://www.hochwasserportal.nrw/data/internet/stations/"
VALID_STATION_TYPES = {
    "Infopegel",
    "Gewässerkundlicher Pegel",
    "Weiter Betreiber Infostufen",
    "Weiterer Betreiber Normal",
}
REQUEST_DELAY = 0.5  # seconds between per-station requests

CACHE_DIR = Path(__file__).parent / "cache"


# -- Pydantic model (ported from pegelstaende-pipeline-nrw) --


class Station(BaseModel):
    station_id: str
    station_no: str
    station_name: str
    site_no: str
    station_latitude: float
    station_longitude: float
    WTO_OBJECT: str  # water body name
    WEB_STATYPE: str  # "Infopegel" or "Gewässerkundlicher Pegel"
    LANUV_MHW: float | None = None
    LANUV_MNW: float | None = None
    LANUV_MW: float | None = None
    LANUV_Info_1: float | None = None
    LANUV_Info_2: float | None = None
    LANUV_Info_3: float | None = None

    @field_validator(
        "LANUV_MHW",
        "LANUV_MNW",
        "LANUV_MW",
        "LANUV_Info_1",
        "LANUV_Info_2",
        "LANUV_Info_3",
        mode="before",
    )
    @classmethod
    def convert_empty_or_zero_strings_to_none(cls, v: Any) -> float | None:
        if v in ("", "0.0"):
            return None
        return v

    @field_validator("station_name", mode="after")
    @classmethod
    def validate_station_name(cls, v: str) -> str:
        if v == "":
            msg = "station_name is empty"
            raise ValueError(msg)
        return v

    @field_validator("WTO_OBJECT", mode="after")
    @classmethod
    def validate_WTO_OBJECT(cls, v: str) -> str:
        if v == "":
            msg = "WTO_OBJECT is empty"
            raise ValueError(msg)
        return v

    @field_validator("WEB_STATYPE", mode="after")
    @classmethod
    def validate_WEB_STATYPE(cls, v: str) -> str:
        if v not in VALID_STATION_TYPES:
            msg = f"Invalid WEB_STATYPE: {v}"
            raise ValueError(msg)
        return v


# -- Caching (pattern from divi_intensivregister/common.py) --


def _fetch_json(session: requests.Session, url: str, cache_filename: str) -> tuple[Any, bool]:
    """Fetch JSON from *url*, using a local file cache when available.

    Returns ``(data, from_cache)`` so callers can skip rate-limit delays on
    cache hits.
    """
    cached = CACHE_DIR / cache_filename
    if cached.exists():
        logger.info("Using cached %s", cache_filename)
        return json.loads(cached.read_text()), True

    response = session.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cached.write_text(json.dumps(data))
    except Exception:
        pass  # read-only on Lambda

    return data, False


# -- Fetchers --


def _fetch_stations(session: requests.Session) -> list[Station]:
    """Fetch the station list, validate each entry, skip invalid ones."""
    raw_entries: list[dict[str, Any]]
    raw_entries, _ = _fetch_json(session, STATIONS_URL, "stations.json")

    stations: list[Station] = []
    for entry in raw_entries:
        if entry.get("WEB_STATYPE") not in VALID_STATION_TYPES:
            logger.warning("Skipping invalid station type: %s", entry.get("WEB_STATYPE"))
            continue

        try:
            stations.append(Station.model_validate(entry))
        except ValidationError:
            logger.exception(
                "Skipping invalid station: %s (%s)",
                entry.get("station_name", "?"),
                entry.get("station_id", "?"),
            )

    return stations


def _fetch_current_level(
    session: requests.Session, site_no: str, station_no: str
) -> tuple[float, datetime, bool]:
    """Fetch the most recent water level measurement from the week endpoint.

    Returns (value_cm, timestamp, from_cache) — values are ``None`` on failure.
    """
    url = f"{BASE_URL}{site_no}/{station_no}/S/week.json"
    cache_filename = f"week_{site_no}_{station_no}.json"
    payload: list[dict[str, Any]]
    payload, from_cache = _fetch_json(session, url, cache_filename)

    if not payload:
        msg = "No payload"
        raise RuntimeError(msg)

    # week.json contains one entry with columns "Timestamp,Value"
    for entry in payload:
        if entry.get("columns") == "Timestamp,Value":
            data = entry.get("data", [])
            if data:
                ts_str, value = data[-1]
                return float(value), datetime.fromisoformat(ts_str), from_cache

    msg = "No data"
    raise RuntimeError(msg)


def _build_pegel_url(station_id: str, station_name: str) -> str:
    return (
        f"https://www.hochwasserportal.nrw/webpublic/#/overview/Wasserstand"
        f"/station/{station_id}/{station_name}/Wasserstand/"
    )


def _tooltip_texts(
    station: Station, value: float, timestamp: datetime, has_info_levels: bool
) -> dict[str, str]:
    # Pre-formatted display columns for Datawrapper tooltips
    display_wasserstand = f"{value:.0f} cm"
    display_messzeitpunkt = timestamp.strftime("%d.%m.%Y, %H:%M Uhr")

    if has_info_levels:
        parts = []
        if station.LANUV_Info_1 is not None:
            parts.append(f"Warnstufe 1: {station.LANUV_Info_1:.0f} cm")
        if station.LANUV_Info_2 is not None:
            parts.append(f"Warnstufe 2: {station.LANUV_Info_2:.0f} cm")
        if station.LANUV_Info_3 is not None:
            parts.append(f"Warnstufe 3: {station.LANUV_Info_3:.0f} cm")
        display_info = " · ".join(parts)
    else:
        display_info = "Keine Hochwasser-Warnstufen vorhanden"

    stats_parts = []
    if station.LANUV_MNW is not None:
        stats_parts.append(f"MNW: {station.LANUV_MNW:.0f}")
    if station.LANUV_MW is not None:
        stats_parts.append(f"MW: {station.LANUV_MW:.0f}")
    if station.LANUV_MHW is not None:
        stats_parts.append(f"MHW: {station.LANUV_MHW:.0f}")
    display_stats = (" · ".join(stats_parts) + " cm") if stats_parts else ""

    return {
        "display_wasserstand": display_wasserstand,
        "display_messzeitpunkt": display_messzeitpunkt,
        "display_info": display_info,
        "display_stats": display_stats,
    }


def run(session: requests.Session) -> list[StationRow]:
    now = local_now()

    logger.info("Fetching LANUK station list...")
    stations = _fetch_stations(session)
    logger.info("Found %d valid LANUK stations", len(stations))

    rows: list[StationRow] = []

    for station in stations:
        try:
            value, timestamp, from_cache = _fetch_current_level(
                session, station.site_no, station.station_no
            )
            timestamp = timestamp.astimezone(BERLIN)  # Normalize to Berlin time
        except Exception:
            logger.exception(
                "Failed to fetch water level for %s (%s)",
                station.station_name,
                station.station_id,
            )
            continue

        use_info_levels = (
            any((station.LANUV_Info_1, station.LANUV_Info_2, station.LANUV_Info_3))
            and station.WEB_STATYPE == "Infopegel"  # Ignore non-LANUK Infopegels
        )
        has_mw = station.LANUV_MW is not None

        if not use_info_levels and not has_mw:
            logger.warning(
                "Skipping station %s (%s) because it has neither info levels nor MW",
                station.station_name,
                station.station_id,
            )
            continue

        warnstufe: int = 0
        if (use_info_levels or has_mw) and value is not None:
            if (
                use_info_levels
                and station.LANUV_Info_3 is not None
                and value >= station.LANUV_Info_3
            ):
                warnstufe = 5
            elif (
                use_info_levels
                and station.LANUV_Info_2 is not None
                and value >= station.LANUV_Info_2
            ):
                warnstufe = 4
            elif (
                use_info_levels
                and station.LANUV_Info_1 is not None
                and value >= station.LANUV_Info_1
            ):
                warnstufe = 3
            elif has_mw and station.LANUV_MW is not None and value >= station.LANUV_MW:
                warnstufe = 2
            elif has_mw:
                warnstufe = 1
            else:
                warnstufe = 0  # has info but no MW, below info_1

        quelle = (
            "LANUK"
            if station.WEB_STATYPE in ("Gewässerkundlicher Pegel", "Infopegel")
            else "LANUK (Externer Betreiber)"
        )

        rows.append(
            StationRow(
                station_id=station.station_id,
                station_name=station.station_name,
                gewaesser=station.WTO_OBJECT,
                station_type=station.WEB_STATYPE,
                latitude=station.station_latitude,
                longitude=station.station_longitude,
                wasserstand_cm=value,
                messzeitpunkt=timestamp,
                info_1=station.LANUV_Info_1,
                info_2=station.LANUV_Info_2,
                info_3=station.LANUV_Info_3,
                mhw=station.LANUV_MHW,
                mnw=station.LANUV_MNW,
                mw=station.LANUV_MW,
                warnstufe=warnstufe,
                warnstufe_color=WARNSTUFE_COLORS[warnstufe],
                url_pegel=_build_pegel_url(station.station_id, station.station_name),
                quelle=quelle,
                abrufdatum=now,
                **_tooltip_texts(station, value, timestamp, use_info_levels),
            )
        )

        if not from_cache:
            time.sleep(REQUEST_DELAY)

    return rows
