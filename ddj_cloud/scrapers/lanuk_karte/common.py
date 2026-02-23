"""Shared data types for the LANUK Karte scraper."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class StationRow:
    station_id: str
    station_name: str
    gewaesser: str
    station_type: str
    latitude: float
    longitude: float
    wasserstand_cm: float | None
    messzeitpunkt: datetime | None
    info_1: float | None
    info_2: float | None
    info_3: float | None
    mhw: float | None
    mnw: float | None
    mw: float | None
    warnstufe: int | None
    url_pegel: str
    abrufdatum: datetime
    display_wasserstand: str
    display_messzeitpunkt: str
    display_info: str
    display_stats: str
