"""Shared data types and constants for the LANUK Karte scraper."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

WARNSTUFE_COLORS: dict[Literal[0, 1, 2, 3, 4, 5], str] = {
    0: "#f0f9e8",  # has info levels but no MW, below info_1
    1: "#aadebd",  # under MW
    2: "#66bbc7",  # over MW (always below info_1)
    3: "#3895c2",  # over info 1
    4: "#1370b0",  # over info 2
    5: "#254b8c",  # over info 3
}


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
    warnstufe: int
    warnstufe_color: str
    url_pegel: str
    abrufdatum: datetime
    quelle: str
    display_wasserstand: str
    display_messzeitpunkt: str
    display_info: str
    display_stats: str
