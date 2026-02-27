"""Shared data types and constants for the LANUK Karte scraper."""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

STATION_TYPE_DISPLAY: dict[str, str] = {
    "Infopegel": "Hochwasser-Meldepegel",
    "Gewässerkundlicher Pegel": "Gewöhnlicher Pegel",
    "Weiter Betreiber Infostufen": "Gewöhnlicher Pegel",  # Maybe use "Infopegel" instead?
    "Weiterer Betreiber Normal": "Gewöhnlicher Pegel",
}


def clean_station_name(name: str) -> str:
    """Clean up a station name for display.

    - Replace underscores with spaces
    - Remove 'WSV' prefix
    - Remove suffixes like 'neu'
    - Separate trailing numbers with a space (e.g. 'Altenbeken2' -> 'Altenbeken 2')
    """
    name = name.replace("_", " ")
    # Remove WSV prefix
    name = re.sub(r"^WSV\s+", "", name)
    # Remove trailing ' neu' suffix
    name = re.sub(r"\s+neu$", "", name, flags=re.IGNORECASE)
    # Add space before trailing number(s) if directly attached to letters
    name = re.sub(r"([A-Za-zäöüÄÖÜß])(\d+)$", r"\1 \2", name)
    # Replace "." with " " if directly attached to letters on both sides
    name = re.sub(r"([A-Za-zäöüÄÖÜß])\.([A-Za-zäöüÄÖÜß])", r"\1 \2", name)
    return name.strip()


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
