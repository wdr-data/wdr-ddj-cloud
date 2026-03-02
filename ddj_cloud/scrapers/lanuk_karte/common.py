"""Shared data types and constants for the LANUK Karte scraper."""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

STATION_TYPE_DISPLAY: dict[str, str] = {
    "Infopegel": "Hochwasser-Warnpegel",
    "Gewässerkundlicher Pegel": "Gewöhnlicher Pegel",
    "Weiter Betreiber Infostufen": "Gewöhnlicher Pegel",
    "Weiterer Betreiber Normal": "Gewöhnlicher Pegel",
}


def clean_station_name(name: str) -> str:
    """Clean up a station name for display.

    - Replace underscores with spaces
    - Remove 'WSV' prefix
    - Remove operator suffixes like '(Agger Verband)'
    - Remove suffixes like 'neu'
    - Separate trailing numbers with a space (e.g. 'Altenbeken2' -> 'Altenbeken 2')
    - Restore written-out umlauts (ue -> ü, oe -> ö, ae -> ä)
    """
    name = name.replace("_", " ")
    # Remove WSV prefix
    name = re.sub(r"^WSV\s+", "", name)
    # Remove operator suffixes in parentheses
    name = re.sub(r"\s*\((?:Agger\s*Verband|Ruhrverband)\)", "", name, flags=re.IGNORECASE)
    # Remove trailing ' neu' suffix
    name = re.sub(r"\s+neu$", "", name, flags=re.IGNORECASE)
    # Add space before trailing number(s) if directly attached to letters
    name = re.sub(r"([A-Za-zäöüÄÖÜß])(\d+)$", r"\1 \2", name)
    # Same for "VA" suffix
    name = re.sub(r"([A-Za-zäöüÄÖÜß])VA$", r"\1 VA", name)
    # Replace "." with " " if directly attached to letters on both sides
    name = re.sub(r"([A-Za-zäöüÄÖÜß])\.([A-Za-zäöüÄÖÜß])", r"\1 \2", name)
    # Restore written-out umlauts (only when followed by at least one more letter,
    # and not preceded by 'a'/'q' for 'ue' to preserve 'aue'/'que')
    name = re.sub(r"(?<![aqAQ])ue(?=[a-zäöüß])", "ü", name)
    name = re.sub(r"(?<![aqAQ])Ue(?=[a-zäöüß])", "Ü", name)
    name = re.sub(r"oe(?=[a-zäöüß])", "ö", name)
    name = re.sub(r"Oe(?=[a-zäöüß])", "Ö", name)
    name = re.sub(r"ae(?=[a-zäöüß])", "ä", name)
    name = re.sub(r"Ae(?=[a-zäöüß])", "Ä", name)
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
    stats_period: str | None
    warnstufe: int
    warnstufe_color: str
    url_pegel: str
    abrufdatum: datetime
    quelle: str
    operator: str
    display_wasserstand: str
    display_messzeitpunkt: str
    display_info: str
    display_stats: str
