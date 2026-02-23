"""LANUK Pegelstände locator map export.

Reads area markers (rivers) from a Datawrapper base chart, generates colored
point markers for each station based on warning level, and publishes the
combined result to a live chart.
"""

import logging
import os
from typing import Any
from uuid import uuid4 as uuid

import sentry_sdk
from datawrapper import Datawrapper

from ddj_cloud.scrapers.lanuk_karte.common import StationRow

logger = logging.getLogger(__name__)

DATAWRAPPER_TOKEN = os.environ.get("LANUK_KARTE_DATAWRAPPER_TOKEN")
CHART_ID_BASE = os.environ.get("LANUK_KARTE_CHART_ID_BASE")
CHART_ID_LIVE = os.environ.get("LANUK_KARTE_CHART_ID_LIVE")


def _make_tooltip(row: StationRow) -> str:
    """Build tooltip HTML for a station marker."""
    return (
        f"<b>{row.gewaesser}</b> &middot; {row.station_type}<br>"
        f"<big>{row.display_wasserstand}</big><br>"
        f"{row.display_messzeitpunkt}"
        f"<hr>"
        f"{row.display_info}<br>"
        f"{row.display_stats}"
    )


def _make_marker(row: StationRow) -> dict[str, Any]:
    """Create a visible Datawrapper locator-map point marker for a station."""

    return {
        "title": "",
        "id": str(uuid()),
        "type": "point",
        "icon": {
            "id": "circle",
            "path": "M1000 350a500 500 0 0 0-500-500 500 500 0 0 0-500 "
            "500 500 500 0 0 0 500 500 500 500 0 0 0 500-500z",
            "horiz-adv-x": 1000,
            "scale": 1,
            "height": 700,
            "width": 1000,
        },
        "scale": 0.5,
        "markerColor": row.warnstufe_color,
        "markerSymbol": "",
        "markerTextColor": "#333333",
        "anchor": "bottom-center",
        "offsetY": 0,
        "offsetX": 0,
        "labelStyle": "plain",
        "text": {
            "bold": False,
            "italic": False,
            "uppercase": False,
            "space": False,
            "color": "#333333",
            "fontSize": 14,
            "halo": "#f2f3f0",
        },
        "class": "",
        "rotate": 0,
        "visible": True,
        "locked": False,
        "preset": "-",
        "visibility": {"mobile": True, "desktop": True},
        "tooltip": {
            "enabled": True,
            "text": _make_tooltip(row),
        },
        "connectorLine": {
            "enabled": False,
            "arrowHead": "lines",
            "type": "curveRight",
            "targetPadding": 3,
            "stroke": 1,
            "lineLength": 0,
        },
        "coordinates": [
            row.longitude,
            row.latitude,
        ],
    }


def run(rows: list[StationRow]) -> None:
    """Update the Datawrapper locator map with current station data."""
    if DATAWRAPPER_TOKEN is None:
        logger.warning("LANUK_KARTE_DATAWRAPPER_TOKEN not set, skipping locator map export")
        return

    if CHART_ID_BASE is None or CHART_ID_LIVE is None:
        logger.warning("LANUK_KARTE_CHART_ID_BASE or LANUK_KARTE_CHART_ID_LIVE not set, skipping")
        return

    try:
        _process_chart(rows)
    except Exception:
        logger.exception("Failed to update locator map")
        sentry_sdk.capture_exception()


def _process_chart(rows: list[StationRow]) -> None:
    assert DATAWRAPPER_TOKEN is not None
    assert CHART_ID_BASE is not None
    assert CHART_ID_LIVE is not None

    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    chart_base = dw.get_chart(CHART_ID_BASE)
    assert chart_base is not None, "Base chart not found."
    assert chart_base["type"] == "locator-map", "Base chart is not a locator map."

    chart_live = dw.get_chart(CHART_ID_LIVE)
    assert chart_live is not None, "Live chart not found."
    assert chart_live["type"] == "locator-map", "Live chart is not a locator map."

    chart_data_base = dw.get_data(CHART_ID_BASE)
    chart_data_live = dw.get_data(CHART_ID_LIVE)

    # Keep area markers (rivers) from the base chart unchanged
    area_markers = [marker for marker in chart_data_base["markers"] if marker["type"] == "area"]

    # Generate fresh point markers for every station
    station_markers = [_make_marker(row) for row in rows]

    logger.info(
        "Generated %d station markers (%d area markers from base)",
        len(station_markers),
        len(area_markers),
    )

    new_chart_data_live = {
        **chart_data_live,
        "markers": area_markers + station_markers,
    }

    dw.add_json(CHART_ID_LIVE, new_chart_data_live)
    dw.publish_chart(CHART_ID_LIVE, display=False)

    logger.info("Published locator map to chart %s", CHART_ID_LIVE)
