"""Pegelstände NRW Karte — orchestrator.

Fetches current water level data from LANUK and EGLV stations and uploads
a combined CSV suitable for a Datawrapper map.
"""

import dataclasses
import logging

import pandas as pd
import requests

from ddj_cloud.scrapers.lanuk_karte import eglv, lanuk
from ddj_cloud.scrapers.lanuk_karte.common import STATION_TYPE_DISPLAY, clean_station_name
from ddj_cloud.scrapers.lanuk_karte.geo_filter import is_in_nrw
from ddj_cloud.utils.storage import upload_dataframe

logger = logging.getLogger(__name__)


def run():
    session = requests.Session()

    lanuk_rows = lanuk.run(session)
    eglv_rows = eglv.run(session)
    all_rows = lanuk_rows + eglv_rows

    for row in all_rows:
        row.station_name = clean_station_name(row.station_name)
        row.station_type = STATION_TYPE_DISPLAY.get(row.station_type, "Gewöhnlicher Pegel")

    filtered_rows = [row for row in all_rows if is_in_nrw(row.latitude, row.longitude)]

    df = pd.DataFrame([dataclasses.asdict(row) for row in filtered_rows])
    df.drop(columns=["warnstufe_color"], inplace=True)

    upload_dataframe(
        df,
        "lanuk-karte/data.csv",
        datawrapper_datetimes=True,
    )

    logger.info(
        "Uploaded data for %d stations total (%d LANUK, %d EGLV), %d filtered outside NRW",
        len(filtered_rows),
        len(lanuk_rows),
        len(eglv_rows),
        len(all_rows) - len(filtered_rows),
    )

    # Locator map: only stations with info levels (not zoomable)
    # Not currently being used
    # from ddj_cloud.scrapers.lanuk_karte import locator_map
    # if os.environ.get("LANUK_KARTE_DATAWRAPPER_TOKEN"):
    #     rows_locator = [row for row in lanuk_rows if any((row.info_1, row.info_2, row.info_3))]
    #     locator_map.run(rows_locator)
