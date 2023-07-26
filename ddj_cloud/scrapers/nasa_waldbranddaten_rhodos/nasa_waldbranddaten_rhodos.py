import os
from pathlib import Path
import datetime as dt
import json
from typing import Union
from zoneinfo import ZoneInfo

import pandas as pd

from ddj_cloud.utils.storage import upload_dataframe
from ddj_cloud.utils.datawrapper_patched import Datawrapper

MAP_KEY = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_MAP_KEY")
DATAWRAPPER_TOKEN = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_DATAWRAPPER_TOKEN")
CHART_ID = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_CHART_ID")

CURRENT_DIR = Path(__file__).parent
NASA_API_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api"
INSTRUMENT = "VIIRS_SNPP_NRT"

MAP_EXTENT_NW = [36.488223, 27.486474]
MAP_EXTENT_SE = [35.851543, 28.307467]

MAP_EXTENT = [
    MAP_EXTENT_NW[1],
    MAP_EXTENT_SE[1],
    MAP_EXTENT_SE[0],
    MAP_EXTENT_NW[0],
]  # West, South, East, North


def make_nasa_data_url(date: Union[dt.date, str]):
    if isinstance(date, str):
        date = dt.date.fromisoformat(date)

    map_extent = ",".join(map(str, MAP_EXTENT))

    url = f"{NASA_API_BASE_URL}/area/csv/{MAP_KEY}/{INSTRUMENT}/{map_extent}/1/{date.isoformat()}"
    return url


def run():
    # Check data availability
    url = f"{NASA_API_BASE_URL}/data_availability/csv/{MAP_KEY}/{INSTRUMENT}"
    df_avail = pd.read_csv(url, sep=",", decimal=".", low_memory=False)
    print(df_avail)
    latest_data_day: str = df_avail.loc[df_avail["data_id"] == INSTRUMENT, "max_date"].values[0]
    print("Latest data day:", latest_data_day)

    # Get the data for the latest day
    url = make_nasa_data_url(latest_data_day)
    df = pd.read_csv(url, sep=",", decimal=".", low_memory=False)

    print(f"Got data for {latest_data_day}!")

    if df is None:
        raise Exception("Borked.")

    # Filter by confidence (nominal and high)
    df = df.loc[df["confidence"].isin(["n", "h"])]

    # Upload the data
    upload_dataframe(df, "nasa_waldbranddaten_rhodos/nasa_data.csv")

    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    print("Datawrapper account info:", dw.account_info())

    props = dw.chart_properties(CHART_ID)
    # print(json.dumps(props))
    assert props is not None, "Chart not found."
    assert isinstance(props, dict), "Chart properties are not a dict."
    assert props["type"] == "locator-map", "Chart is not a locator map."

    markers = []

    for _, row in df.iterrows():
        marker = {
            "type": "point",
            "title": "",
            "coordinates": [row["longitude"], row["latitude"]],
            "markerColor": "#be0000",
            "scale": 0.3,
            "icon": {"path": "M0 1000h1000v-1000h-1000z", "height": 1000, "width": 1000},
            "anchor": "bottom-right",
            "offsetY": 0,
            "offsetX": 0,
            "visible": True,
            "visibility": {"mobile": True, "desktop": True},
            "tooltip": {
                "enabled": False,
                "text": "",
            },
            "text": {},
        }
        markers.append(marker)

    with open(CURRENT_DIR / "static_markers.json") as fp:
        static_markers = json.load(fp)

    resp = dw.add_data_json(CHART_ID, {"markers": [*static_markers, *markers]})
    resp.raise_for_status()

    print("Updated markers.")

    # Update notes
    day_formatted = dt.date.fromisoformat(latest_data_day).strftime("%d.%m.%Y")
    now_berlin = dt.datetime.now(tz=ZoneInfo("Europe/Berlin")).strftime("%H:%M, %d.%m.%Y")
    notes = f"Basierend auf NASA-Daten vom {day_formatted}, zuletzt aktualisiert um {now_berlin}."
    dw.update_metadata(CHART_ID, {"annotate": {"notes": notes}})
    print("Updated notes.")

    # Publish the chart
    dw.publish_chart(CHART_ID, display=False)
    print("Published chart.")
