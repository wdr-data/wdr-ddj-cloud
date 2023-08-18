import os
from pathlib import Path
import datetime as dt
import json
from typing import Union
from uuid import uuid4
from zoneinfo import ZoneInfo
import dateparser

import pandas as pd

from ddj_cloud.utils.storage import upload_dataframe
from ddj_cloud.utils.datawrapper_patched import Datawrapper

MAP_KEY = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_MAP_KEY")
DATAWRAPPER_TOKEN = os.environ.get("NASA_WALDBRANDDATEN_TENERIFFA_DATAWRAPPER_TOKEN")
CHART_ID = os.environ.get("NASA_WALDBRANDDATEN_TENERIFFA_CHART_ID")

CURRENT_DIR = Path(__file__).parent
NASA_API_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api"
INSTRUMENT = "VIIRS_SNPP_NRT"

MAP_EXTENT_NW = [29.013010, -18.319668]
MAP_EXTENT_SE = [27.427655, -14.937175]

MAP_EXTENT = [
    MAP_EXTENT_NW[1],
    MAP_EXTENT_SE[0],
    MAP_EXTENT_SE[1],
    MAP_EXTENT_NW[0],
]  # West, South, East, North

TZ_BERLIN = ZoneInfo("Europe/Berlin")
TZ_UTC = ZoneInfo("UTC")


def make_nasa_data_url():
    map_extent = ",".join(map(str, MAP_EXTENT))

    url = f"{NASA_API_BASE_URL}/area/csv/{MAP_KEY}/{INSTRUMENT}/{map_extent}/5"
    return url


def run():
    # Get the data for the latest day
    url = make_nasa_data_url()
    df = pd.read_csv(
        url,
        sep=",",
        decimal=".",
        low_memory=False,
        dtype={"acq_time": str, "acq_date": str},
    )

    print(f"Got data!")
    print(df.head())
    print(df.info())

    if df is None:
        raise Exception("Borked.")

    # Filter by confidence (nominal and high)
    df = df.loc[df["confidence"].isin(["n", "h"])]

    # Convert datetime from columns "acq_date" and "acq_time" to a single datetime column
    df["acq_datetime"] = pd.to_datetime(
        df["acq_date"] + " " + df["acq_time"],
        format="%Y-%m-%d %H%M",
        utc=True,
    )

    # Upload the data
    upload_dataframe(df, "nasa_waldbranddaten_teneriffa/nasa_data.csv")

    # Setup Datawrapper
    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    print("Datawrapper account info:", dw.account_info())

    props = dw.chart_properties(CHART_ID)
    # print(json.dumps(props))
    assert props is not None, "Chart not found."
    assert isinstance(props, dict), "Chart properties are not a dict."
    assert props["type"] == "locator-map", "Chart is not a locator map."

    markers = []

    for _, row in df.iterrows():
        marker_dt = row["acq_datetime"].astimezone(TZ_BERLIN)

        marker = {
            "id": str(uuid4()),
            "type": "point",
            "title": "",
            "coordinates": [row["longitude"], row["latitude"]],
            "markerColor": "#be0000",
            "scale": 0.25,
            "icon": {"path": "M0 1000h1000v-1000h-1000z", "height": 1000, "width": 1000},
            "anchor": "bottom-right",
            "offsetY": 0,
            "offsetX": 0,
            "visible": True,
            "visibility": {"mobile": True, "desktop": True},
            "tooltip": {
                "enabled": False,
                "text": f"Dieses Feuer wurde von der NASA am {marker_dt.strftime('%d.%m.%Y um %H:%M Uhr')} registriert.",
            },
            "text": {},
        }
        markers.append(marker)

    with open(CURRENT_DIR / "static_markers.json", encoding="utf-8") as fp:
        static_markers = json.load(fp)

    resp = dw.add_data_json(CHART_ID, {"markers": [*static_markers, *markers]})
    resp.raise_for_status()

    print("Updated markers.")

    # Update notes
    dt_update_str = dt.datetime.now(tz=TZ_BERLIN).strftime("%d.%m.%Y um %H:%M")

    dt_earliest = df["acq_datetime"].min()
    dt_latest = df["acq_datetime"].max()

    if dt_latest.date() != dt_earliest.date():
        dt_earliest_str = dt_earliest.astimezone(TZ_BERLIN).strftime("%d.%m.%Y um %H:%M")
        dt_latest_str = dt_latest.astimezone(TZ_BERLIN).strftime("%d.%m.%Y um %H:%M")
        dt_range_str = f"vom {dt_earliest_str} bis zum {dt_latest_str}"
    elif dt_latest == dt_earliest:
        dt_range_str = f"am {dt_earliest.astimezone(TZ_BERLIN).strftime('%d.%m.%Y um %H:%M')}"
    else:
        dt_earliest_str = dt_earliest.astimezone(TZ_BERLIN).strftime("%d.%m.%Y zwischen %H:%M")
        dt_latest_str = dt_latest.astimezone(TZ_BERLIN).strftime("%H:%M")
        dt_range_str = f"am {dt_earliest_str} und {dt_latest_str}"

    notes = f"Basierend auf NASA-Satellitendaten aufgenommen {dt_range_str}, zuletzt aktualisiert am {dt_update_str}."
    dw.update_metadata(CHART_ID, {"annotate": {"notes": notes}})
    print("Updated notes.")

    # Publish the chart
    dw.publish_chart(CHART_ID, display=False)
    print("Published chart.")
