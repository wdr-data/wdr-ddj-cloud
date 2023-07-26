import os
from pathlib import Path
import datetime as dt
import json
from zoneinfo import ZoneInfo
from io import StringIO
from uuid import uuid4

import dateparser
import pandas as pd
import requests as r

from ddj_cloud.utils.storage import upload_dataframe
from ddj_cloud.utils.datawrapper_patched import Datawrapper

DATAWRAPPER_TOKEN = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_DATAWRAPPER_TOKEN")
CHART_ID = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_CHART_ID")

CURRENT_DIR = Path(__file__).parent
URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_24h.csv"

MAP_EXTENT_NW = [36.488223, 27.486474]
MAP_EXTENT_SE = [35.851543, 28.307467]

TZ_BERLIN = ZoneInfo("Europe/Berlin")
TZ_UTC = ZoneInfo("UTC")


def run():
    # Get the data for the last 24 hours
    resp_nasa_csv = r.get(URL)
    resp_nasa_csv.raise_for_status()

    df = pd.read_csv(
        StringIO(resp_nasa_csv.text),
        sep=",",
        decimal=".",
        low_memory=False,
        dtype={"acq_time": str, "acq_date": str},
    )

    print(f"Got data!")

    # Filter by confidence (nominal and high)
    df = df.loc[df["confidence"].isin(["nominal", "high"])]

    # Filter by location
    df = df.loc[
        (df["latitude"] >= MAP_EXTENT_SE[0])
        & (df["latitude"] <= MAP_EXTENT_NW[0])
        & (df["longitude"] >= MAP_EXTENT_NW[1])
        & (df["longitude"] <= MAP_EXTENT_SE[1])
    ]

    # Convert datetime from columns "acq_date" and "acq_time" to a single datetime column
    df["acq_datetime"] = pd.to_datetime(
        df["acq_date"] + " " + df["acq_time"],
        format="%Y-%m-%d %H%M",
        utc=True,
    )

    # Upload the data
    upload_dataframe(df, "nasa_waldbranddaten_rhodos/nasa_data.csv")

    # Setup Datawrapper
    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    print("Datawrapper account info:", dw.account_info())

    props = dw.chart_properties(CHART_ID)
    # print(json.dumps(props))
    assert props is not None, "Chart not found."
    assert isinstance(props, dict), "Chart properties are not a dict."
    assert props["type"] == "locator-map", "Chart is not a locator map."

    # Update markers
    markers = []

    dt_earliest = df["acq_datetime"].min()
    dt_latest = df["acq_datetime"].max()

    for _, row in df.iterrows():
        # marker_age = (row["acq_datetime"] - dt_earliest) / (dt_latest - dt_earliest)
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
                "enabled": True,
                "text": f"Dieses Feuer wurde von der NASA am {marker_dt.strftime('%d.%m.%Y um %H:%M Uhr')} registriert.",
            },
            "text": {},
        }
        markers.append(marker)

    with open(CURRENT_DIR / "static_markers.json") as fp:
        static_markers = json.load(fp)

    resp_dw = dw.add_data_json(CHART_ID, {"markers": [*static_markers, *markers]})
    resp_dw.raise_for_status()

    print("Updated markers.")

    # Update notes
    dt_update_str = dateparser.parse(
        resp_nasa_csv.headers["Last-Modified"],
        languages=["en"],
    ).strftime("%d.%m.%Y um %H:%M")

    if dt_latest.date() != dt_earliest.date():
        dt_earliest_str = dt_earliest.astimezone(TZ_BERLIN).strftime("%d.%m.%Y um %H:%M")
        dt_latest_str = dt_latest.astimezone(TZ_BERLIN).strftime("%d.%m.%Y um %H:%M")
        dt_range_str = f"vom {dt_earliest_str} bis zum {dt_latest_str}"
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
