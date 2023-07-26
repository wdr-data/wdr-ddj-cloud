import os
from pathlib import Path
import datetime as dt
import json

import pandas as pd

from ddj_cloud.utils.storage import upload_dataframe
from ddj_cloud.utils.datawrapper_patched import Datawrapper

MAP_KEY = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_MAP_KEY")
DATAWRAPPER_TOKEN = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_DATAWRAPPER_TOKEN")
CHART_ID = os.environ.get("NASA_WALDBRANDDATEN_RHODOS_CHART_ID")

CURRENT_DIR = Path(__file__).parent


def make_nasa_url(date: dt.date):
    location_str = "27.648249,35.843826,28.337617,36.494931"
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_SNPP_NRT/{location_str}/1/{date.isoformat()}"
    return url


def run():
    # Get today's date
    day = dt.date.today()
    df = None

    for _ in range(2):
        print(f"Trying to get data for {day.isoformat()}...")

        # Get the url for the current date
        url = make_nasa_url(day)
        df = pd.read_csv(url, sep=",", decimal=".", low_memory=False)

        if len(df) == 0:
            day = day - dt.timedelta(days=1)
            continue

        print(f"Got data for {day.isoformat()}!")
        break

    if df is None:
        raise Exception("Borked.")

    # Filter by confidence (nominal and high)
    df = df.loc[df["confidence"].isin(["n", "h"])]

    # Upload the data
    upload_dataframe(df, "nasa_waldbranddaten_rhodos/nasa_data.csv")

    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    print(dw.account_info())

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
        }
        markers.append(marker)

    with open(CURRENT_DIR / "static_markers.json") as fp:
        static_markers = json.load(fp)

    resp = dw.add_data_json(CHART_ID, {"markers": [*static_markers, *markers]})

    print(resp.status_code)
    print(resp.text)
