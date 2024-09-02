import os
from uuid import uuid4 as uuid

from datawrapper import Datawrapper
import pandas as pd
import sentry_sdk

from .locator_maps import RENAMES

DATAWRAPPER_TOKEN = os.environ.get("TALSPERREN_DATAWRAPPER_TOKEN")


def _make_marker(current: dict) -> dict:

    return {
        "title": f'Tooltipmarker: "{current["name"]}"',
        "id": str(uuid()),
        "type": "point",
        "icon": {
            "id": "circle",
            "path": "M1000 350a500 500 0 0 0-500-500 500 500 0 0 0-500 500 500 500 0 0 0 500 500 500 500 0 0 0 500-500z",
            "horiz-adv-x": 1000,
            "scale": 1,
            "height": 700,
            "width": 1000,
        },
        "scale": 3,
        "markerColor": "#ffffffaa",
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
            "text": f"Tooltip für {current['name']}",
        },
        "connectorLine": {
            "enabled": False,
            "arrowHead": "lines",
            "type": "curveRight",
            "targetPadding": 3,
            "stroke": 1,
            "lineLength": 0,
        },
        # "coordinates": [7.684433678231585, 50.9428533167156],
        "coordinates": [
            current["lon"],
            current["lat"],
        ],
    }


def run(df_base: pd.DataFrame) -> None:
    assert DATAWRAPPER_TOKEN is not None

    # Drop everything but the latest data
    df_base.insert(0, "id", df_base["federation_name"] + "_" + df_base["name"])
    df_base.sort_values(by=["ts_measured"], inplace=True)
    df_base.drop_duplicates(subset="id", keep="last", inplace=True)

    # Build dict from reservoir names to their data
    current = {}
    for _, row in df_base.iterrows():
        current[row["name"]] = row.to_dict()

    # Setup Datawrapper
    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    # print("Datawrapper account info:", dw.account_info())

    charts = []

    for chart_id_base in charts:
        print()
        print(f"Processing chart {chart_id_base}")

        try:
            _process_chart(current, dw, chart_id_base)
        except Exception as e:
            print("Skipping chart due to error:")
            print(e)
            sentry_sdk.capture_exception(e)


def _process_chart(current: dict, dw: Datawrapper, chart_id_base: str) -> None:
    chart_base = dw.get_chart(chart_id_base)

    assert chart_base is not None, "Base chart not found."
    assert chart_base["type"] == "locator-map", "Base chart is not a locator map."

    chart_data_base = dw.get_data(chart_id_base)

    markers = chart_data_base["markers"]
    new_markers = []

    for marker in markers:
        if marker["type"] != "area":
            # print(f"Skipping {marker['title']} because it is not an area")
            continue

        if "Vignette" in marker["title"]:
            # print(f"Skipping {marker['title']} because it is a vignette")
            continue

        if "NAME_GEWFL" not in marker.get("data", {}):
            print(f"Skipping {marker['title']} because it has no NAME_GEWFL")
            continue

        name = marker["data"]["NAME_GEWFL"]
        name = RENAMES.get(name, name)

        if name not in current:
            print(f'Skipping "{name}" because it is not in the current data')
            continue

        print(f"NAME_GEWFL: {name}")

        current_for_name = current[name]

        new_markers.append(_make_marker(current_for_name))

    # Copy the markers from the base chart
    dw.add_json(
        chart_id_base,
        {
            **chart_data_base,
            "markers": new_markers + chart_data_base["markers"],
        },
    )

    # Publish the chart
    dw.publish_chart(chart_id_base, display=False)
    print("Published chart.")
