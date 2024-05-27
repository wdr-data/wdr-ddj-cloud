import os

from datawrapper import Datawrapper
import pandas as pd


DATAWRAPPER_TOKEN = os.environ.get("TALSPERREN_DATAWRAPPER_TOKEN")
CHART_ID_BASE = os.environ.get("TALSPERREN_DATAWRAPPER_CHART_ID_BASE_AGGER")
CHART_ID_LIVE = os.environ.get("TALSPERREN_DATAWRAPPER_CHART_ID_LIVE_AGGER")


def foo(base_df: pd.DataFrame) -> None:
    assert DATAWRAPPER_TOKEN is not None
    assert CHART_ID_BASE is not None
    assert CHART_ID_LIVE is not None

    # Setup Datawrapper
    dw = Datawrapper(access_token=DATAWRAPPER_TOKEN)

    print("Datawrapper account info:", dw.account_info())

    chart_base = dw.get_chart(CHART_ID_BASE)
    chart_data = dw.get_data(CHART_ID_BASE)

    # print(json.dumps(props))
    assert chart_base is not None, "Chart not found."
    assert chart_base["type"] == "locator-map", "Chart is not a locator map."

    # print("Chart properties:", chart_data)
    # Copy the markers from the base chart
    # dw.add_json(CHART_ID_LIVE, {"markers": chart_data["markers"]})

    markers = chart_data["markers"]
    for marker in markers:
        if marker["type"] != "area":  # or marker["title"] != "Aggertalsperre / Vignette 1":
            continue

        print(f"title: {marker['title']}")
        print(f"id: {marker['id']}")
        print(f"type: {marker['type']}")
        # print(f"icon: {marker['icon']}")
        # print(f"scale: {marker['scale']}")
        # print(f"textPosition: {marker['textPosition']}")
        # print(f"markerColor: {marker['markerColor']}")
        # print(f"markerSymbol: {marker['markerSymbol']}")
        # print(f"markerTextColor: {marker['markerTextColor']}")
        # print(f"anchor: {marker['anchor']}")
        # print(f"offsetY: {marker['offsetY']}")
        # print(f"offsetX: {marker['offsetX']}")
        # print(f"visible: {marker['visible']}")
        # print(f"visibility: {marker['visibility']}")
        # print(f"tooltip: {marker['tooltip']}")
        # print(f"connectorLine: {marker['connectorLine']}")
        # print(f"coordinates: {marker['coordinates']}")
        # print(f"orgLatLng: {marker['orgLatLng']}")
        print("")
        # print(marker)

    # Publish the chart
    # dw.publish_chart(CHART_ID_LIVE, display=False)
    # print("Published chart.")
