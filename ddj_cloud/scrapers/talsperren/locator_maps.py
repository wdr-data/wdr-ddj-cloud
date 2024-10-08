import json
import os
import re
from typing import Literal

import pandas as pd
import sentry_sdk
from datawrapper import Datawrapper

from ddj_cloud.scrapers.talsperren.common import FEDERATION_RENAMES, RESERVOIR_RENAMES
from ddj_cloud.utils.formatting import format_datetime, format_number

REVERSE_RESERVOIR_RENAMES = {v: k for k, v in RESERVOIR_RENAMES.items()}

DATAWRAPPER_TOKEN = os.environ.get("TALSPERREN_DATAWRAPPER_TOKEN")


RENAMES = {
    "Stauanlage Ahausen": "Stausee Ahausen",
    "Stauanlage Beyenburg": "Stausee Beyenburg",
    "Obersee (Vorsperre der Rurtalsperre)": "Rurtalsperre Obersee",
}


color_map_fill = {
    0: "rgb(158, 103, 0)",
    25: "rgb(252, 199, 87)",
    50: "rgb(119, 219, 249)",
    75: "rgb(82, 157, 220)",
    90: "rgb(0, 72, 162)",
    100: "rgb(227, 6, 20)",
}

color_map_text = {
    0: "white",
    25: "black",
    50: "black",
    75: "black",
    90: "white",
    100: "white",
}


def _get_color(fill_percent: float, color_map: dict) -> str:
    for threshold, color in reversed(list(color_map.items())):
        if fill_percent >= threshold:
            return color

    return color_map[0]


def _make_tooltip(current: dict, variant: Literal["desktop", "mobile"]) -> str:
    bar_text = format_number(current["fill_percent"], places=1) + "     %"
    bar_color = _get_color(current["fill_percent"], color_map_fill)
    bar_text_color = _get_color(current["fill_percent"], color_map_text)
    bar_text_margin = "28px" if current["fill_percent"] < 25 else "3px"  # noqa: PLR2004

    name = RESERVOIR_RENAMES.get(current["name"], current["name"])
    fill_percent = max(min(current["fill_percent"], 100), 0)
    content_mio_m3 = format_number(current["content_mio_m3"], places=2)
    capacity_mio_m3 = format_number(current["capacity_mio_m3"], places=2)
    federation_name = FEDERATION_RENAMES.get(current["federation_name"], current["federation_name"])
    ts_measured = format_datetime(current["ts_measured"])

    width = "156px"  # if variant == "desktop" else "100px"  # Doesn't seem to shrink anyways
    font_size_header = "13px" if variant == "desktop" else "10px"
    font_size_bar = "12px" if variant == "desktop" else "9px"
    font_size_text = "11px" if variant == "desktop" else "8px"
    height_bar = "22px" if variant == "desktop" else "16px"
    margin_wide = "10px" if variant == "desktop" else "5px"
    margin_narrow = "5px" if variant == "desktop" else "2px"
    margin_outer = "0px" if variant == "desktop" else "-6px"

    tooltip_html = f"""
<u style="display: block; text-decoration: none; min-width: {width}; max-width: {width}; margin: { margin_outer }; overflow:hidden; font-size: { font_size_text }; line-height: 1.25;">
    <b style="display: block; font-size: { font_size_header }; margin-bottom: { margin_wide };">{ name }</b>
    <u style="display: flex; text-decoration: none; align-items: center; background: #f2f2f2; width: 100%; height: { height_bar }; ">
        <u style="display: flex; text-decoration: none; background: { bar_color }; align-items: center; height: 100%; width: { fill_percent }%">
            <b style="color: { bar_text_color }; font-size: { font_size_bar }; margin: 0px { margin_narrow }; margin-left: { bar_text_margin };"> { bar_text }</b>
        </u>
    </u>
    <u style="display: block; text-decoration: none; margin-top: 2px; margin-bottom: { margin_wide };">{ content_mio_m3 } von { capacity_mio_m3 } Mio. m³</u>
    <u style="display: grid; text-decoration: none; gap: { margin_narrow };">
        <u style="display: block; text-decoration: none;">
            <b>Verband:</b><br/>
            <span style="overflow-wrap: anywhere;">{ federation_name }</span>
        </u>
        <u style="display: block; text-decoration: none;">
            <b>Messzeit:</b><br/>
            <td>{ ts_measured }</td>
        </u>
    </u>
</u>
""".replace("    ", "").replace("\n", "")

    return tooltip_html


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

    charts = [
        ("aqXpg", "ZXiUo"),
        ("cZfsi", "BL07F"),
        ("WSgd6", "aHate"),
        ("QZfQN", "kcuUG"),
    ]
    for chart_id_base, chart_id_live in charts:
        print()
        print(f"Processing chart {chart_id_base} -> {chart_id_live}")

        try:
            _process_chart(current, dw, chart_id_base, chart_id_live)
        except Exception as e:
            print("Skipping chart due to error:")
            print(e)
            sentry_sdk.capture_exception(e)


def _process_chart(current: dict, dw: Datawrapper, chart_id_base: str, chart_id_live: str) -> None:  # noqa: PLR0915
    chart_base = dw.get_chart(chart_id_base)
    chart_live = dw.get_chart(chart_id_live)

    assert chart_base is not None, "Base chart not found."
    assert chart_base["type"] == "locator-map", "Base chart is not a locator map."

    assert chart_live is not None, "Live chart not found."
    assert chart_live["type"] == "locator-map", "Live chart is not a locator map."

    chart_data_base = dw.get_data(chart_id_base)
    chart_data_live = dw.get_data(chart_id_live)

    markers = chart_data_base["markers"]

    print("Updating fill colors")
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
        fill_percent = current_for_name["fill_percent"]
        fill_color = _get_color(fill_percent, color_map_fill)

        marker["properties"]["fill"] = fill_color
        # marker["properties"]["stroke"] = fill_color

    print("Updating tooltip markers")
    for marker in chart_data_base["markers"]:
        if marker["type"] != "point":
            continue

        match = re.match(r'Tooltipmarker: "(.*?)".*', marker["title"])

        if match is None:
            continue

        name = match.group(1)
        name = REVERSE_RESERVOIR_RENAMES.get(name, name)
        print(f"NAME_GEWFL: {name}")

        if name not in current:
            print(f'Skipping "{name}" because it is not in the current data')
            continue

        variant = (
            "desktop"
            if marker["visibility"]["desktop"]
            else "mobile"
            if marker["visibility"]["mobile"]
            else None
        )

        if variant is None:
            print(f"Skipping {name} because it is visible on neither desktop nor mobile")
            continue

        marker["title"] = ""
        marker["visible"] = True
        marker["markerColor"] = "#ffffff00"
        marker["tooltip"]["text"] = _make_tooltip(current[name], variant)

    # Copy the markers from the base chart
    new_chart_data_live = {
        **chart_data_live,
        "markers": chart_data_base["markers"],
    }

    print("Size:", len(json.dumps(new_chart_data_live).encode("utf-8")), "/ 2097152")

    dw.add_json(
        chart_id_live,
        new_chart_data_live,
    )

    # Publish the chart
    dw.publish_chart(chart_id_live, display=False)
    print("Published chart.")
