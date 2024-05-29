import os

from datawrapper import Datawrapper
import pandas as pd
import sentry_sdk


DATAWRAPPER_TOKEN = os.environ.get("TALSPERREN_DATAWRAPPER_TOKEN")


RENAMES = {
    "Stauanlage Ahausen": "Stausee Ahausen",
    "Stauanlage Beyenburg": "Stausee Beyenburg",
    "Obersee (Vorsperre der Rurtalsperre)": "Rurtalsperre Obersee",
}


color_map = {
    0: "rgb(158, 103, 0)",
    25: "rgb(252, 199, 87)",
    50: "rgb(119, 219, 249)",
    75: "rgb(82, 157, 220)",
    90: "rgb(0, 72, 162)",
    100: "rgb(227, 6, 20)",
}


def _get_color(fill_percent: float) -> str:
    for threshold, color in reversed(list(color_map.items())):
        if fill_percent >= threshold:
            return color

    return color_map[0]


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
        ("jG5QE", "ZXiUo"),
        ("cZfsi", "BL07F"),
        ("WSgd6", "IsUxG"),
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


def _process_chart(current: dict, dw: Datawrapper, chart_id_base: str, chart_id_live: str) -> None:
    chart_base = dw.get_chart(chart_id_base)
    chart_live = dw.get_chart(chart_id_live)

    assert chart_base is not None, "Base chart not found."
    assert chart_base["type"] == "locator-map", "Base chart is not a locator map."

    assert chart_live is not None, "Live chart not found."
    assert chart_live["type"] == "locator-map", "Live chart is not a locator map."

    chart_data_base = dw.get_data(chart_id_base)
    chart_data_live = dw.get_data(chart_id_live)

    markers = chart_data_base["markers"]

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
        fill_color = _get_color(fill_percent)

        marker["properties"]["fill"] = fill_color
        # marker["properties"]["stroke"] = fill_color

    # Copy the markers from the base chart
    dw.add_json(
        chart_id_live,
        {
            **chart_data_live,
            "markers": chart_data_base["markers"],
        },
    )

    # Publish the chart
    dw.publish_chart(chart_id_live, display=False)
    print("Published chart.")
