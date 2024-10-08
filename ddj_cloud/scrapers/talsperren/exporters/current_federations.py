import pandas as pd

from ddj_cloud.scrapers.talsperren.common import (
    FEDERATION_RENAMES_BREAKS,
    GELSENWASSER_DETAILED,
    Exporter,
)


class CurrentFederationsExporter(Exporter):
    """Current fill level grouped by federation"""

    filename = "current_federations"

    def run(self, df_base: pd.DataFrame) -> pd.DataFrame:
        df_base.insert(0, "id", df_base["federation_name"] + "_" + df_base["name"])

        # Only use "Haltern und Hullern Gesamt" for now, it should be more reliable
        df_base = df_base[~df_base["name"].isin(GELSENWASSER_DETAILED)]

        # Generate map with latest data
        df_map = df_base.copy()
        df_map.sort_values(by=["ts_measured"], inplace=True)
        df_map.drop_duplicates(subset="id", keep="last", inplace=True)
        df_map.sort_values(by="id", inplace=True)
        df_map.reset_index(drop=True, inplace=True)

        # Sum by federation
        df_map = (
            df_map.groupby(["federation_name"])
            .aggregate(
                {
                    "capacity_mio_m3": "sum",
                    "content_mio_m3": "sum",
                }
            )
            .reset_index()
        )

        # Add "Alle Talsperren" row
        df_map = pd.concat(
            [
                df_map,
                pd.DataFrame(
                    [
                        {
                            "federation_name": "Alle Talsperren",
                            "capacity_mio_m3": df_map["capacity_mio_m3"].sum(),
                            "content_mio_m3": df_map["content_mio_m3"].sum(),
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

        # Add fill percentage
        df_map["fill_percent"] = df_map["content_mio_m3"] / df_map["capacity_mio_m3"] * 100

        # round all floats to 5 decimals
        df_map = df_map.round(5)

        # Rename federation names
        df_map["federation_name"].replace(
            FEDERATION_RENAMES_BREAKS,
            inplace=True,
        )

        # Sort by capacity
        df_map.sort_values(by="capacity_mio_m3", ascending=False, inplace=True)

        return df_map
