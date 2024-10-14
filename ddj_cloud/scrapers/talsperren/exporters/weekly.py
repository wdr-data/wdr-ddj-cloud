import pandas as pd
from dateutil.relativedelta import relativedelta

from ddj_cloud.scrapers.talsperren.common import (
    FEDERATION_ORDER_SIZE,
    FEDERATION_RENAMES_BREAKS,
    GELSENWASSER_DETAILED,
    Exporter,
)
from ddj_cloud.utils.date_and_time import local_today_midnight


class WeeklyExporter(Exporter):
    filename = "weekly"

    def run(self, df_base: pd.DataFrame) -> pd.DataFrame:
        df_base.insert(0, "id", df_base["federation_name"] + "_" + df_base["name"])

        # Only use "Haltern und Hullern Gesamt" for now, it should be more reliable and it has history
        df_base = df_base[~df_base["name"].isin(GELSENWASSER_DETAILED)]

        # Drop all data before one year ago,
        # plus some extra so we don't underfill any medians/means
        df_base = df_base.loc[
            df_base["ts_measured"] > local_today_midnight() - relativedelta(years=1, weeks=3)
        ]

        # First, set the 'ts_measured' column as the DataFrame index
        df_base.set_index("ts_measured", inplace=True)
        df_base.index = df_base.index.tz_convert("Europe/Berlin")  # type: ignore

        # Group by 'federation_name' and 'name', then resample to daily frequency using median
        df_weekly: pd.DataFrame = (
            df_base.groupby(
                ["id"],
            )
            .resample("W")
            .aggregate(
                {
                    "content_mio_m3": "median",
                    "capacity_mio_m3": "median",
                }
            )
        )

        # Create a new MultiIndex with all permutations of 'id' and 'ts_measured'
        idx = df_weekly.index
        multi_idx = pd.MultiIndex.from_product(
            [idx.get_level_values(level=0).unique(), idx.get_level_values(level=1).unique()],
            names=["id", "ts_measured"],
        )

        # Reindex DataFrame and forward fill missing values from those of the same station
        df_weekly = df_weekly.reindex(multi_idx)
        df_weekly = df_weekly.groupby(level=0).ffill()

        # Reconstruct the 'federation_name' (and 'name') columns
        # df_weekly["federation_name"], df_weekly["name"] = (
        #     df_weekly.index.get_level_values(level=0).str.split("_", 1).str
        # )
        df_weekly["federation_name"] = (
            df_weekly.index.get_level_values(level=0).str.split("_", n=1).str[0]
        )

        # Drop the 'id' column from the index
        df_weekly.reset_index(level=0, drop=True, inplace=True)
        df_weekly.reset_index(inplace=True)
        df_weekly.set_index("ts_measured", inplace=True)

        # Create a new dataframe with columns for each "federation_name"
        # with the mean fill_percent for each week across all reservoirs
        # of that federation
        df_weekly_fed = df_weekly.groupby(["ts_measured", "federation_name"]).aggregate(
            {
                "content_mio_m3": sum_nan,
                "capacity_mio_m3": sum_nan,
            }
        )

        # Add "fill_percent" columns
        df_weekly_fed["fill_percent"] = (
            df_weekly_fed["content_mio_m3"] / df_weekly_fed["capacity_mio_m3"] * 100
        )

        # Drop "content_mio_m3" and "capacity_mio_m3" columns
        df_weekly_fed.drop(columns=["content_mio_m3", "capacity_mio_m3"], inplace=True)

        df_weekly_fed = df_weekly_fed.unstack()
        df_weekly_fed.columns = df_weekly_fed.columns.droplevel(0)

        # Add "Gesamt" column
        df_weekly_gesamt = df_weekly.groupby(["ts_measured"]).aggregate(
            {
                "content_mio_m3": sum_nan,
                "capacity_mio_m3": sum_nan,
            }
        )
        df_weekly_gesamt["fill_percent"] = (
            df_weekly_gesamt["content_mio_m3"] / df_weekly_gesamt["capacity_mio_m3"] * 100
        )
        df_weekly_gesamt.drop(columns=["content_mio_m3", "capacity_mio_m3"], inplace=True)
        df_weekly_gesamt.columns = ["Alle Talsperren"]
        df_weekly_fed = df_weekly_fed.join(df_weekly_gesamt, how="outer")

        df_weekly_fed.reset_index(inplace=True)

        # TODO: Remove later
        # Drop everything from before we have data for all federations
        df_weekly_fed = df_weekly_fed.loc[df_weekly_fed["ts_measured"] > "2023-11-03"]

        # Drop all data before one year ago cleanly
        df_weekly_fed = df_weekly_fed.loc[
            df_weekly_fed["ts_measured"] > local_today_midnight() - relativedelta(years=1)
        ]

        df_weekly_fed.rename(columns={"ts_measured": "date"}, inplace=True)

        # Use start of week as date instead of end of week
        # df_weekly_fed["date"] = df_weekly_fed["date"] - to_offset("6D")

        # Convert date to ISO Week format
        df_weekly_fed["date"] = df_weekly_fed["date"].dt.strftime("%G-W%V")

        # Reorder columns by federation size (only federations that have data, else we get an error)
        filtered_federation_order_size = [
            federation_name
            for federation_name in FEDERATION_ORDER_SIZE
            if federation_name in df_weekly_fed.columns
        ]
        df_weekly_fed = df_weekly_fed[["date", "Alle Talsperren", *filtered_federation_order_size]]

        # Rename federations
        df_weekly_fed.rename(columns=FEDERATION_RENAMES_BREAKS, inplace=True)

        return df_weekly_fed


def sum_nan(x):
    return x.sum(min_count=1)
