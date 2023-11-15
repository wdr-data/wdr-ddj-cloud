import pandas as pd
from dateutil.relativedelta import relativedelta

from ddj_cloud.scrapers.talsperren.common import Exporter
from ddj_cloud.utils.date_and_time import local_today_midnight


class DailyExporter(Exporter):
    filename = "daily"

    def run(self, df_base: pd.DataFrame) -> pd.DataFrame:
        df_base.insert(0, "id", df_base["federation_name"] + "_" + df_base["name"])

        # Drop all data before one month ago (plus some extra so we don't underfill any medians/means)
        df_base = df_base.loc[
            df_base["ts_measured"] > local_today_midnight() - relativedelta(months=3)
        ]

        # First, set the 'ts_measured' column as the DataFrame index
        df_base.set_index("ts_measured", inplace=True)
        df_base.index = df_base.index.tz_convert("Europe/Berlin")  # type: ignore

        # Group by 'federation_name' and 'name', then resample to daily frequency using median
        df_daily: pd.DataFrame = (
            df_base.groupby(
                ["id"],
            )
            .resample("D")
            .aggregate(  # type: ignore
                {
                    "content_mio_m3": "median",
                    "capacity_mio_m3": "median",
                }
            )
        )

        # Create a new MultiIndex with all permutations of 'id' and 'ts_measured'
        idx = df_daily.index
        multi_idx = pd.MultiIndex.from_product(
            [idx.get_level_values(level=0).unique(), idx.get_level_values(level=1).unique()],
            names=["id", "ts_measured"],
        )

        # Reindex DataFrame and forward fill missing values from those of the same station
        df_daily = df_daily.reindex(multi_idx)
        df_daily = df_daily.groupby(level=0).ffill()

        # Reconstruct the 'federation_name' (and 'name') columns
        # df_weekly["federation_name"], df_weekly["name"] = (
        #     df_weekly.index.get_level_values(level=0).str.split("_", 1).str
        # )
        df_daily["federation_name"] = (
            df_daily.index.get_level_values(level=0).str.split("_", n=1).str[0]
        )

        # Drop the 'id' column from the index
        df_daily.reset_index(level=0, drop=True, inplace=True)
        df_daily.reset_index(inplace=True)
        df_daily.set_index("ts_measured", inplace=True)

        # Create a new dataframe with columns for each "federation_name"
        # with the mean fill_percent for each week across all reservoirs
        # of that federation
        df_daily_fed = df_daily.groupby(["ts_measured", "federation_name"]).aggregate(
            {
                "content_mio_m3": sum_nan,
                "capacity_mio_m3": sum_nan,
            }
        )

        # Add "fill_percent" columns
        df_daily_fed["fill_percent"] = (
            df_daily_fed["content_mio_m3"] / df_daily_fed["capacity_mio_m3"] * 100
        )

        # Drop "content_mio_m3" and "capacity_mio_m3" columns
        df_daily_fed.drop(columns=["content_mio_m3", "capacity_mio_m3"], inplace=True)

        df_daily_fed = df_daily_fed.unstack()
        df_daily_fed.columns = df_daily_fed.columns.droplevel(0)

        # Add "Gesamt" column
        df_weekly_gesamt = df_daily.groupby(["ts_measured"]).aggregate(
            {
                "content_mio_m3": sum_nan,
                "capacity_mio_m3": sum_nan,
            }
        )
        df_weekly_gesamt["fill_percent"] = (
            df_weekly_gesamt["content_mio_m3"] / df_weekly_gesamt["capacity_mio_m3"] * 100
        )
        df_weekly_gesamt.drop(columns=["content_mio_m3", "capacity_mio_m3"], inplace=True)
        df_weekly_gesamt.columns = ["Gesamt"]
        df_daily_fed = df_daily_fed.join(df_weekly_gesamt, how="outer")

        df_daily_fed.reset_index(inplace=True)

        # Drop all data before one month ago cleanly
        df_daily_fed = df_daily_fed.loc[
            df_daily_fed["ts_measured"] > local_today_midnight() - relativedelta(months=1)
        ]

        df_daily_fed.rename(columns={"ts_measured": "date"}, inplace=True)

        # Convert datetime to iso date string
        df_daily_fed["date"] = df_daily_fed["date"].dt.strftime("%Y-%m-%d")

        return df_daily_fed


def sum_nan(x):
    return x.sum(min_count=1)
