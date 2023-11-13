import datetime as dt

import pandas as pd
from dateutil.relativedelta import relativedelta

from ddj_cloud.scrapers.talsperren.common import Exporter
from ddj_cloud.utils.date_and_time import local_today_midnight


class MapExporter(Exporter):
    filename = "map"

    def _add_daily_fill_percent_to_map(
        self, df_base: pd.DataFrame, df_map: pd.DataFrame
    ) -> pd.DataFrame:
        # Resample df to daily
        df_res = df_base.copy()
        # First, set the 'ts_measured' column as the DataFrame index
        df_res.set_index("ts_measured", inplace=True)
        df_res.index = df_res.index.tz_convert("Europe/Berlin")  # type: ignore

        # Group by 'federation_name' and 'name', then resample to daily frequency using median
        df_daily = (
            df_res.groupby(
                ["id"],
            )
            .resample("D")
            .aggregate(  # type: ignore
                {
                    "fill_percent": "median",
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

        # Add a new column to `df_map` for each of the last 7 days
        today_midnight = local_today_midnight()
        for days_offset in range(0, 8):
            # Use Python to calculate the timestamp for correct timezone support, then convert to pandas
            ts = today_midnight - dt.timedelta(days=days_offset)
            ts = pd.Timestamp(ts)
            df_day = df_daily.loc[(slice(None), ts), :].reset_index(level=1, drop=True)
            df_day.rename(columns={"fill_percent": f"fill_percent_day_{days_offset}"}, inplace=True)
            df_map = df_map.merge(df_day, how="left", on="id")

        return df_map

    def _add_monthly_fill_percent_to_map(
        self, df_base: pd.DataFrame, df_map: pd.DataFrame
    ) -> pd.DataFrame:
        # Resample df to monthly
        df_res = df_base.copy()

        # First, set the 'ts_measured' column as the DataFrame index
        df_res.set_index("ts_measured", inplace=True)
        df_res.index = df_res.index.tz_convert("Europe/Berlin")  # type: ignore

        # Group by 'id', then resample to monthly frequency using median
        df_monthly: pd.DataFrame = (
            df_res.groupby(
                ["id"],
            )
            .resample("M", closed="right", label="left")
            .aggregate(  # type: ignore
                {
                    "fill_percent": "median",
                }
            )
        )

        # Create a new MultiIndex with all permutations of 'id' and 'ts_measured'
        idx = df_monthly.index
        multi_idx = pd.MultiIndex.from_product(
            [idx.get_level_values(level=0).unique(), idx.get_level_values(level=1).unique()],
            names=["id", "ts_measured"],
        )

        # Reindex DataFrame and forward fill missing values from those of the same station
        df_monthly = df_monthly.reindex(multi_idx)
        df_monthly = df_monthly.groupby(level=0).ffill()

        # Add a new column to `df_map` for each of the last 6 months
        today_midnight = local_today_midnight()
        current_month = today_midnight.replace(day=1)
        for months_offset in range(0, 13):
            ts = current_month - relativedelta(months=months_offset, days=1)
            ts = pd.Timestamp(ts)
            df_month = df_monthly.loc[(slice(None), ts), :].reset_index(level=1, drop=True)
            df_month.rename(
                columns={"fill_percent": f"fill_percent_month_{months_offset}"}, inplace=True
            )
            df_map = df_map.merge(df_month, how="left", on="id")

        return df_map

    def run(self, df_base: pd.DataFrame) -> pd.DataFrame:
        df_base.insert(0, "id", df_base["federation_name"] + "_" + df_base["name"])

        # Gernerate map with latest data
        df_map = df_base.copy()
        df_map.sort_values(by=["ts_measured"], inplace=True)
        df_map.drop_duplicates(subset="id", keep="last", inplace=True)
        df_map.sort_values(by="id", inplace=True)
        df_map.reset_index(drop=True, inplace=True)

        # Add daily fill ratio
        df_map = self._add_daily_fill_percent_to_map(df_base, df_map)

        # Add monthly fill ratio
        df_map = self._add_monthly_fill_percent_to_map(df_base, df_map)

        # round all floats to 5 decimals
        df_map = df_map.round(5)

        return df_map
