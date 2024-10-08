from collections.abc import Sequence

import pandas as pd
from dateutil.relativedelta import relativedelta
from slugify import slugify

from ddj_cloud.scrapers.talsperren.common import (
    FEDERATION_RENAMES,
    GELSENWASSER_DETAILED,
    GELSENWASSER_GESAMT,
    RESERVOIR_RENAMES,
    RESERVOIR_RENAMES_BREAKS,
    Exporter,
)
from ddj_cloud.scrapers.talsperren.federations.agger import AggerFederation
from ddj_cloud.scrapers.talsperren.federations.eifel_rur import EifelRurFederation
from ddj_cloud.scrapers.talsperren.federations.gelsenwasser import GelsenwasserFederation
from ddj_cloud.scrapers.talsperren.federations.ruhr import RuhrFederation
from ddj_cloud.scrapers.talsperren.federations.wahnbach import WahnbachReservoirFederation
from ddj_cloud.scrapers.talsperren.federations.wupper import WupperFederation
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
        df_daily: pd.DataFrame = (
            df_res.groupby(
                ["id"],
            )
            .resample("D")
            .aggregate(
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
            # Use Python to calculate the timestamp for correct timezone support,
            # then convert to pandas
            ts = today_midnight - relativedelta(days=days_offset)
            ts = pd.Timestamp(ts)
            try:
                df_day = df_daily.loc[(slice(None), ts), :].reset_index(level=1, drop=True)
                df_day.rename(
                    columns={"fill_percent": f"fill_percent_day_{days_offset}"},
                    inplace=True,
                )
                df_map = df_map.merge(df_day, how="left", on="id")
            except KeyError:  # no data for this day
                df_map[f"fill_percent_day_{days_offset}"] = pd.NA

        return df_map

    def _add_weekly_fill_percent_to_map(
        self, df_base: pd.DataFrame, df_map: pd.DataFrame
    ) -> pd.DataFrame:
        # Resample df to weekly
        df_res = df_base.copy()

        # First, set the 'ts_measured' column as the DataFrame index
        df_res.set_index("ts_measured", inplace=True)
        df_res.index = df_res.index.tz_convert("Europe/Berlin")  # type: ignore

        # Group by 'id', then resample to weekly frequency using median
        df_weekly: pd.DataFrame = (
            df_res.groupby(
                ["id"],
            )
            .resample("W", closed="right", label="left")
            .aggregate(
                {
                    "fill_percent": "median",
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

        # Add a new column to `df_map` for each of the last 6 weeks
        today_midnight = local_today_midnight()
        current_week = today_midnight - relativedelta(days=today_midnight.weekday() + 1)

        for weeks_offset in range(0, 13):
            ts = current_week - relativedelta(weeks=weeks_offset)
            ts = pd.Timestamp(ts)
            df_week = df_weekly.loc[(slice(None), ts), :].reset_index(level=1, drop=True)
            df_week.rename(
                columns={"fill_percent": f"fill_percent_week_{weeks_offset}"},
                inplace=True,
            )
            df_map = df_map.merge(df_week, how="left", on="id")

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
            .aggregate(
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
                columns={"fill_percent": f"fill_percent_month_{months_offset}"},
                inplace=True,
            )
            df_map = df_map.merge(df_month, how="left", on="id")

        return df_map

    def _add_marker_size(self, df_map: pd.DataFrame) -> pd.DataFrame:
        source_column = df_map["capacity_mio_m3"]
        df_map.insert(
            1,
            "marker_size",
            (source_column - source_column.min()) / (source_column.max() - source_column.min()) * 95
            + 5,
        )
        return df_map

    def run(
        self,
        df_base: pd.DataFrame,
        do_reservoir_rename: bool = True,
        # Only use "Haltern und Hullern Gesamt" by default for now, it should be more reliable and it has history
        # Overridden for filtered maps
        ignored_reservoirs: list[str] | None = GELSENWASSER_DETAILED,
    ) -> pd.DataFrame:
        df_base.insert(0, "id", df_base["federation_name"] + "_" + df_base["name"])

        if ignored_reservoirs:
            df_base = df_base[~df_base["name"].isin(ignored_reservoirs)]

        # Gernerate map with latest data
        df_map = df_base.copy()
        df_map.sort_values(by=["ts_measured"], inplace=True)
        df_map.drop_duplicates(subset="id", keep="last", inplace=True)
        df_map.sort_values(by="id", inplace=True)
        df_map.reset_index(drop=True, inplace=True)

        # Add daily fill ratio
        df_map = self._add_daily_fill_percent_to_map(df_base, df_map)

        # Add weekly fill ratio
        df_map = self._add_weekly_fill_percent_to_map(df_base, df_map)

        # Add monthly fill ratio
        df_map = self._add_monthly_fill_percent_to_map(df_base, df_map)

        # Add marker size
        df_map = self._add_marker_size(df_map)

        # round all floats to 5 decimals
        df_map = df_map.round(5)

        # Rename federation names
        df_map["federation_name"].replace(
            FEDERATION_RENAMES,
            inplace=True,
        )

        if do_reservoir_rename:
            df_map["name"].replace(
                RESERVOIR_RENAMES,
                inplace=True,
            )

        return df_map


def _sort_with_special_cases(df: pd.DataFrame, pairs: list[tuple[str, str]]):
    df.insert(0, "__sort", df["capacity_mio_m3"] * 1_000_000.0)

    for hauptsperre, vorsperre in pairs:
        if any(name not in df["name"].unique() for name in [hauptsperre, vorsperre]):
            continue

        df.loc[df["name"] == vorsperre, "__sort"] = (
            df.loc[df["name"] == hauptsperre, "__sort"].values[0] - 1.0
        )

    df.sort_values(by="__sort", ascending=False, inplace=True)
    df.drop(columns=["__sort"], inplace=True)


def _make_filtered_map_exporter(federation_names: Sequence[str]) -> MapExporter:
    class FilteredMapExporter(MapExporter):
        filename = f"filtered_map_{slugify('_'.join(federation_names))}"

        def run(
            self,
            df_base: pd.DataFrame,
            do_reservoir_rename: bool = False,
            # For filtered maps, ignore "Haltern und Hullern Gesamt" because we don't use the
            # history anyways and prefer detailed data for current fill level
            ignored_reservoirs: list[str] | None = GELSENWASSER_GESAMT,
        ) -> pd.DataFrame:
            df_map = super().run(
                df_base,
                do_reservoir_rename=do_reservoir_rename,
                ignored_reservoirs=ignored_reservoirs,
            )

            translated_names = [
                FEDERATION_RENAMES.get(fed_name, fed_name) for fed_name in federation_names
            ]

            df_filtered = df_map.loc[df_map["federation_name"].isin(translated_names)].copy()

            _sort_with_special_cases(
                df_filtered,
                [
                    ("Große Dhünntalsperre", "Vorsperre Große Dhünn"),
                    ("Rurtalsperre Hauptsee", "Rurtalsperre Obersee"),
                    ("Rurtalsperre Obersee", "Urftalsperre"),
                    ("Biggetalsperre", "Listertalsperre"),
                    ("Talsperre Haltern Nordbecken", "Talsperre Haltern Südbecken"),
                    ("Talsperre Haltern Südbecken", "Talsperre Hullern"),
                ],
            )

            df_filtered["name"].replace(
                RESERVOIR_RENAMES_BREAKS,
                inplace=True,
            )

            return df_filtered.reset_index(drop=True)

    return FilteredMapExporter()  # type: ignore


def filtered_map_exporters() -> list[MapExporter]:
    filters = [
        [AggerFederation.name],
        [EifelRurFederation.name],
        [RuhrFederation.name],
        [WupperFederation.name],
        [WahnbachReservoirFederation.name, GelsenwasserFederation.name],
    ]
    return [_make_filtered_map_exporter(filter) for filter in filters]
