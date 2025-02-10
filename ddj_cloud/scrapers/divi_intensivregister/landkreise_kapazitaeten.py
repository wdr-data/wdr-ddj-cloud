import pandas as pd

from ddj_cloud.scrapers.divi_intensivregister.common import (
    add_rows_for_missing_dates,
    filter_by_landkreis,
    filter_by_latest_date,
    load_data,
    make_latest_date_single,
)
from ddj_cloud.utils.storage import upload_dataframe

url = "https://github.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/raw/refs/heads/main/Intensivregister_Landkreise_Kapazitaeten.csv"

meta_columns = [
    "datum",
    "bundesland_id",
    "bundesland_name",
    "landkreis_id",
    "landkreis_name",
]


def add_columns(df: pd.DataFrame):
    df["intensivbetten_gesamt"] = df["intensivbetten_frei"] + df["intensivbetten_belegt"]
    df["intensivbetten_erwachsen"] = (
        df["intensivbetten_frei_erwachsen"] + df["intensivbetten_belegt_erwachsen"]
    )
    df["intensivbetten_kind"] = df["intensivbetten_gesamt"] - df["intensivbetten_erwachsen"]
    df["intensivbetten_kind_frei_kind"] = (
        df["intensivbetten_frei"] - df["intensivbetten_frei_erwachsen"]
    )
    df["intensivbetten_kind_belegt_kind"] = (
        df["intensivbetten_belegt"] - df["intensivbetten_belegt_erwachsen"]
    )


def run():
    df = load_data(url)
    add_columns(df)

    upload_dataframe(df, "divi_intensivregister/landkreise_kapazitaeten/history.csv", archive=False)

    df_latest_date = filter_by_latest_date(df)
    upload_dataframe(df_latest_date, "divi_intensivregister/landkreise_kapazitaeten/latest.csv")

    max_date = df["datum"].max()

    for landkreis_id, df_landkreis_ in filter_by_landkreis(df):
        df_landkreis = add_rows_for_missing_dates(df_landkreis_, meta_columns, max_date=max_date)

        upload_dataframe(
            df_landkreis,
            f"divi_intensivregister/landkreise_kapazitaeten/by_landkreis/{landkreis_id:05d}/history.csv",
            archive=False,
        )

        df_landkreis_latest_date = filter_by_latest_date(df_landkreis)
        upload_dataframe(
            df_landkreis_latest_date,
            f"divi_intensivregister/landkreise_kapazitaeten/by_landkreis/{landkreis_id:05d}/latest.csv",
        )

        df_landkreis_latest_date_single = make_latest_date_single(
            df_landkreis,
            meta_columns,
            {},
        )
        upload_dataframe(
            df_landkreis_latest_date_single,
            f"divi_intensivregister/landkreise_kapazitaeten/by_landkreis/{landkreis_id:05d}/latest_single.csv",
        )
