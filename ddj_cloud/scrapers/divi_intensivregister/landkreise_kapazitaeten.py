import pandas as pd

from ddj_cloud.scrapers.divi_intensivregister.common import (
    filter_by_landkreis,
    filter_by_latest_date,
    load_data,
)
from ddj_cloud.utils.storage import upload_dataframe

url = "https://github.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/raw/refs/heads/main/Intensivregister_Landkreise_Kapazitaeten.csv"


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

    for landkreis_id, df_landkreis in filter_by_landkreis(df):
        upload_dataframe(
            df_landkreis,
            f"divi_intensivregister/landkreise_kapazitaeten/by_landkreis/{landkreis_id:05d}/history.csv",
            archive=False,
        )

        df_bundesland_latest_date = filter_by_latest_date(df_landkreis)
        upload_dataframe(
            df_bundesland_latest_date,
            f"divi_intensivregister/landkreise_kapazitaeten/by_landkreis/{landkreis_id:05d}/latest.csv",
        )
