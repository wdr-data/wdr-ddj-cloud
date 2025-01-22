import pandas as pd

from ddj_cloud.scrapers.divi_intensivregister.common import (
    filter_by_bundesland,
    filter_by_latest_date,
    load_data,
)
from ddj_cloud.utils.storage import upload_dataframe

url = "https://github.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/raw/refs/heads/main/Intensivregister_Bundeslaender_Kapazitaeten.csv"


def run():
    df = load_data(url)
    upload_dataframe(df, "divi_intensivregister/bundeslaender_kapazitaeten/history.csv")

    df_latest_date = filter_by_latest_date(df)
    upload_dataframe(df_latest_date, "divi_intensivregister/bundeslaender_kapazitaeten/latest.csv")

    for bundesland_id, df_bundesland in filter_by_bundesland(df):
        upload_dataframe(
            df_bundesland,
            f"divi_intensivregister/bundeslaender_kapazitaeten/by_bundesland/{bundesland_id:02d}/history.csv",
        )

        df_bundesland_latest_date = filter_by_latest_date(df_bundesland)
        upload_dataframe(
            df_bundesland_latest_date,
            f"divi_intensivregister/bundeslaender_kapazitaeten/by_bundesland/{bundesland_id:02d}/latest.csv",
        )
