from ddj_cloud.scrapers.divi_intensivregister.common import (
    add_rows_for_missing_dates,
    filter_by_bundesland,
    filter_by_latest_date,
    load_data,
    make_latest_date_single,
)
from ddj_cloud.utils.storage import upload_dataframe

url = "https://github.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/raw/refs/heads/main/Intensivregister_Bundeslaender_Kapazitaeten.csv"

meta_columns = [
    "bundesland_id",
    "bundesland_name",
    "behandlungsgruppe",
    "behandlungsgruppe_level_2",
]


def run():
    df = load_data(url)
    upload_dataframe(df, "divi_intensivregister/bundeslaender_kapazitaeten/history.csv")

    df_latest_date = filter_by_latest_date(df)
    upload_dataframe(df_latest_date, "divi_intensivregister/bundeslaender_kapazitaeten/latest.csv")

    max_date = df["datum"].max()

    for bundesland_id, df_bundesland_ in filter_by_bundesland(df):
        df_bundesland = add_rows_for_missing_dates(df_bundesland_, meta_columns, max_date=max_date)

        upload_dataframe(
            df_bundesland,
            f"divi_intensivregister/bundeslaender_kapazitaeten/by_bundesland/{bundesland_id:02d}/history.csv",
        )

        df_bundesland_latest_date = filter_by_latest_date(df_bundesland)
        upload_dataframe(
            df_bundesland_latest_date,
            f"divi_intensivregister/bundeslaender_kapazitaeten/by_bundesland/{bundesland_id:02d}/latest.csv",
        )

        df_bundesland_latest_date_single = make_latest_date_single(
            df_bundesland,
            ["datum", *meta_columns],
            {},
        )
        upload_dataframe(
            df_bundesland_latest_date_single,
            f"divi_intensivregister/bundeslaender_kapazitaeten/by_bundesland/{bundesland_id:02d}/latest_single.csv",
        )
