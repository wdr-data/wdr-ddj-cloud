from ddj_cloud.scrapers.divi_intensivregister.common import (
    add_rows_for_missing_dates,
    filter_by_latest_date,
    load_data,
    make_latest_date_single,
)
from ddj_cloud.utils.storage import upload_dataframe

url = "https://github.com/robert-koch-institut/Intensivkapazitaeten_und_COVID-19-Intensivbettenbelegung_in_Deutschland/raw/refs/heads/main/Intensivregister_Deutschland_Altersgruppen.csv"

meta_columns = [
    "datum",
    "bundesland_id",
    "bundesland_name",
]


def run():
    df = load_data(url)
    df = add_rows_for_missing_dates(df, meta_columns)

    upload_dataframe(df, "divi_intensivregister/deutschland_altersgruppen/history.csv")

    df_latest_date = filter_by_latest_date(df)
    upload_dataframe(df_latest_date, "divi_intensivregister/deutschland_altersgruppen/latest.csv")

    df_latest_date_single = make_latest_date_single(
        df_latest_date,
        meta_columns,
        {},
    )
    upload_dataframe(
        df_latest_date_single, "divi_intensivregister/deutschland_altersgruppen/latest_single.csv"
    )
