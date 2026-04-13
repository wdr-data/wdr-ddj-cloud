"""
Holt den täglichen Anteil erneuerbarer Energien von der Fraunhofer Energy Charts API
und schreibt die Daten auf eine Datawrapper-Chart (Flächengrafik).
"""

import os  # noqa: I001
from datetime import datetime

import pandas as pd
import datawrapper as dw
import requests
import sentry_sdk
from dotenv import load_dotenv

load_dotenv()

FRAUNHOFER_URL = "https://api.energy-charts.info/"
DAILY_SHARE = "ren_share_daily_avg"
SHARE_FORECAST = "ren_share_forecast"
PUBLIC_POWER = "public_power"  # Stromerzeugungs-Kapazität (öffentlich)
INSTALLED_POWER = "installed_power"
# PRICES = "price" # Spotmarkt-Preise

MIX_ID = "n3FOA"
MIX_NOTES = '<br><b style="float:left; margin: 5px; width: 45px; height: 45px; background: url(https://www.quarks.de/wp-content/uploads/Quarks-Icon-Profilbild-1x1-1.png); background-size: 45px 45px;"><em style="opacity:0.0;">quarks.de</em></b>Durchschnittlicher Anteil von Wind, Photovoltaik, Wasserkraft und Biomasse im Strommix.'
POWER_ID = "p5sHV"
POWER_NOTES = '<br><b style="float:left; margin: 5px; width: 45px; height: 45px; background: url(https://www.quarks.de/wp-content/uploads/Quarks-Icon-Profilbild-1x1-1.png); background-size: 45px 45px;"><em style="opacity:0.0;">quarks.de</em></b>Installierte Kapazitäten.'

def fetch_renewable_share(year=None):
    """Holt die täglichen Erneuerbare-Anteile für ein bestimmtes Jahr."""
    if not year:
        year = datetime.now().year
    try:
        resp = requests.get(FRAUNHOFER_URL + DAILY_SHARE, params={"country": "de", "year": year})
        resp.raise_for_status()
    except requests.RequestException as e:
        sentry_sdk.capture_exception(e)
        raise
    data = resp.json()
    return data["days"], data["data"]


def fetch_renewable_forecast():
    """Vorhersage. Umfasst idR nur den nächsten Tag in Viertelstundenschritten."""
    try:
        resp = requests.get(FRAUNHOFER_URL + SHARE_FORECAST, params={"country": "de"})
        resp.raise_for_status()
    except requests.RequestException as e:
        sentry_sdk.capture_exception(e)
        raise
    data = resp.json()
    # Unix-Sekunden in Daten umwandeln
    dates = [datetime.fromtimestamp(d).strftime("%d.%m.%Y %H:%M") for d in data["unix_seconds"]]
    return dates, data["ren_share"]


def fetch_public_power():
    """
    Holt die gesamte Stromproduktion für die letzten 24 Stunden.
    Die Daten sind in 15-Minuten-Schritten.
    """
    try:
        resp = requests.get(FRAUNHOFER_URL + PUBLIC_POWER, params={"country": "de"})
        resp.raise_for_status()
    except requests.RequestException as e:
        sentry_sdk.capture_exception(e)
        raise
    data = resp.json()
    data_df = pd.DataFrame({"time": data["unix_seconds"]}).set_index("time")
    for item in data["production_types"]:
        data_df[item["name"]] = item["data"]
    return data_df


def fetch_installed_power(time_step="yearly"):
    """Holt die installierte Leistung für die letzten 24 Stunden."""
    try:
        resp = requests.get(
            FRAUNHOFER_URL + INSTALLED_POWER, params={"country": "de", "time_step": time_step}
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        sentry_sdk.capture_exception(e)
        raise
    data = resp.json()
    data_df = pd.DataFrame({"time": data["time"]}).set_index("time")
    for item in data["production_types"]:
        data_df[item["name"]] = item["data"]
    return data_df


def fetch_last_n_years(n=10):
    """Holt die täglichen Erneuerbare-Anteile der letzten n Jahre."""
    current_year = datetime.now().year
    all_days = []
    all_values = []
    for year in range(current_year - n + 1, current_year + 1):
        print(f"  Lade {year}...")
        days, values = fetch_renewable_share(year)
        all_days.extend(days)
        all_values.extend(values)
    return all_days, all_values


def build_dataframe(days, values):
    """Baut ein DataFrame aus den Rohdaten."""
    df = pd.DataFrame({"Datum": days, "Anteil Erneuerbare (%)": values})
    df["Datum"] = pd.to_datetime(df["Datum"], format="%d.%m.%Y")
    df = df.dropna(subset=["Anteil Erneuerbare (%)"])
    return df


def aggregate_monthly(df):
    """Aggregiert die täglichen Werte zu Monatsmittelwerten."""
    df_monthly = df.resample("MS", on="Datum").agg({"Anteil Erneuerbare (%)": "mean"})
    df_monthly = df_monthly.reset_index()
    df_monthly["Anteil Erneuerbare (%)"] = df_monthly["Anteil Erneuerbare (%)"].round(1)
    return df_monthly


def aggregate_yearly(df):
    """Berechnet Jahresdurchschnitte, jeweils auf den 1. Dezember des Jahres gelegt."""
    df_yearly = df.groupby(df["Datum"].dt.year).agg({"Anteil Erneuerbare (%)": "mean"})
    df_yearly = df_yearly.reset_index()
    df_yearly.columns = ["Jahr", "Jahresdurchschnitt (%)"]
    df_yearly["Jahresdurchschnitt (%)"] = df_yearly["Jahresdurchschnitt (%)"].round(1)
    df_yearly["Datum"] = pd.to_datetime(df_yearly["Jahr"].astype(str) + "-12-01")
    df_yearly = df_yearly.drop(columns=["Jahr"])
    return df_yearly


def build_csv(df):
    """Konvertiert das DataFrame in CSV für Datawrapper."""
    df = df.copy()
    df["Datum"] = df["Datum"].dt.strftime("%Y-%m-%d")
    return df.to_csv(index=False)


def build_csv_from_index(df):
    """Konvertiert ein DataFrame mit Index als Zeitachse in CSV für Datawrapper."""
    return df.to_csv()


def dw_authenticate():
    token = os.environ.get("DATAWRAPPER_API_KEY")
    if not token:
        raise RuntimeError("Bitte DATAWRAPPER_API_KEY als Umgebungsvariable setzen.")
    client = dw.Datawrapper(access_token = token)
    return client


def upload_to_datawrapper(dw_client, dw_id, csv_data, metadata = None):
    # Update the chart
    dw_client.add_data(chart_id=dw_id, data=csv_data)
    if metadata:
        dw_client.update_metadata(
            chart_id=dw_id,
            metadata=metadata
        )
        """
        # The usual suspects:
        metadata = {
            title="Demo",
            intro="This chart shows population trends over the past decade.",
            notes="Data updated quarterly.",
            source_name="Destatis",
            source_url="https://www.destatis.de",
            byline="WDR-Data",
            "visualize": {
                "custom-colors": {
                    "Category A": "#FF6B6B",
                    "Category B": "#4ECDC4",
                    "Category C": "#45B7D1"
                }
        }
        You may also save the metadata as a JSON for remote control.
        """
    # Republish to see changes
    dw_client.publish_chart(chart_id=dw_id)
    print(f"Chart publiziert: https://datawrapper.dwcdn.net/{dw_id}/")


def update_energiemix():
    print("Datawrapper einbinden...")
    dw_client = dw_authenticate()



    print("Hole Daten der letzten 10 Jahre von Fraunhofer Energy Charts...")
    days, values = fetch_last_n_years(10)
    print(f"{len(days)} Tage geladen.")

    df = build_dataframe(days, values)
    df_monthly = aggregate_monthly(df)
    print(f"{len(df_monthly)} Monate aggregiert.")

    df_yearly = aggregate_yearly(df)
    print(f"{len(df_yearly)} Jahresdurchschnitte berechnet.")

    # Beide Reihen über Datum zusammenführen
    df_combined = pd.merge(df_monthly, df_yearly, on="Datum", how="left")
    csv_data = build_csv(df_combined)
    print("CSV erstellt.")
    metadata = {
        "notes": f"{MIX_NOTES}<br><br><i>Zuletzt aktualisiert: {datetime.now().strftime('%d.%m.%Y, %H:%M')}</i>",
    }
    upload_to_datawrapper(dw_client, MIX_ID, csv_data, metadata)

    installed_df = fetch_installed_power()
    # Daten filtern
    kapa_df = installed_df[[
        "Nuclear",
        "Fossil brown coal / lignite",
        "Fossil hard coal",
        "Fossil gas",
        "Fossil oil",
        "Other, non-renewable",
        "Hydro",
        "Biomass",
        "Wind offshore",
        "Wind onshore",
        "Solar DC",
        "Solar AC",
    ]]
    kapa_csv = build_csv_from_index(kapa_df)
    metadata = {
        "notes": f"{POWER_NOTES}<br><br><i>Zuletzt aktualisiert: {datetime.now().strftime('%d.%m.%Y, %H:%M')}</i>",
    }
    upload_to_datawrapper(dw_client, POWER_ID, kapa_csv, metadata)


    return df_combined


if __name__ == "__main__":
    update_energiemix()
