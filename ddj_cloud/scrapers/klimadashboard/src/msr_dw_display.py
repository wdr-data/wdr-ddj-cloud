"""
Datawrapper-Upload für Wind- und Solar-Ausbaudaten.

Nimmt die von msr_wind_processor und msr_solar_processor erzeugten DataFrames
und lädt sie gefiltert in Datawrapper-Charts hoch.

Charts:
- Wind-Ausbau (täglich, Gesamtleistung onshore/offshore + nötig)
- Solar-Ausbau (täglich, Gesamtleistung + nötig)
- Gesamt-Ausbau (täglich, Wind + Solar kombiniert)
- Ausbau pro Jahr (jährlich, Wind + Solar Zubau)
"""

import os

import pandas as pd
from datawrapper import Datawrapper
from dotenv import load_dotenv

load_dotenv()

# Datawrapper Chart-IDs (TODO: eintragen nach Erstellung)
CHART_WIND = ""  # Wind-Ausbau Gesamtleistung
CHART_SOLAR = ""  # Solar-Ausbau Gesamtleistung
CHART_GESAMT = ""  # Wind + Solar kombiniert
CHART_ZUBAU_JAHR = ""  # Jährlicher Zubau Wind + Solar


def _get_dw_client() -> Datawrapper:
    token = os.environ.get("DATAWRAPPER_API_KEY")
    if not token:
        msg = "Bitte DATAWRAPPER_API_KEY als Umgebungsvariable setzen."
        raise RuntimeError(msg)
    return Datawrapper(access_token=token)


def _upload_chart(client: Datawrapper, chart_id: str, df: pd.DataFrame, title: str):
    """Lädt DataFrame als CSV in einen Datawrapper-Chart hoch."""
    if not chart_id:
        print(f"  Überspringe '{title}' (keine Chart-ID konfiguriert)")
        return
    csv_data = df.to_csv(index=False)
    client.add_data(chart_id=chart_id, data=csv_data)
    client.update_metadata(chart_id=chart_id, metadata={"title": title})
    client.publish_chart(chart_id=chart_id)
    print(f"  Chart '{title}' aktualisiert ({chart_id})")


def build_wind_chart_data(
    df_onshore: pd.DataFrame,
    df_offshore: pd.DataFrame,
) -> pd.DataFrame:
    """Baut Wind-Chart: Datum, Onshore, Offshore, Onshore geplant, Offshore geplant, Nötig."""
    on = df_onshore[["datum", "installiert_gesamt", "geplant_gesamt", "noetig_gesamt"]].copy()
    on.columns = ["Datum", "Onshore (GW)", "Onshore geplant (GW)", "Nötig Onshore (GW)"]

    off = df_offshore[["datum", "installiert_gesamt", "geplant_gesamt", "noetig_gesamt"]].copy()
    off.columns = ["Datum", "Offshore (GW)", "Offshore geplant (GW)", "Nötig Offshore (GW)"]

    merged = on.merge(off, on="Datum")
    return merged


def build_solar_chart_data(df_solar: pd.DataFrame) -> pd.DataFrame:
    """Baut Solar-Chart: Datum, Installiert, Geplant, Nötig."""
    out = df_solar[["datum", "installiert_gesamt", "geplant_gesamt", "noetig_gesamt"]].copy()
    out.columns = ["Datum", "Installiert (GW)", "Geplant (GW)", "Nötig (GW)"]
    return out


def build_gesamt_chart_data(
    df_onshore: pd.DataFrame,
    df_offshore: pd.DataFrame,
    df_solar: pd.DataFrame,
) -> pd.DataFrame:
    """Baut Gesamt-Chart: Wind + Solar kombiniert."""
    wind_on = df_onshore.set_index("datum")[["installiert_gesamt"]].rename(
        columns={"installiert_gesamt": "Wind Onshore (GW)"}
    )
    wind_off = df_offshore.set_index("datum")[["installiert_gesamt"]].rename(
        columns={"installiert_gesamt": "Wind Offshore (GW)"}
    )
    solar = df_solar.set_index("datum")[["installiert_gesamt"]].rename(
        columns={"installiert_gesamt": "Solar (GW)"}
    )

    merged = wind_on.join(wind_off).join(solar)
    merged.index.name = "Datum"
    return merged.reset_index()


def build_zubau_jahr_data(
    wind_zubau: pd.DataFrame,
    solar_zubau: pd.DataFrame,
) -> pd.DataFrame:
    """Baut Jahres-Zubau-Chart: Wind + Solar MW pro Jahr."""
    w = wind_zubau.copy()
    w["Wind Onshore (MW)"] = w["onshore"]
    w["Wind Offshore (MW)"] = w["offshore"]
    w["Jahr"] = pd.to_datetime(w["datum"]).dt.year

    s = solar_zubau.copy()
    s["Solar (MW)"] = s["installiert"]
    s["Jahr"] = pd.to_datetime(s["datum"]).dt.year

    merged = w[["Jahr", "Wind Onshore (MW)", "Wind Offshore (MW)"]].merge(
        s[["Jahr", "Solar (MW)"]], on="Jahr"
    )
    return merged


def upload_all(
    df_onshore: pd.DataFrame,
    df_offshore: pd.DataFrame,
    df_solar: pd.DataFrame,
    wind_zubau_jahr: pd.DataFrame,
    solar_zubau_jahr: pd.DataFrame,
):
    """Lädt alle Charts auf Datawrapper hoch."""
    client = _get_dw_client()

    # Wind-Ausbau
    df_wind = build_wind_chart_data(df_onshore, df_offshore)
    _upload_chart(client, CHART_WIND, df_wind, "Windkraft-Ausbau in Deutschland")

    # Solar-Ausbau
    df_sol = build_solar_chart_data(df_solar)
    _upload_chart(client, CHART_SOLAR, df_sol, "Solarenergie-Ausbau in Deutschland")

    # Gesamt
    df_ges = build_gesamt_chart_data(df_onshore, df_offshore, df_solar)
    _upload_chart(client, CHART_GESAMT, df_ges, "Erneuerbarer Ausbau in Deutschland")

    # Zubau pro Jahr
    df_zub = build_zubau_jahr_data(wind_zubau_jahr, solar_zubau_jahr)
    _upload_chart(client, CHART_ZUBAU_JAHR, df_zub, "Jährlicher Zubau Wind & Solar")
