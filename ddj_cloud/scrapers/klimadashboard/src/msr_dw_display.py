"""
Datawrapper-Upload für Wind- und Solar-Ausbaudaten.

Nimmt die von msr_wind_processor und msr_solar_processor erzeugten DataFrames
und lädt sie gefiltert in Datawrapper-Charts hoch.

Charts (alle monatlich):
- Wind-Ausbau: Gesamtleistung onshore/offshore + nötig
- Solar-Ausbau: Gesamtleistung + nötig
- Wind-Zubau: Neue Kapazität onshore/offshore pro Monat
- Solar-Zubau: Neue Kapazität pro Monat
"""

import os
from datetime import datetime

import pandas as pd
from datawrapper import Datawrapper
from dotenv import load_dotenv

load_dotenv()

# Jährliche statt monatliche Aggregation
YEARLY_AGGREGATES = True

# String für die Anmerkungen
NOTES_STR = '<br><b style="float:left; margin: 5px; width: 45px; height: 45px; background: url(https://www.quarks.de/wp-content/uploads/Quarks-Icon-Profilbild-1x1-1.png); background-size: 45px 45px;"><em style="opacity:0.0;">quarks.de</em></b>'

# Datawrapper Chart-IDs (TODO: eintragen nach Erstellung)
CHART_WIND = "EgOti"  # Wind-Ausbau Gesamtleistung
CHART_SOLAR = "1rxLQ"  # Solar-Ausbau Gesamtleistung
CHART_WIND_ZUBAU = "7yMTK"  # Wind-Zubau
CHART_SOLAR_ZUBAU = "kPzGf"  # Solar-Zubau


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
    # String mit der letzten Aktualisierung
    notes_str = f"{NOTES_STR}Zuletzt aktualisiert: {datetime.now().strftime('%d.%m.%Y, %H:%M')}"
    csv_data = df.to_csv(index=False)
    client.add_data(chart_id=chart_id, data=csv_data)
    client.update_chart(chart_id=chart_id, title=title, metadata={"annotate": {"notes": notes_str}})
    client.publish_chart(chart_id=chart_id)
    print(f"  Chart '{title}' aktualisiert ({chart_id})")


def build_wind_chart_data(wind_gesamt_monatlich: pd.DataFrame) -> pd.DataFrame:
    """Wind-Gesamtleistung monatlich: Onshore, Offshore, geplant, nötig."""
    df = wind_gesamt_monatlich.copy()
    df.columns = ["Datum", "Onshore (GW)", "Onshore geplant (GW)", "Offshore (GW)", "Offshore geplant (GW)"]
    return df


def build_solar_chart_data(solar_gesamt_monatlich: pd.DataFrame) -> pd.DataFrame:
    """Solar-Gesamtleistung monatlich: Installiert, geplant."""
    df = solar_gesamt_monatlich.copy()
    df.columns = ["Datum", "Installiert (GW)", "Geplant (GW)"]
    return df


def build_wind_zubau_data(wind_zubau_monatlich: pd.DataFrame) -> pd.DataFrame:
    """Wind-Zubau monatlich: Onshore, Offshore, geplant."""
    df = wind_zubau_monatlich.copy()
    df.columns = ["Datum", "Onshore (MW)", "Onshore geplant (MW)", "Offshore (MW)", "Offshore geplant (MW)"]
    return df


def build_solar_zubau_data(solar_zubau_monatlich: pd.DataFrame) -> pd.DataFrame:
    """Solar-Zubau monatlich: Installiert, geplant."""
    df = solar_zubau_monatlich.copy()
    df.columns = ["Datum", "Installiert (MW)", "Geplant (MW)"]
    return df


def upload_all(
    wind_summaries: dict[str, pd.DataFrame],
    solar_summaries: dict[str, pd.DataFrame],
):
    """Lädt alle Charts auf Datawrapper hoch.

    Nutzt monatliche oder jährliche Daten je nach YEARLY_AGGREGATES.
    """
    suffix = "jaehrlich" if YEARLY_AGGREGATES else "monatlich"
    period = "pro Jahr" if YEARLY_AGGREGATES else "pro Monat"
    client = _get_dw_client()

    df = build_wind_chart_data(wind_summaries[f"gesamt_{suffix}"])
    _upload_chart(client, CHART_WIND, df, "Windkraft-Ausbau in Deutschland")

    df = build_solar_chart_data(solar_summaries[f"gesamt_{suffix}"])
    _upload_chart(client, CHART_SOLAR, df, "Solarenergie-Ausbau in Deutschland")

    df = build_wind_zubau_data(wind_summaries[f"zubau_{suffix}"])
    _upload_chart(client, CHART_WIND_ZUBAU, df, f"Windkraft-Zubau {period}")

    df = build_solar_zubau_data(solar_summaries[f"zubau_{suffix}"])
    _upload_chart(client, CHART_SOLAR_ZUBAU, df, f"Solarenergie-Zubau {period}")
