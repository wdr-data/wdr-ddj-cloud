"""
Wind-Prozessor: Berechnet tägliche Ausbaudaten (installiert, geplant, nötig)
aus der lokalen MaStR-Datenbank.

Liest aus wind_extended, schreibt in ee_wind_taeglich.
Pendant zu msr_php/wka_to_data.php.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import sentry_sdk

# Ausbauziele 2030
TARGET_ONSHORE_GW = 115  # Wind-an-Land-Gesetz, in Kraft seit 01.02.2023
TARGET_OFFSHORE_GW = 30  # Wind-auf-See-Gesetz, in Kraft seit 01.01.2023
TARGET_DATE = "2031-01-01"
BASELINE_ONSHORE = "2023-02-01"
BASELINE_OFFSHORE = "2023-01-01"

# Lage-Filter (Werte aus open-mastr nach Bulk-Cleansing)
ONSHORE_LABELS = ("Windkraft an Land",)
OFFSHORE_LABELS = ("Windkraft auf See",)

# Status-Filter für inaktive Anlagen
INACTIVE_STATUSES = ("In Planung", "Vorübergehend stillgelegt")


def _create_result_table(db: sqlite3.Connection):
    """Erstellt die Ergebnis-Tabelle ee_wind_taeglich."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS ee_wind_taeglich (
            datum TEXT NOT NULL,
            lage_einheit TEXT NOT NULL,
            installiert_gesamt REAL,
            installiert_taeglich REAL,
            geplant_gesamt REAL,
            geplant_taeglich REAL,
            noetig_gesamt REAL,
            noetig_taeglich REAL,
            stand TEXT NOT NULL,
            PRIMARY KEY (datum, lage_einheit)
        )
    """)
    db.commit()


def _calculate_daily_capacity(
    df: pd.DataFrame,
    location_labels: tuple[str, ...],
    target_gw: float,
    baseline_date: str,
) -> pd.DataFrame:
    """
    Berechnet für eine Lage (onshore/offshore) die täglichen Ausbaudaten.

    Identische Logik wie im Original msr_wind.py, angepasst an open-mastr-Spaltennamen.
    """
    heute = datetime.now().strftime("%Y-%m-%d")

    # Aktive Anlagen filtern (nicht in Planung, nicht stillgelegt)
    active = df[
        df["WindAnLandOderAufSee"].isin(location_labels)
        & ~df["EinheitBetriebsstatus"].isin(INACTIVE_STATUSES)
    ].copy()
    active["Inbetriebnahmedatum"] = pd.to_datetime(active["Inbetriebnahmedatum"])
    active["datum_stilllegung"] = pd.to_datetime(active["datum_stilllegung"])
    active["Nettonennleistung"] = pd.to_numeric(active["Nettonennleistung"], errors="coerce")

    # Geplante Anlagen
    planned = df[
        df["WindAnLandOderAufSee"].isin(location_labels)
        & (df["EinheitBetriebsstatus"] == "In Planung")
    ].copy()
    planned["GeplantesInbetriebnahmedatum"] = pd.to_datetime(
        planned["GeplantesInbetriebnahmedatum"]
    )
    planned["Nettonennleistung"] = pd.to_numeric(planned["Nettonennleistung"], errors="coerce")

    # Vorleistung bis Ende 2009
    pre_2010 = active[active["Inbetriebnahmedatum"] < "2010-01-01"]["Nettonennleistung"].sum()
    decom_pre_2010 = active[
        active["datum_stilllegung"].notna() & (active["datum_stilllegung"] < "2010-01-01")
    ]["Nettonennleistung"].sum()
    base_capacity_kw = pre_2010 - decom_pre_2010

    # Zubau pro Tag (kW)
    additions = (
        active[active["Inbetriebnahmedatum"] >= "2010-01-01"]
        .groupby("Inbetriebnahmedatum")["Nettonennleistung"]
        .sum()
    )
    # Abbau pro Tag (kW)
    removals = (
        active[active["datum_stilllegung"].notna() & (active["datum_stilllegung"] >= "2010-01-01")]
        .groupby("datum_stilllegung")["Nettonennleistung"]
        .sum()
    )
    # Geplante Zubauten pro Tag (kW)
    planned_additions = (
        planned[planned["GeplantesInbetriebnahmedatum"].notna()]
        .groupby("GeplantesInbetriebnahmedatum")["Nettonennleistung"]
        .sum()
    )

    baseline_dt = pd.Timestamp(baseline_date)
    target_dt = pd.Timestamp(TARGET_DATE)
    days_to_target = (target_dt - baseline_dt).days

    rows = _iterate_date_range(
        date_range=pd.date_range("2010-01-01", "2030-12-31", freq="D"),
        additions=additions,
        removals=removals,
        planned_additions=planned_additions,
        base_capacity_kw=base_capacity_kw,
        target_gw=target_gw,
        baseline_dt=baseline_dt,
        days_to_target=days_to_target,
        heute=heute,
    )
    return pd.DataFrame(rows)


def _iterate_date_range(  # noqa: PLR0913
    date_range: pd.DatetimeIndex,
    additions: pd.Series,
    removals: pd.Series,
    planned_additions: pd.Series,
    base_capacity_kw: float,
    target_gw: float,
    baseline_dt: pd.Timestamp,
    days_to_target: int,
    heute: str,
) -> list[dict]:
    """Iteriert über den Datumsbereich und berechnet Tagesdaten."""
    rows = []
    cumulative_kw = base_capacity_kw
    cumulative_planned_gw = 0.0
    baseline_capacity_gw = None
    cumulative_needed_gw = None
    daily_needed_gw = None

    for day in date_range:
        # Zubau/Abbau des Tages
        added_kw = additions.get(day, 0.0)
        removed_kw = removals.get(day, 0.0)
        net_kw = added_kw - removed_kw
        cumulative_kw += net_kw
        cumulative_gw = round(cumulative_kw / 1_000_000, 2)
        daily_mw = round(net_kw / 1_000, 1)

        # Baseline-Stand merken
        if day == baseline_dt:
            baseline_capacity_gw = cumulative_gw

        # Nötige Leistung (ab Baseline)
        noetig_gesamt = None
        noetig_taeglich = None
        if baseline_capacity_gw is not None and day >= baseline_dt:
            if daily_needed_gw is None:
                daily_needed_gw = (target_gw - baseline_capacity_gw) / days_to_target
                cumulative_needed_gw = baseline_capacity_gw
            cumulative_needed_gw += daily_needed_gw
            noetig_gesamt = round(cumulative_needed_gw, 2)
            noetig_taeglich = round(daily_needed_gw * 1000, 1)

        # Geplante Zubauten (nur in Zukunft)
        geplant_gesamt = None
        geplant_taeglich = None
        day_str = day.strftime("%Y-%m-%d")
        if day_str >= heute:
            planned_kw = planned_additions.get(day, 0.0)
            cumulative_planned_gw += planned_kw / 1_000_000
            geplant_gesamt = round(cumulative_gw + cumulative_planned_gw, 2)
            geplant_taeglich = round(planned_kw / 1_000, 1)

        row = {
            "datum": day_str,
            "installiert_gesamt": cumulative_gw if day_str <= heute else None,
            "installiert_taeglich": daily_mw if day_str <= heute else None,
            "geplant_gesamt": geplant_gesamt,
            "geplant_taeglich": geplant_taeglich,
            "noetig_gesamt": noetig_gesamt,
            "noetig_taeglich": noetig_taeglich,
        }
        rows.append(row)

    return rows


def _aggregate_summaries(
    df_onshore: pd.DataFrame,
    df_offshore: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Erstellt monatliche und jährliche Zusammenfassungen.

    Gibt dict mit 4 DataFrames zurück:
    - gesamt_monatlich: Gesamtleistung (GW) zum Monatsende
    - zubau_monatlich: Neue Kapazität (MW) pro Monat
    - gesamt_jaehrlich: Gesamtleistung (GW) zum Jahresende
    - zubau_jaehrlich: Neue Kapazität (MW) pro Jahr
    """
    # Datum als Index für Resampling
    for df in (df_onshore, df_offshore):
        df["_datum"] = pd.to_datetime(df["datum"])

    results = {}
    for freq, label in [("M", "monatlich"), ("Y", "jaehrlich")]:
        # Gesamtleistung: letzter Wert der Periode
        gesamt = pd.DataFrame({"datum": df_onshore.set_index("_datum").resample(freq).last().index})
        gesamt["datum"] = gesamt["datum"].dt.strftime("%Y-%m-%d")

        on = df_onshore.set_index("_datum").resample(freq).last()
        off = df_offshore.set_index("_datum").resample(freq).last()
        gesamt["onshore"] = on["installiert_gesamt"].values
        gesamt["onshore_geplant"] = on["geplant_gesamt"].values
        gesamt["offshore"] = off["installiert_gesamt"].values
        gesamt["offshore_geplant"] = off["geplant_gesamt"].values
        results[f"gesamt_{label}"] = gesamt

        # Zubau: Summe der Periode (MW)
        zubau = pd.DataFrame({"datum": df_onshore.set_index("_datum").resample(freq).sum(numeric_only=True).index})
        zubau["datum"] = zubau["datum"].dt.strftime("%Y-%m-%d")

        on_sum = df_onshore.set_index("_datum").resample(freq).sum(numeric_only=True)
        off_sum = df_offshore.set_index("_datum").resample(freq).sum(numeric_only=True)
        zubau["onshore"] = on_sum["installiert_taeglich"].values
        zubau["onshore_geplant"] = on_sum["geplant_taeglich"].values
        zubau["offshore"] = off_sum["installiert_taeglich"].values
        zubau["offshore_geplant"] = off_sum["geplant_taeglich"].values
        results[f"zubau_{label}"] = zubau

    # Temporäre Spalte entfernen
    for df in (df_onshore, df_offshore):
        df.drop(columns=["_datum"], inplace=True)

    return results


def process_wind(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    """
    Berechnet die täglichen Ausbaudaten für Onshore und Offshore Wind.

    Liest aus wind_extended in mastr.db, schreibt ee_wind_taeglich.
    Gibt (df_onshore, df_offshore, summaries) zurück.
    """
    db = sqlite3.connect(db_path)
    _create_result_table(db)

    try:
        df = pd.read_sql_query("SELECT * FROM wind_extended", db)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        msg = "wind_extended-Tabelle nicht gefunden in mastr.db"
        raise RuntimeError(msg) from e

    print(f"  {len(df)} Wind-Einheiten in der Datenbank.")

    print("  Berechne Onshore-Daten...")
    df_onshore = _calculate_daily_capacity(df, ONSHORE_LABELS, TARGET_ONSHORE_GW, BASELINE_ONSHORE)
    df_onshore["lage_einheit"] = "Windkraft an Land"

    print("  Berechne Offshore-Daten...")
    df_offshore = _calculate_daily_capacity(df, OFFSHORE_LABELS, TARGET_OFFSHORE_GW, BASELINE_OFFSHORE)
    df_offshore["lage_einheit"] = "Windkraft auf See"

    # In DB speichern
    heute = datetime.now().strftime("%Y-%m-%d")
    db.execute("DELETE FROM ee_wind_taeglich")
    for row_df in (df_onshore, df_offshore):
        for _, row in row_df.iterrows():
            db.execute(
                """INSERT INTO ee_wind_taeglich
                   (datum, lage_einheit, installiert_gesamt, installiert_taeglich,
                    geplant_gesamt, geplant_taeglich, noetig_gesamt, noetig_taeglich, stand)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row["datum"],
                    row["lage_einheit"],
                    row.get("installiert_gesamt"),
                    row.get("installiert_taeglich"),
                    row.get("geplant_gesamt"),
                    row.get("geplant_taeglich"),
                    row.get("noetig_gesamt"),
                    row.get("noetig_taeglich"),
                    heute,
                ),
            )
    db.commit()
    db.close()

    print(f"  {len(df_onshore) + len(df_offshore)} Tagesdatensätze berechnet.")

    # Monatliche und jährliche Zusammenfassungen
    print("  Berechne monatliche/jährliche Zusammenfassungen...")
    summaries = _aggregate_summaries(df_onshore, df_offshore)

    return df_onshore, df_offshore, summaries


if __name__ == "__main__":
    process_wind(Path(__file__).parent / "mastr.db")
