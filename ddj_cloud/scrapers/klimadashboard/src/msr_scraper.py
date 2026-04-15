"""
MaStR-Scraper: Lädt alle Energiearten aus dem Marktstammdatenregister
über die open-mastr-Bibliothek (Bulk-Download, kein API-Key nötig).

Schreibt die Daten in eine lokale SQLite-Datenbank (mastr.db).
"""

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import sentry_sdk
from open_mastr import Mastr

ENERGY_TYPES = ["wind", "solar", "biomass", "hydro", "combustion", "nuclear", "gsgk", "storage"]

OPEN_MASTR_DB = Path.home() / ".open-MaStR" / "data" / "sqlite" / "open-mastr.db"


def _check_last_download(target_db: Path) -> str | None:
    """Prüft DatumDownload in der Ziel-DB. Gibt Datum als String oder None zurück."""
    if not target_db.exists():
        return None
    try:
        with sqlite3.connect(target_db) as conn:
            row = conn.execute("SELECT DatumDownload FROM wind_extended LIMIT 1").fetchone()
            return row[0] if row else None
    except Exception:
        return None


def scrape_mastr(db_path: Path) -> dict[str, int]:
    """Lädt alle Energiearten via open-mastr und schreibt sie in die lokale DB.

    Überspringt den Download wenn die DB bereits Daten von heute enthält.

    Args:
        db_path: Pfad zur Ziel-SQLite-Datenbank (mastr.db)

    Returns:
        Dict mit Anzahl der Einheiten pro Energieart.
    """
    # Skip download if DB already has today's data
    last_download = _check_last_download(db_path)
    today = date.today().isoformat()

    if last_download == today:
        print(f"  DB bereits aktuell ({today}), überspringe Download.")
        counts = {}
        with sqlite3.connect(db_path) as conn:
            for energy_type in ENERGY_TYPES:
                table_name = f"{energy_type}_extended"
                try:
                    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # noqa: S608
                    counts[energy_type] = row[0]
                except Exception:
                    counts[energy_type] = 0
        return counts

    print(f"  Letzter Download: {last_download or 'keiner'}, starte Update...")

    try:
        mastr = Mastr()
        mastr.download(data=ENERGY_TYPES)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise

    counts = {}
    total = len(ENERGY_TYPES)

    with sqlite3.connect(OPEN_MASTR_DB) as src_conn, sqlite3.connect(db_path) as dst_conn:
        for i, energy_type in enumerate(ENERGY_TYPES, 1):
            table_name = f"{energy_type}_extended"
            print(f"  [{i}/{total}] {energy_type}...")

            try:
                df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)  # noqa: S608
            except Exception as e:
                print(f"  Warnung: Tabelle {table_name} nicht gefunden: {e}")
                sentry_sdk.capture_exception(e)
                continue

            col_temp = "DatumBeginnVoruebergehendeStilllegung"
            col_final = "DatumEndgueltigeStilllegung"
            if col_temp in df.columns and col_final in df.columns:
                temp = pd.to_datetime(df[col_temp], errors="coerce")
                final = pd.to_datetime(df[col_final], errors="coerce")
                combined = pd.concat([temp, final], axis=1).max(axis=1)
                df["datum_stilllegung"] = combined.dt.strftime("%Y-%m-%d")
                df.loc[combined.isna(), "datum_stilllegung"] = None

            df.to_sql(table_name, dst_conn, if_exists="replace", index=False)
            counts[energy_type] = len(df)
            print(f"  [{i}/{total}] {energy_type}: {len(df)} Einheiten")

    return counts


if __name__ == "__main__":
    scrape_mastr(Path(__file__).parent / "mastr.db")
