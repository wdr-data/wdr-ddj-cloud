import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from ddj_cloud.scrapers.klimadashboard.src.energiemix import update_energiemix
from ddj_cloud.scrapers.klimadashboard.src.msr_dw_display import upload_all as upload_dw_charts
from ddj_cloud.scrapers.klimadashboard.src.msr_solar_processor import process_solar
from ddj_cloud.scrapers.klimadashboard.src.msr_wind_processor import process_wind
from ddj_cloud.utils.storage import (
    upload_dataframe,
    upload_file,
)

VERSION_STRING = "V0.04 vom 13.04.2026"

# mastr.db in local_storage (analog zu anderen Scrapern)
DB_LOCAL_PATH = Path(__file__).parent.parent.parent.parent / "local_storage" / "klimadashboard" / "mastr.db"
DB_S3_KEY = "klimadashboard/mastr.db"
SCRAPER_SCRIPT = Path(__file__).parent / "src" / "msr_scraper.py"



def _upload_db():
    """Lädt mastr.db auf S3 hoch."""
    if not DB_LOCAL_PATH.exists():
        print("  Warnung: mastr.db nicht gefunden, Upload übersprungen.")
        return
    upload_file(
        DB_LOCAL_PATH.read_bytes(),
        DB_S3_KEY,
        archive=False,
    )
    size_mb = DB_LOCAL_PATH.stat().st_size / 1024 / 1024
    print(f"  mastr.db auf S3 hochgeladen ({size_mb:.1f} MB)")


def _run_scraper():
    """Führt den MaStR-Scraper in einem isolierten venv aus (via uv run).

    Stderr wird live gestreamt (Fortschritt), stdout enthält JSON-Ergebnis.
    """
    DB_LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)

    process = subprocess.Popen(
        ["uv", "run", str(SCRAPER_SCRIPT), str(DB_LOCAL_PATH)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Stream stderr live for progress
    for line in process.stderr:
        print(f"  {line}", end="", file=sys.stderr, flush=True)

    process.wait()
    stdout = process.stdout.read()

    if process.returncode != 0:
        try:
            status = json.loads(stdout.strip())
            msg = status.get("message", "")
        except (json.JSONDecodeError, ValueError):
            msg = stdout.strip()
        err_msg = f"MaStR-Scraper fehlgeschlagen: {msg}"
        raise RuntimeError(err_msg)

    status = json.loads(stdout.strip())
    counts = status.get("counts", {})
    total = sum(counts.values())
    print(f"  MaStR-Scraper: {total} Einheiten geladen")


def run():
    # Energiemix (Fraunhofer API)
    df = update_energiemix()
    upload_dataframe(df, "klimadashboard/test_energiemix1.csv")

    # MaStR: Scraper, Prozessoren, DB auf S3
    print("MaStR-Daten aktualisieren...")
    _run_scraper()

    # Wind
    print("Wind-Daten verarbeiten...")
    df_onshore, df_offshore, wind_summaries = process_wind(DB_LOCAL_PATH)
    df_wind = pd.concat([df_onshore, df_offshore], ignore_index=True)
    upload_dataframe(df_wind, "klimadashboard/wind_taeglich.csv")
    for name, df_summary in wind_summaries.items():
        upload_dataframe(df_summary, f"klimadashboard/wind_{name}.csv")

    # Solar
    print("Solar-Daten verarbeiten...")
    df_solar, solar_summaries = process_solar(DB_LOCAL_PATH)
    upload_dataframe(df_solar, "klimadashboard/solar_taeglich.csv")
    for name, df_summary in solar_summaries.items():
        upload_dataframe(df_summary, f"klimadashboard/solar_{name}.csv")

    # Datawrapper-Charts aktualisieren
    print("Datawrapper-Charts aktualisieren...")
    upload_dw_charts(
        wind_gesamt_monatlich=wind_summaries["gesamt_monatlich"],
        wind_zubau_monatlich=wind_summaries["zubau_monatlich"],
        solar_gesamt_monatlich=solar_summaries["gesamt_monatlich"],
        solar_zubau_monatlich=solar_summaries["zubau_monatlich"],
    )

    # DB auf S3 hochladen
    _upload_db()

    print("MaStR-Daten aktualisiert.")
