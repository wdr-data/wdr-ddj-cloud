import json
import subprocess
from pathlib import Path

import pandas as pd

from ddj_cloud.scrapers.klimadashboard.src.energiemix import update_energiemix
from ddj_cloud.scrapers.klimadashboard.src.msr_wind_processor import process_wind
from ddj_cloud.utils.storage import upload_dataframe

VERSION_STRING = "V0.03 vom 10.04.2026"

DB_LOCAL_PATH = Path(__file__).parent / "src" / "mastr.db"
SCRAPER_SCRIPT = Path(__file__).parent / "src" / "msr_scraper.py"


def _run_scraper():
    """Führt den MaStR-Scraper in einem isolierten venv aus (via uv run)."""
    result = subprocess.run(
        ["uv", "run", str(SCRAPER_SCRIPT), str(DB_LOCAL_PATH)],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        try:
            status = json.loads(result.stdout.strip())
            msg = status.get("message", stderr)
        except (json.JSONDecodeError, ValueError):
            msg = stderr or result.stdout.strip()
        err_msg = f"MaStR-Scraper fehlgeschlagen: {msg}"
        raise RuntimeError(err_msg)

    status = json.loads(result.stdout.strip())
    counts = status.get("counts", {})
    total = sum(counts.values())
    print(f"  MaStR-Scraper: {total} Einheiten geladen ({counts})")


def run():
    # Energiemix (Fraunhofer API)
    df = update_energiemix()
    upload_dataframe(df, "klimadashboard/test_energiemix1.csv")

    # MaStR Wind-Ausbau
    print("MaStR Wind-Daten aktualisieren...")
    _run_scraper()
    df_onshore, df_offshore = process_wind(DB_LOCAL_PATH)

    # Ergebnisse als CSV auf S3 hochladen
    df_combined = pd.concat([df_onshore, df_offshore], ignore_index=True)
    upload_dataframe(df_combined, "klimadashboard/wind_taeglich.csv")
    print("MaStR Wind-Daten aktualisiert.")
