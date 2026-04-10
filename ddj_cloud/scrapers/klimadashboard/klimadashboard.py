import json
import subprocess
from pathlib import Path

from ddj_cloud.scrapers.klimadashboard.src.energiemix import update_energiemix
from ddj_cloud.scrapers.klimadashboard.src.msr_wind_processor import process_wind
from ddj_cloud.utils.storage import (
    DownloadFailedException,
    download_file,
    upload_dataframe,
    upload_file,
)

VERSION_STRING = "V0.02 vom 10.04.2026"

DB_S3_KEY = "klimadashboard/mastr.db"
DB_LOCAL_PATH = Path(__file__).parent / "src" / "mastr.db"
SCRAPER_SCRIPT = Path(__file__).parent / "src" / "msr_scraper.py"


def _download_db():
    """Lädt mastr.db von S3 herunter (falls vorhanden)."""
    try:
        bio = download_file(DB_S3_KEY)
        DB_LOCAL_PATH.write_bytes(bio.read())
        size_mb = DB_LOCAL_PATH.stat().st_size / 1024 / 1024
        print(f"  mastr.db von S3 heruntergeladen ({size_mb:.1f} MB)")
    except DownloadFailedException:
        print("  Keine mastr.db auf S3 gefunden (erster Lauf).")


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
    """Führt den MaStR-Scraper in einem isolierten venv aus (via uv run)."""
    result = subprocess.run(
        ["uv", "run", str(SCRAPER_SCRIPT), str(DB_LOCAL_PATH)],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        # Versuche JSON-Status aus stdout zu parsen
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
    _download_db()
    _run_scraper()
    df_onshore, df_offshore = process_wind(DB_LOCAL_PATH)
    _upload_db()
    print("MaStR Wind-Daten aktualisiert.")
