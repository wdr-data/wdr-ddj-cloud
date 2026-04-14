import gzip
import os
from datetime import UTC, datetime

import requests

from ddj_cloud.utils.storage import upload_file

URL = (
    "https://apis.deutschebahn.com/db-api-marketplace/"
    "apis/fasta/v2/facilities?type=ESCALATOR,ELEVATOR&state=ACTIVE,INACTIVE,UNKNOWN"
)


def run():
    """
    Ruft den aktuellen Status aller Aufzüge/Rolltreppen ab
    und speichert das JSON mit Zeitstempel im Dateinamen.
    """

    if os.environ.get("STAGE") == "prod":
        print("Skipping in production, has only ever been running in staging")
        return

    client_id = os.environ.get("DB_CLIENT_ID")
    api_key = os.environ.get("DB_API_KEY")
    if not client_id or not api_key:
        msg = "Umgebungsvariablen fehlen."
        raise RuntimeError(msg)

    headers = {
        "Accept": "application/json",
        "DB-Client-ID": client_id,
        "DB-Api-Key": api_key,
    }

    resp = requests.get(URL, headers=headers, timeout=60)
    resp.raise_for_status()  # wirft Exception bei HTTP-Fehlern

    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"bahn-fasta-scraper/raw_{timestamp}.json.gz"

    # Gzip-Komprimierung des Inhalts
    compressed_content = gzip.compress(resp.content)

    upload_file(compressed_content, filename)
