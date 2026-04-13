# /// script
# dependencies = ["open-mastr>=0.17"]
# requires-python = ">=3.11"
# ///

import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd
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


def main():
    from datetime import date

    target_db = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "mastr.db"

    # Skip download if DB already has today's data
    last_download = _check_last_download(target_db)
    today = date.today().isoformat()

    if last_download == today:
        print(f"DB already up to date ({today}), skipping download.", file=sys.stderr, flush=True)
        # Read counts from existing DB
        counts = {}
        with sqlite3.connect(target_db) as conn:
            for energy_type in ENERGY_TYPES:
                table_name = f"{energy_type}_extended"
                try:
                    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # noqa: S608
                    counts[energy_type] = row[0]
                except Exception:
                    counts[energy_type] = 0
        print(json.dumps({"status": "ok", "counts": counts}))
        return

    print(f"Last download: {last_download or 'none'}, updating...", file=sys.stderr, flush=True)

    mastr = Mastr()
    mastr.download(data=ENERGY_TYPES)

    counts = {}
    total = len(ENERGY_TYPES)

    with sqlite3.connect(OPEN_MASTR_DB) as src_conn, sqlite3.connect(target_db) as dst_conn:
        for i, energy_type in enumerate(ENERGY_TYPES, 1):
            table_name = f"{energy_type}_extended"
            # Progress to stderr (visible live), JSON result to stdout
            print(f"[{i}/{total}] {energy_type}...", file=sys.stderr, flush=True)

            df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)  # noqa: S608

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
            print(f"[{i}/{total}] {energy_type}: {len(df)} Einheiten", file=sys.stderr, flush=True)

    print(json.dumps({"status": "ok", "counts": counts}))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"status": "error", "message": str(e)}))
        sys.exit(1)
