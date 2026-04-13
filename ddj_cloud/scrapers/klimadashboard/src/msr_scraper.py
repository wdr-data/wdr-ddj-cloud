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


def main():
    target_db = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "mastr.db"

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
