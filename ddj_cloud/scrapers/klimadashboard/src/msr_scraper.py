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

    with sqlite3.connect(OPEN_MASTR_DB) as src_conn, sqlite3.connect(target_db) as dst_conn:
        for energy_type in ENERGY_TYPES:
            table_name = f"{energy_type}_extended"
            df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)  # noqa: S608

            if (
                "DatumBeginnVoruebergehendeStilllegung" in df.columns
                and "DatumEndgueltigeStilllegung" in df.columns
            ):
                df["datum_stilllegung"] = df[
                    ["DatumBeginnVoruebergehendeStilllegung", "DatumEndgueltigeStilllegung"]
                ].max(axis=1)

            df.to_sql(table_name, dst_conn, if_exists="replace", index=False)
            counts[energy_type] = len(df)

    print(json.dumps({"status": "ok", "counts": counts}))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"status": "error", "message": str(e)}))
        sys.exit(1)
