# MaStR open-mastr Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SOAP-API-based `msr_wind.py` with two scripts (scraper + processor) using the `open-mastr` library, downloading all energy types into `mastr.db` with S3 integration.

**Architecture:** A generic scraper (`msr_scraper.py`) downloads all MaStR data via open-mastr and writes it to a local `mastr.db`. A wind-specific processor (`msr_wind_processor.py`) reads from that DB and calculates daily expansion data. The orchestrator (`klimadashboard.py`) handles S3 download/upload of the DB file.

**Tech Stack:** Python 3.11, open-mastr, SQLite, pandas, boto3 (via `ddj_cloud.utils.storage`)

**Spec:** `docs/superpowers/specs/2026-04-02-msr-wind-open-mastr-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify | Add `open-mastr` dependency |
| `src/msr_scraper.py` | Create | Download all energy types from MaStR via open-mastr, write to `mastr.db` |
| `src/msr_wind_processor.py` | Create | Read wind data from `mastr.db`, calculate daily expansion stats, write `ee_wind_taeglich` |
| `klimadashboard.py` | Modify | Add S3 download/upload of `mastr.db`, call scraper + processor |
| `src/msr_wind.py` | Delete | Replaced by scraper + processor |
| `README_msr.md` | Modify | Update for new architecture |

---

### Task 1: Add open-mastr dependency

**Files:**
- Modify: `pyproject.toml:12-35`

- [ ] **Step 1: Add open-mastr to dependencies**

In `pyproject.toml`, add `open-mastr` to the `dependencies` list. Add after the `requests` line:

```toml
    "open-mastr~=0.17",
```

- [ ] **Step 2: Install dependencies**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv sync`
Expected: open-mastr and its dependencies install successfully.

- [ ] **Step 3: Verify import works**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python -c "from open_mastr import Mastr; print('open-mastr OK')"`
Expected: `open-mastr OK`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat: add open-mastr dependency"
```

---

### Task 2: Create the MaStR scraper (`msr_scraper.py`)

**Files:**
- Create: `src/msr_scraper.py`

This scraper downloads all energy types from the MaStR bulk export via open-mastr and writes them into our own `mastr.db`. It copies relevant tables from the open-mastr DB to our DB, adding a computed `datum_stilllegung` column where applicable.

- [ ] **Step 1: Create `src/msr_scraper.py`**

```python
"""
MaStR-Scraper: Lädt alle Energiearten aus dem Marktstammdatenregister
über die open-mastr-Bibliothek (Bulk-Download, kein API-Key nötig).

Schreibt die Daten in eine lokale SQLite-Datenbank (mastr.db).
"""

import sqlite3
from pathlib import Path

import pandas as pd
import sentry_sdk
from open_mastr import Mastr

# Energiearten, die heruntergeladen werden
ENERGY_TYPES = ["wind", "solar", "biomass", "hydro", "combustion", "nuclear", "gsgk", "storage"]

# Mapping: open-mastr data-Name -> Tabellenname in der open-mastr-DB
TABLE_NAMES = {
    "wind": "wind_extended",
    "solar": "solar_extended",
    "biomass": "biomass_extended",
    "hydro": "hydro_extended",
    "combustion": "combustion_extended",
    "nuclear": "nuclear_extended",
    "gsgk": "gsgk_extended",
    "storage": "storage_extended",
}

# Technologien mit Stilllegungsdaten (max aus zwei Datumsfeldern)
TECHS_WITH_SHUTDOWN = {"wind", "solar", "biomass", "hydro", "combustion", "nuclear", "gsgk", "storage"}


def _compute_shutdown_date(df: pd.DataFrame) -> pd.DataFrame:
    """Berechnet datum_stilllegung als max(DatumBeginnVoruebergehendeStilllegung, DatumEndgueltigeStilllegung)."""
    col_temp = "DatumBeginnVoruebergehendeStilllegung"
    col_final = "DatumEndgueltigeStilllegung"

    if col_temp not in df.columns and col_final not in df.columns:
        return df

    if col_temp in df.columns and col_final in df.columns:
        temp = pd.to_datetime(df[col_temp], errors="coerce")
        final = pd.to_datetime(df[col_final], errors="coerce")
        df["datum_stilllegung"] = temp.where(temp > final, final)
        df["datum_stilllegung"] = df["datum_stilllegung"].dt.strftime("%Y-%m-%d")
    elif col_temp in df.columns:
        df["datum_stilllegung"] = df[col_temp]
    else:
        df["datum_stilllegung"] = df[col_final]

    return df


def _get_open_mastr_db_path() -> Path:
    """Gibt den Pfad zur open-mastr SQLite-Datenbank zurück."""
    return Path.home() / ".open-MaStR" / "data" / "sqlite" / "open-mastr.db"


def scrape_mastr(db_path: Path) -> dict[str, int]:
    """
    Lädt alle Energiearten via open-mastr und schreibt sie in die lokale DB.

    Args:
        db_path: Pfad zur Ziel-SQLite-Datenbank (mastr.db)

    Returns:
        Dict mit Anzahl der Einheiten pro Energieart.
    """
    print("MaStR Bulk-Download starten...")

    # Schritt 1: open-mastr Bulk-Download
    try:
        mastr = Mastr()
        mastr.download(data=ENERGY_TYPES)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise RuntimeError("open-mastr Bulk-Download fehlgeschlagen") from e

    # Schritt 2: Daten aus open-mastr-DB lesen und in unsere DB schreiben
    open_mastr_db = _get_open_mastr_db_path()
    if not open_mastr_db.exists():
        msg = f"open-mastr-DB nicht gefunden: {open_mastr_db}"
        raise FileNotFoundError(msg)

    counts = {}
    source_conn = sqlite3.connect(open_mastr_db)
    target_conn = sqlite3.connect(db_path)

    try:
        for energy_type, table_name in TABLE_NAMES.items():
            print(f"  Kopiere {energy_type} ({table_name})...")
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", source_conn)  # noqa: S608
            except Exception as e:
                print(f"  Warnung: Tabelle {table_name} nicht gefunden: {e}")
                sentry_sdk.capture_exception(e)
                continue

            if energy_type in TECHS_WITH_SHUTDOWN:
                df = _compute_shutdown_date(df)

            # In unsere DB schreiben (Tabelle komplett ersetzen)
            df.to_sql(table_name, target_conn, if_exists="replace", index=False)
            counts[energy_type] = len(df)
            print(f"  {len(df)} Einheiten für {energy_type} geschrieben.")

        target_conn.commit()
    finally:
        source_conn.close()
        target_conn.close()

    print(f"MaStR-Daten in {db_path} geschrieben: {counts}")
    return counts


if __name__ == "__main__":
    scrape_mastr(Path(__file__).parent / "mastr.db")
```

- [ ] **Step 2: Verify syntax**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python -c "import ast; ast.parse(open('ddj_cloud/scrapers/klimadashboard/src/msr_scraper.py').read()); print('Syntax OK')"`
Expected: `Syntax OK`

- [ ] **Step 3: Run linter**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run ruff check ddj_cloud/scrapers/klimadashboard/src/msr_scraper.py`
Expected: No errors (fix any that appear).

- [ ] **Step 4: Commit**

```bash
git add ddj_cloud/scrapers/klimadashboard/src/msr_scraper.py
git commit -m "feat: add MaStR scraper using open-mastr bulk download"
```

---

### Task 3: Create the wind processor (`msr_wind_processor.py`)

**Files:**
- Create: `src/msr_wind_processor.py`

This processor reads wind units from `mastr.db` (table `wind_extended`) and calculates daily expansion data (installed, planned, needed for 2030 targets). The calculation logic is ported from the existing `_calculate_daily_capacity()` in `msr_wind.py`.

Key differences from `msr_wind.py`:
- Reads from `wind_extended` table (open-mastr schema) instead of `ee_wind`
- Column names: `Lage` (not `lage_einheit`), `EinheitBetriebsstatus` (not `betriebsstatus`), `Nettonennleistung` (not `nettonennleistung`), `Inbetriebnahmedatum` (not `datum_inbetriebnahme`), `GeplantesInbetriebnahmedatum` (not `datum_geplante_inbetriebnahme`), `datum_stilllegung` (computed by scraper)
- Status values after bulk cleansing: `"In Planung"`, `"Vorübergehend stillgelegt"` (space-separated German, not CamelCase)

- [ ] **Step 1: Create `src/msr_wind_processor.py`**

```python
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
        df["Lage"].isin(location_labels)
        & ~df["EinheitBetriebsstatus"].isin(INACTIVE_STATUSES)
    ].copy()
    active["Inbetriebnahmedatum"] = pd.to_datetime(active["Inbetriebnahmedatum"])
    active["datum_stilllegung"] = pd.to_datetime(active["datum_stilllegung"])
    active["Nettonennleistung"] = pd.to_numeric(active["Nettonennleistung"], errors="coerce")

    # Geplante Anlagen
    planned = df[
        df["Lage"].isin(location_labels)
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

    # Tägliche Zubau-/Abbau-Summen berechnen
    date_range = pd.date_range("2010-01-01", "2030-12-31", freq="D")

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

    rows = []
    cumulative_kw = base_capacity_kw
    cumulative_planned_gw = 0.0

    # Nötige Rate berechnen
    baseline_dt = pd.Timestamp(baseline_date)
    target_dt = pd.Timestamp(TARGET_DATE)
    days_to_target = (target_dt - baseline_dt).days
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

    return pd.DataFrame(rows)


def process_wind(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Berechnet die täglichen Ausbaudaten für Onshore und Offshore Wind.

    Liest aus wind_extended in mastr.db, schreibt ee_wind_taeglich.
    Gibt (df_onshore, df_offshore) zurück.
    """
    db = sqlite3.connect(db_path)
    _create_result_table(db)

    try:
        df = pd.read_sql_query("SELECT * FROM wind_extended", db)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise RuntimeError("wind_extended-Tabelle nicht gefunden in mastr.db") from e

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
    return df_onshore, df_offshore


if __name__ == "__main__":
    process_wind(Path(__file__).parent / "mastr.db")
```

- [ ] **Step 2: Verify syntax**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python -c "import ast; ast.parse(open('ddj_cloud/scrapers/klimadashboard/src/msr_wind_processor.py').read()); print('Syntax OK')"`
Expected: `Syntax OK`

- [ ] **Step 3: Run linter**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run ruff check ddj_cloud/scrapers/klimadashboard/src/msr_wind_processor.py`
Expected: No errors (fix any that appear).

- [ ] **Step 4: Commit**

```bash
git add ddj_cloud/scrapers/klimadashboard/src/msr_wind_processor.py
git commit -m "feat: add wind processor with daily expansion calculation"
```

---

### Task 4: Update klimadashboard.py with S3 integration

**Files:**
- Modify: `klimadashboard.py`

Add S3 download/upload of `mastr.db` and call the new scraper + processor.

- [ ] **Step 1: Update `klimadashboard.py`**

Replace the entire file content with:

```python
from pathlib import Path

from ddj_cloud.scrapers.klimadashboard.src.energiemix import update_energiemix
from ddj_cloud.scrapers.klimadashboard.src.msr_scraper import scrape_mastr
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


def _download_db():
    """Lädt mastr.db von S3 herunter (falls vorhanden)."""
    try:
        bio = download_file(DB_S3_KEY)
        DB_LOCAL_PATH.write_bytes(bio.read())
        print(f"  mastr.db von S3 heruntergeladen ({DB_LOCAL_PATH.stat().st_size / 1024 / 1024:.1f} MB)")
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
    print(f"  mastr.db auf S3 hochgeladen ({DB_LOCAL_PATH.stat().st_size / 1024 / 1024:.1f} MB)")


def run():
    # Energiemix (Fraunhofer API)
    df = update_energiemix()
    upload_dataframe(df, "klimadashboard/test_energiemix1.csv")

    # MaStR Wind-Ausbau
    print("MaStR Wind-Daten aktualisieren...")
    _download_db()
    scrape_mastr(DB_LOCAL_PATH)
    df_onshore, df_offshore = process_wind(DB_LOCAL_PATH)
    _upload_db()
    print("MaStR Wind-Daten aktualisiert.")
```

- [ ] **Step 2: Run linter**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run ruff check ddj_cloud/scrapers/klimadashboard/klimadashboard.py`
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add ddj_cloud/scrapers/klimadashboard/klimadashboard.py
git commit -m "feat: integrate MaStR scraper+processor with S3 download/upload"
```

---

### Task 5: Delete old msr_wind.py and update docs

**Files:**
- Delete: `src/msr_wind.py`
- Modify: `README_msr.md`
- Modify: `README.md`

- [ ] **Step 1: Delete `src/msr_wind.py`**

Run: `rm ddj_cloud/scrapers/klimadashboard/src/msr_wind.py`

- [ ] **Step 2: Update `README_msr.md`**

Replace entire content with:

```markdown
# MaStR-Scraper und Wind-Prozessor

Python-Port der PHP-Skripte `msr_php/wka_daily.php` und `msr_php/wka_to_data.php`,
jetzt basierend auf der [open-mastr](https://github.com/OpenEnergyPlatform/open-mastr)-Bibliothek.

## Architektur

```
klimadashboard.py (Orchestrator)
  │
  ├── S3: download mastr.db
  ├── msr_scraper.py  → alle Energiearten aus MaStR
  ├── msr_wind_processor.py → Wind-Tagesdaten berechnen
  ├── S3: upload mastr.db
  └── Datawrapper-Upload
```

### 1. Scraper (`src/msr_scraper.py`)

Lädt alle Energiearten (Wind, Solar, Biomasse, Wasser, Kernkraft, Verbrennung, Geothermie/Grubengas, Speicher)
über den open-mastr Bulk-Download und speichert sie in `mastr.db`.

**Kein API-Key nötig** -- nutzt die öffentlichen Bulk-Daten des MaStR.

### 2. Wind-Prozessor (`src/msr_wind_processor.py`)

Berechnet tägliche Ausbaudaten (2010-2030) für Onshore und Offshore Wind:
- Kumulierte installierte Leistung (GW)
- Täglicher Zubau/Abbau (MW)
- Geplante zukünftige Installationen
- Nötiger täglicher Ausbau für die Klimaschutzziele 2030

**Klimaziele 2030:**
- Onshore: 115 GW (Wind-an-Land-Gesetz, seit 01.02.2023)
- Offshore: 30 GW (Wind-auf-See-Gesetz, seit 01.01.2023)

## Benötigte Secrets / Umgebungsvariablen

| Variable | Beschreibung | Wo beantragen? |
|----------|-------------|----------------|
| `DATAWRAPPER_API_KEY` | API-Token für Datawrapper-Charts | [Datawrapper Account Settings](https://app.datawrapper.de/account/api-tokens) |
| `BUCKET_NAME` | S3-Bucket für mastr.db | AWS-Konfiguration |

**Nicht mehr nötig:** `MASTR_API_KEY`, `MASTR_AKTEUR_NR` (open-mastr nutzt öffentliche Bulk-Daten).

## Datenbank

Die SQLite-Datenbank `mastr.db` wird auf S3 gespeichert und bei jedem Lauf heruntergeladen/hochgeladen.

**Tabellen aus MaStR** (open-mastr-Schema):
- `wind_extended`, `solar_extended`, `biomass_extended`, `hydro_extended`,
  `combustion_extended`, `nuclear_extended`, `gsgk_extended`, `storage_extended`

**Berechnete Tabellen:**
- `ee_wind_taeglich`: Tägliche Ausbaudaten (installiert, geplant, nötig) pro Lage (onshore/offshore)

## Unterschiede zum PHP-Original

| Aspekt | PHP | Python |
|--------|-----|--------|
| Datenquelle | MaStR SOAP-API (API-Key nötig) | open-mastr Bulk-Download (kein Key) |
| Datenbank | MySQL (remote) | SQLite auf S3 |
| Datenverarbeitung | SQL-Queries pro Tag | pandas (vektorisiert) |
| Energiearten | Nur Wind | Alle (Wind, Solar, Biomasse, etc.) |
| Architektur | wka_daily.php + wka_to_data.php | msr_scraper.py + msr_wind_processor.py |

## Erweiterbarkeit

Weitere Prozessoren können hinzugefügt werden, die auf denselben Daten in `mastr.db` arbeiten:
- `msr_solar_processor.py` (Ausbauziel: 215 GW, EEG 2023)
- `msr_biomasse_processor.py`
```

- [ ] **Step 3: Update `README.md`**

In `README.md`, replace the "Wind-Ausbau" section (lines 18-25) with:

```markdown
### MaStR-Daten / Wind-Ausbau (`src/msr_scraper.py` + `src/msr_wind_processor.py`)

Holt alle Energiearten aus dem Marktstammdatenregister über [open-mastr](https://github.com/OpenEnergyPlatform/open-mastr) (Bulk-Download, kein API-Key nötig) und berechnet tägliche Wind-Ausbaudaten (installiert, geplant, nötig für Klimaziel 2030).

- Onshore-Ziel: 115 GW bis 2030 (Wind-an-Land-Gesetz)
- Offshore-Ziel: 30 GW bis 2030 (Wind-auf-See-Gesetz)

**Benötigte Umgebungsvariablen:** `DATAWRAPPER_API_KEY`

Siehe [README_msr.md](README_msr.md) für Details zur Architektur.
```

- [ ] **Step 4: Commit**

```bash
git add -A ddj_cloud/scrapers/klimadashboard/
git commit -m "refactor: remove old msr_wind.py, update documentation"
```

---

### Task 6: Add mastr.db to .gitignore

**Files:**
- Modify or create: `.gitignore` (in klimadashboard directory or project root)

The SQLite database should not be committed to git.

- [ ] **Step 1: Check existing .gitignore**

Run: `cat /Users/janeggers/Code/wdr-ddj-cloud/.gitignore 2>/dev/null || echo "no .gitignore"`

- [ ] **Step 2: Add mastr.db and open-mastr cache to .gitignore**

Add to the project's `.gitignore` (create `ddj_cloud/scrapers/klimadashboard/.gitignore` if a local one is preferred):

```
# MaStR databases
*.db
```

- [ ] **Step 3: Commit**

```bash
git add ddj_cloud/scrapers/klimadashboard/.gitignore
git commit -m "chore: add .gitignore for MaStR database files"
```

---

### Task 7: Verify end-to-end (manual smoke test)

- [ ] **Step 1: Run the scraper standalone**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python ddj_cloud/scrapers/klimadashboard/src/msr_scraper.py`

Expected: Downloads MaStR bulk data, creates `src/mastr.db` with all energy type tables. This will take a few minutes on first run.

- [ ] **Step 2: Verify database contents**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python -c "
import sqlite3
db = sqlite3.connect('ddj_cloud/scrapers/klimadashboard/src/mastr.db')
tables = [r[0] for r in db.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
print('Tables:', tables)
for t in tables:
    count = db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    print(f'  {t}: {count} rows')
db.close()
"`

Expected: Tables for wind_extended, solar_extended, etc. with thousands of rows each.

- [ ] **Step 3: Check wind Lage values**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python -c "
import sqlite3
db = sqlite3.connect('ddj_cloud/scrapers/klimadashboard/src/mastr.db')
print('Lage values:', db.execute('SELECT DISTINCT Lage FROM wind_extended').fetchall())
print('Status values:', db.execute('SELECT DISTINCT EinheitBetriebsstatus FROM wind_extended').fetchall())
db.close()
"`

Expected: Lage should include "Windkraft an Land" and "Windkraft auf See". If values differ, update `ONSHORE_LABELS` and `OFFSHORE_LABELS` in `msr_wind_processor.py`.

- [ ] **Step 4: Run the processor standalone**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python ddj_cloud/scrapers/klimadashboard/src/msr_wind_processor.py`

Expected: Reads wind data, calculates daily stats, prints count of records.

- [ ] **Step 5: Verify results**

Run: `cd /Users/janeggers/Code/wdr-ddj-cloud && uv run python -c "
import sqlite3
db = sqlite3.connect('ddj_cloud/scrapers/klimadashboard/src/mastr.db')
rows = db.execute('SELECT * FROM ee_wind_taeglich WHERE datum = \"2025-01-01\"').fetchall()
for r in rows:
    print(r)
db.close()
"`

Expected: Two rows (onshore + offshore) with plausible GW values for 2025-01-01. Onshore should be ~62-65 GW, offshore ~8-9 GW.