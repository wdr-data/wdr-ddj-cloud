# Design: msr_wind mit open-mastr und Scraper/Prozessor-Trennung

## Zusammenfassung

Umbau von `src/msr_wind.py` auf die `open-mastr`-Bibliothek (kein API-Key mehr nötig) und Aufteilung in zwei Skripte analog zum PHP-Original (`wka_daily.php` + `wka_to_data.php`). Integration des S3-Download/Upload-Musters aus dem Talsperren-Scraper.

## Architektur

```
klimadashboard.py (Orchestrator)
  │
  ├── S3: download msr_wind.db
  ├── msr_wind_scraper.py  → befüllt wind-Tabelle in msr_wind.db
  ├── msr_wind_processor.py → berechnet ee_wind_taeglich aus wind-Daten
  ├── S3: upload msr_wind.db
  └── Datawrapper-Upload (bestehend)
```

## Dateien

| Datei | Rolle | Analog PHP |
|---|---|---|
| `src/msr_wind_scraper.py` | Daten aus MaStR holen via open-mastr | `wka_daily.php` |
| `src/msr_wind_processor.py` | Tagesdaten berechnen aus lokaler DB | `wka_to_data.php` |
| `klimadashboard.py` | Orchestrierung, S3-Integration | -- |
| `src/msr_wind.py` | Wird entfernt (ersetzt durch die zwei neuen) | -- |

## Scraper (`msr_wind_scraper.py`)

### Ablauf

1. `open_mastr.Mastr()` initialisieren (Standard-DB unter `~/.open-MaStR/`)
2. `db.download(data="wind")` -- lädt Bulk-XML-Dump vom MaStR-Portal, kein API-Key nötig
3. Wind-Einheiten aus `wind_extended`-Tabelle der open-mastr-DB lesen (via pandas/sqlite3)
4. Stilllegungsdatum berechnen: `max(DatumBeginnVoruebergehendeStilllegung, DatumEndgueltigeStilllegung)` als abgeleitete Spalte
5. Per `INSERT OR REPLACE` in unsere `msr_wind.db` mergen
6. Spaltennamen werden 1:1 aus open-mastr übernommen (kein Mapping)

### Hauptfunktion

```python
def scrape_wind(db_path: Path) -> int:
    """Lädt Wind-Einheiten via open-mastr und schreibt sie in die lokale DB.
    Gibt Anzahl der Einheiten zurück."""
```

## Prozessor (`msr_wind_processor.py`)

### Ablauf

1. Alle Wind-Einheiten aus `msr_wind.db` lesen
2. Für Onshore und Offshore jeweils:
   - Vorleistung bis Ende 2009 berechnen
   - Täglichen Zubau/Abbau 2010-2030 (kumuliert)
   - Geplante Installationen (nur Zukunft)
   - Nötigen täglichen Ausbau für Klimaziel 2030
3. Ergebnis in `ee_wind_taeglich`-Tabelle schreiben

### Klimaziele

- Onshore: 115 GW bis 2031-01-01 (Wind-an-Land-Gesetz, Baseline 2023-02-01)
- Offshore: 30 GW bis 2031-01-01 (Wind-auf-See-Gesetz, Baseline 2023-01-01)

### Anpassungen gegenüber aktuellem Code

- Filter für Lage/Status: Werte aus open-mastr-DB verwenden (bei Implementierung verifizieren)
- `ee_wind_taeglich`-Schema bleibt gleich (eigene Berechnung)
- Berechnungslogik bleibt identisch zu `_calculate_daily_capacity()`

### Hauptfunktion

```python
def process_wind(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Berechnet tägliche Ausbaudaten aus der lokalen DB.
    Gibt (df_onshore, df_offshore) zurück."""
```

## S3-Integration (`klimadashboard.py`)

### Download vor Verarbeitung

```python
from ddj_cloud.utils.storage import download_file, upload_file, DownloadFailedException

DB_S3_KEY = "klimadashboard/msr_wind.db"
DB_LOCAL_PATH = Path(__file__).parent / "src" / "msr_wind.db"

try:
    bio = download_file(DB_S3_KEY)
    DB_LOCAL_PATH.write_bytes(bio.read())
except DownloadFailedException:
    pass  # Erster Lauf -- DB wird vom Scraper erstellt
```

### Upload nach Verarbeitung

```python
upload_file(DB_LOCAL_PATH.read_bytes(), DB_S3_KEY, archive=False)
```

`archive=False` da die DB bei jedem Lauf mehrere MB groß ist.

## Dependencies

- `open-mastr` (PyPI: `open-mastr`, aktuell v0.16.1) -- neue Dependency
- Kein `MASTR_API_KEY` und `MASTR_AKTEUR_NR` mehr nötig
- `DATAWRAPPER_API_KEY` weiterhin nötig (für Chart-Upload)

## Error Handling

- Sentry-Integration wie bisher: `sentry_sdk.capture_exception(e)` bei Fehlern
- open-mastr-Download-Fehler: Scraper bricht ab, Prozessor läuft nicht
- S3-Download-Fehler beim ersten Lauf: OK, Scraper erstellt neue DB

## Migration

- `src/msr_wind.py` wird gelöscht
- `README_msr.md` wird aktualisiert (keine API-Keys mehr, neue Architektur)
- `klimadashboard.py` wird erweitert um Wind-Aufruf mit S3-Handling
- `MASTR_API_KEY` und `MASTR_AKTEUR_NR` können aus der Konfiguration entfernt werden