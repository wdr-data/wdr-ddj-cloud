# Design: MaStR-Daten mit open-mastr und Scraper/Prozessor-Trennung

## Zusammenfassung

Umbau von `src/msr_wind.py` auf die `open-mastr`-Bibliothek (kein API-Key mehr nötig) und Aufteilung in zwei Skripte analog zum PHP-Original (`wka_daily.php` + `wka_to_data.php`). Integration des S3-Download/Upload-Musters aus dem Talsperren-Scraper. Alle Energiearten werden heruntergeladen und in `mastr.db` gespeichert.

## Architektur

```
klimadashboard.py (Orchestrator)
  │
  ├── S3: download mastr.db
  ├── msr_scraper.py  → befüllt alle Energiearten-Tabellen in mastr.db
  ├── msr_wind_processor.py → berechnet ee_wind_taeglich aus wind-Daten
  ├── S3: upload mastr.db
  └── Datawrapper-Upload (bestehend)
```

## Dateien

| Datei | Rolle | Analog PHP |
|---|---|---|
| `src/msr_scraper.py` | Alle Energiearten aus MaStR holen via open-mastr | `wka_daily.php` |
| `src/msr_wind_processor.py` | Wind-Tagesdaten berechnen aus lokaler DB | `wka_to_data.php` |
| `klimadashboard.py` | Orchestrierung, S3-Integration | -- |
| `src/msr_wind.py` | Wird entfernt (ersetzt durch die zwei neuen) | -- |

## Scraper (`msr_scraper.py`)

### Ablauf

1. `open_mastr.Mastr()` initialisieren (Standard-DB unter `~/.open-MaStR/`)
2. `db.download()` -- lädt Bulk-XML-Dumps aller Energiearten vom MaStR-Portal, kein API-Key nötig
3. Einheiten aus allen `*_extended`-Tabellen der open-mastr-DB lesen (via pandas/sqlite3)
4. Stilllegungsdatum berechnen: `max(DatumBeginnVoruebergehendeStilllegung, DatumEndgueltigeStilllegung)` als abgeleitete Spalte (wo vorhanden)
5. Per `INSERT OR REPLACE` in unsere `mastr.db` mergen -- eine Tabelle pro Energieart
6. Spaltennamen werden 1:1 aus open-mastr übernommen (kein Mapping)

### Energiearten

Alle von open-mastr unterstützten Technologien:
- Wind (`wind_extended`)
- Solar (`solar_extended`)
- Biomasse (`biomass_extended`)
- Wasser (`hydro_extended`)
- Kernkraft (`nuclear_extended`)
- Speicher (`storage_extended`)
- Geothermie, Verbrennung, etc.

### Hauptfunktion

```python
def scrape_mastr(db_path: Path) -> dict[str, int]:
    """Lädt alle Energiearten via open-mastr und schreibt sie in die lokale DB.
    Gibt dict mit Anzahl der Einheiten pro Energieart zurück."""
```

## Prozessor (`msr_wind_processor.py`)

### Ablauf

1. Wind-Einheiten aus `wind_extended`-Tabelle in `mastr.db` lesen
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

DB_S3_KEY = "klimadashboard/mastr.db"
DB_LOCAL_PATH = Path(__file__).parent / "src" / "mastr.db"

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

## Erweiterbarkeit

Die `mastr.db` enthält alle Energiearten. Weitere Prozessoren (z.B. `msr_solar_processor.py`, `msr_biomasse_processor.py`) können später hinzugefügt werden und arbeiten auf denselben Daten -- ohne erneuten Download.