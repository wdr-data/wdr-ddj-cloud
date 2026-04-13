# MaStR-Scraper und Wind-Prozessor

Python-Port der PHP-Skripte `msr_php/wka_daily.php` und `msr_php/wka_to_data.php`,
jetzt basierend auf der [open-mastr](https://github.com/OpenEnergyPlatform/open-mastr)-Bibliothek.

## Architektur

```
klimadashboard.py (Orchestrator)
  │
  ├── S3: download mastr.db
  ├── msr_scraper.py  → alle Energiearten aus MaStR (isoliertes venv via uv run)
  ├── msr_wind_processor.py → Wind-Tagesdaten berechnen
  ├── S3: upload mastr.db
  └── Datawrapper-Upload
```

### 1. Scraper (`src/msr_scraper.py`)

Lädt alle Energiearten (Wind, Solar, Biomasse, Wasser, Kernkraft, Verbrennung, Geothermie/Grubengas, Speicher)
über den open-mastr Bulk-Download und speichert sie in `mastr.db`.

**Kein API-Key nötig** -- nutzt die öffentlichen Bulk-Daten des MaStR.

**Isoliertes venv:** Der Scraper nutzt PEP 723 inline script metadata und wird via `uv run`
in einem eigenen virtuellen Environment ausgeführt (open-mastr benötigt pandas>=2.2,
das Hauptprojekt nutzt pandas~=1.5).

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
# - `msr_biomasse_processor.py`
- `energiemix_processor.py` (ersetzt energiemix.py mit den Fraunhofer-Daten; erzeugt aktuelle Verlaufsdaten zum Energiemix)