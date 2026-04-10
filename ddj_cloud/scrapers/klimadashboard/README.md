# klimadashboard

**Contact:** Jan Eggers (jan.eggers@fm.wdr.de)

Automation für Quarks.de: Ausbau von Wind- und Solarenergie, Energiemix in D und mehr

## Scraper-Module

### Energiemix (`src/energiemix.py`)

Holt den täglichen Anteil erneuerbarer Energien von der [Fraunhofer Energy Charts API](https://api.energy-charts.info/) und publiziert die Daten auf Datawrapper-Charts.

- **Chart "Erneuerbare-Anteil"** (`n3FOA`): Monatsmittel + Jahresdurchschnitte, 10 Jahre
- **Chart "Installierte Leistung"** (`p5sHV`): Kapazitäten nach Energieträger

**Benötigte Umgebungsvariablen:** `DATAWRAPPER_API_KEY`

### MaStR-Daten / Wind-Ausbau (`src/msr_scraper.py` + `src/msr_wind_processor.py`)

Holt alle Energiearten aus dem Marktstammdatenregister über [open-mastr](https://github.com/OpenEnergyPlatform/open-mastr) (Bulk-Download, kein API-Key nötig) und berechnet tägliche Wind-Ausbaudaten (installiert, geplant, nötig für Klimaziel 2030).

- Onshore-Ziel: 115 GW bis 2030 (Wind-an-Land-Gesetz)
- Offshore-Ziel: 30 GW bis 2030 (Wind-auf-See-Gesetz)

**Benötigte Umgebungsvariablen:** `DATAWRAPPER_API_KEY`

Siehe [README_msr.md](README_msr.md) für Details zur Architektur.

## Einstiegspunkt

`klimadashboard.py` wird vom zentralen Handler aufgerufen und orchestriert die einzelnen Module.
