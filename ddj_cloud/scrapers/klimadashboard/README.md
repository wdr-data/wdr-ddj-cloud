# klimadashboard

**Contact:** Jan Eggers (jan.eggers@fm.wdr.de)

Automation für Quarks.de: Ausbau von Wind- und Solarenergie, Energiemix in D und mehr

## Scraper-Module

### Energiemix (`src/energiemix.py`)

Holt den täglichen Anteil erneuerbarer Energien von der [Fraunhofer Energy Charts API](https://api.energy-charts.info/) und publiziert die Daten auf Datawrapper-Charts.

- **Chart "Erneuerbare-Anteil"** (`n3FOA`): Monatsmittel + Jahresdurchschnitte, 10 Jahre
- **Chart "Installierte Leistung"** (`p5sHV`): Kapazitäten nach Energieträger

**Benötigte Umgebungsvariablen:** `DATAWRAPPER_API_KEY`

### MaStR-Daten (`src/msr_scraper.py` + Prozessoren)

Holt alle Energiearten aus dem Marktstammdatenregister über [open-mastr](https://github.com/OpenEnergyPlatform/open-mastr) (Bulk-Download, kein API-Key nötig).

**Wind-Prozessor** (`src/msr_wind_processor.py`): Tägliche, monatliche und jährliche Wind-Ausbaudaten.
- Onshore-Ziel: 115 GW bis 2030 (Wind-an-Land-Gesetz)
- Offshore-Ziel: 30 GW bis 2030 (Wind-auf-See-Gesetz)

**Solar-Prozessor** (`src/msr_solar_processor.py`): Tägliche, monatliche und jährliche Solar-Ausbaudaten.
- Solar-Ziel: 215 GW bis 2030 (EEG 2023)

**Benötigte Umgebungsvariablen:** `DATAWRAPPER_API_KEY`

Siehe [README_msr.md](README_msr.md) für Details zur Architektur.

## Einstiegspunkt

`klimadashboard.py` wird vom zentralen Handler aufgerufen und orchestriert die einzelnen Module.
