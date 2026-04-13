# klimadashboard

**Contact:** Jan Eggers (jan.eggers@fm.wdr.de)

Automation für Quarks.de: Ausbau von Wind- und Solarenergie, Energiemix in D und mehr

## Architektur

```
klimadashboard.py (Orchestrator)
  │
  ├── msr_scraper.py  → alle Energiearten aus MaStR (isoliertes venv via uv run)
  ├── msr_wind_processor.py → Wind-Tagesdaten berechnen
  ├── msr_solar_processor.py → Solar-Tagesdaten berechnen
  ├── msr_dw_display.py → Datawrapper-Charts aktualisieren
  ├── S3: upload mastr.db + CSVs
  └── energiemix.py → Fraunhofer-Daten + DW-Charts
```


## MaStR-Scraper; Auswertung Wind- und Solarenergie

Ausbaustand Wind- und Solarenenergie: Wie geht es voran? Was muss passieren, um die Ziele des EEG zu erreichen?

Ursprünglich ein Python-Port der PHP-Skripte `msr_php/wka_daily.php` und `msr_php/wka_to_data.php`, jetzt basierend auf der [open-mastr](https://github.com/OpenEnergyPlatform/open-mastr)-Bibliothek des [Rainer-Lemoine-Instituts](https://wam.rl-institut.de/#showcase). Die Maintainer dort sind Jonathan Amme und Ludwig Hülk - die das mehr oder weniger nebenbei entwickeln und für Props und Kooperationen offen sind.


### 1. Scraper (`src/msr_scraper.py`)

Lädt alle Energiearten (Wind, Solar, Biomasse, Wasser, Kernkraft, Verbrennung, Geothermie/Grubengas, Speicher)
über den open-mastr Bulk-Download und speichert sie in `mastr.db`.

**Kein API-Key nötig** -- nutzt die öffentlichen Bulk-Daten des MaStR.

**Isoliertes venv:** Der Scraper nutzt PEP 723 inline script metadata und wird via `uv run`
in einem eigenen virtuellen Environment ausgeführt (open-mastr benötigt pandas>=2.2,
das Hauptprojekt nutzt pandas~=1.5).

**Caching:** Wenn `mastr.db` bereits Daten von heute enthält (`DatumDownload`), wird der Download übersprungen.

### 2. Wind-Prozessor (`src/msr_wind_processor.py`)

Berechnet tägliche Ausbaudaten (2010-2030) für Onshore und Offshore Wind:
- Kumulierte installierte Leistung (GW)
- Täglicher Zubau/Abbau (MW)
- Geplante zukünftige Installationen
- Nötiger täglicher Ausbau für die Klimaschutzziele 2030
- Monatliche und jährliche Zusammenfassungen

**Klimaziele 2030:**
- Onshore: 115 GW (Wind-an-Land-Gesetz, seit 01.02.2023)
- Offshore: 30 GW (Wind-auf-See-Gesetz, seit 01.01.2023)

### 3. Solar-Prozessor (`src/msr_solar_processor.py`)

Berechnet tägliche Ausbaudaten (2010-2030) für Solarenergie:
- Kumulierte installierte Leistung (GW)
- Täglicher Zubau/Abbau (MW)
- Geplante zukünftige Installationen
- Nötiger täglicher Ausbau für das Klimaziel 2030
- Monatliche und jährliche Zusammenfassungen

**Klimaziel 2030:** 215 GW (EEG 2023)

### 4. Datawrapper-Display (`src/msr_dw_display.py`)

Lädt aufbereitete Daten in Datawrapper-Charts hoch:
- **Wind-Ausbau** (`EgOti`): Gesamtleistung Onshore/Offshore
- **Solar-Ausbau** (`1rxLQ`): Gesamtleistung Solar
- **Wind-Zubau** (`7yMTK`): Zubau pro Monat/Jahr
- **Solar-Zubau** (`kPzGf`): Zubau pro Monat/Jahr

Umschaltbar zwischen monatlicher und jährlicher Aggregation via `YEARLY_AGGREGATES`.

## Benötigte Secrets / Umgebungsvariablen

| Variable | Beschreibung | Wo? |
|----------|-------------|-----|
| `DATAWRAPPER_API_KEY` | API-Token für Datawrapper-Charts | [Datawrapper Account Settings](https://app.datawrapper.de/account/api-tokens), in .env des Projekts |

Der Upload ins S3-Bucket erfolgt über eine Bibliotheksfunktion des Projekts; keine Extra-Keys nötig.

## Datenbank

Die SQLite-Datenbank `mastr.db` liegt in `local_storage/klimadashboard/` und wird nach Verarbeitung auf S3 hochgeladen.

**Tabellen aus MaStR** (open-mastr-Schema):
- `wind_extended`, `solar_extended`, `biomass_extended`, `hydro_extended`,
  `combustion_extended`, `nuclear_extended`, `gsgk_extended`, `storage_extended`

**Berechnete Tabellen:**
- `ee_wind_taeglich`: Tägliche Wind-Ausbaudaten pro Lage (onshore/offshore)
- `ee_solar_taeglich`: Tägliche Solar-Ausbaudaten

## Erweiterbarkeit

Weitere Prozessoren können hinzugefügt werden, die auf denselben Daten in `mastr.db` arbeiten:
- `msr_biomasse_processor.py`
- `energiemix_processor.py` (ersetzt energiemix.py mit den Fraunhofer-Daten; erzeugt aktuelle Verlaufsdaten zum Energiemix)

## Energiemix (`src/energiemix.py`)

Monolithischer Scraper, der Daten des [Fraunhofer ISE](https://www.energy-charts.info/?l=de&c=DE) über die API holt, aufarbeitet und auf zwei Datawrapper-Grafiken schiebt.

- **Chart "Erneuerbare-Anteil"** (`n3FOA`): Monatsmittel + Jahresdurchschnitte, 10 Jahre
- **Chart "Installierte Leistung"** (`p5sHV`): Kapazitäten nach Energieträger

**Weshalb nicht aus dem MaStR?** Dort findet sich die *installierte* Leistung; was aus dieser Kapazität tatsächlich herauskommt, kann man erst im Nachhinein sagen - bzw. mit Modellen und unter Zuhilfenahme anderer Quellen ergänzen. Das tut die Fraunhofer-Plattform.

## Einstiegspunkt

`klimadashboard.py` wird vom zentralen Handler aufgerufen und orchestriert die einzelnen Module.
