# Energiemix als Demo für das Energie-Dashboard

## Anteil erneuerbare Energien

Die einfachste Grafik: Das ist ein Prozentanteil, der über die Fraunhofer-API taggenau abrufbar ist. Die Daten gehen bis zum 1.1.2015 zurück.

- Einfaches Datenauslese-Skript ✅
- Upload Datawrapper ✅
- Scraper im wdr-data Format
- Scraper im mage.ai Format
- Einbau in Quarks-Demoseite

## Energiemix

- Recherche zu den Daten
- Datenauslese-Skript
- Upload Datawrapper
- Scraperformat festlegen

## MaStR-Daten (Marktstammdatenregister)

### Scraper ✅
- open-mastr Bulk-Download aller Energiearten ✅
- Isoliertes venv via PEP 723 / uv run ✅

### Wind-Prozessor ✅
- Tägliche Ausbaudaten (onshore/offshore) ✅
- Monatliche/jährliche Zusammenfassungen ✅
- Klimaziel-Berechnung (115 GW onshore, 30 GW offshore) ✅

### Solar-Prozessor ✅
- Tägliche Ausbaudaten ✅
- Monatliche/jährliche Zusammenfassungen ✅
- Klimaziel-Berechnung (215 GW) ✅

### Offen
- Datawrapper-Charts für Wind und Solar erstellen
- Biomasse-Prozessor