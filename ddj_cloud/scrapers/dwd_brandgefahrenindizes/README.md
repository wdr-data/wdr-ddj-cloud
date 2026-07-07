# DWD Waldbrand- & Graslandfeuerindex

**Contact:** Jannes Höke (mail@jhoeke.de)

Generiert Quelldaten für Datawrapper-Karten des **Waldbrandgefahrenindexes** und
des **Graslandfeuerindexes** – jeweils für NRW und bundesweit.

## Quelle

Die Vorhersage-Tabellen werden von den öffentlichen wettergefahren.de-Seiten
geparst (je eine Tabelle pro Bundesland):

- Waldbrand: <https://www.wettergefahren.de/warnungen/indizes/waldbrand.html>
- Grasland: <https://www.wettergefahren.de/warnungen/indizes/grasland.html>

Beide enthalten für jede Station den aktuellen Tag sowie die kommenden Tage
(i.d.R. fünf Tage insgesamt) mit einem Index von 1 (sehr geringe Gefahr) bis 5
(sehr hohe Gefahr). Beide Seiten teilen sich denselben Stationsbestand.

## Stationskoordinaten

Die Tabellen enthalten nur Stationsnamen, keine Koordinaten. Die Lat/Lon-Werte
werden aus [`stations.json`](stations.json) ergänzt (Join über den Stationsnamen).

`stations.json` wird **nicht zur Laufzeit** abgefragt, sondern liegt statisch im
Repo. Die Daten stammen aus einem authentifizierten WFS-Endpunkt (die Basic-Auth
stammt aus dem Frontend der DWD-Stationskarte und kann sich ändern). Bei neuen
Stationen die Datei manuell neu generieren. Der Header-Wert muss in `.env` als
`DWD_BRANDGEFAHRENINDIZES_BASIC_AUTH` stehen:

```bash
uv run ddj_cloud/scrapers/dwd_brandgefahrenindizes/update_stations.py
```

Details und ggf. ein neuer Auth-Header stehen in
[`update_stations.py`](update_stations.py).

Fehlen zur Laufzeit für bis zu zehn Stationen die Koordinaten (z. B. weil der DWD
neue Stationen aufgenommen hat), werden die Karten trotzdem hochgeladen und
Sentry per Warning benachrichtigt. Bei mehr fehlenden Stationen bricht der
Scraper ab, damit keine kaputte Karte veröffentlicht wird.

## Ausgabe

Zwei CSVs im Wide-Format, eine Zeile pro Station:

- `dwd_brandgefahrenindizes/brandgefahrenindizes_de.csv` – bundesweit
- `dwd_brandgefahrenindizes/brandgefahrenindizes_nrw.csv` – nur NRW

Beide enthalten sowohl den Waldbrand- als auch den Graslandfeuerindex. Neben den
Metadaten-Spalten `bundesland`, `stationsname`, `stationskennung`, `latitude` und
`longitude` gibt es pro Vorhersagetag (`tag0` = heute, `tag1` = morgen, …):

| Spalte              | Beschreibung                                       |
| ------------------- | -------------------------------------------------- |
| `datum_tag0`        | Datum, z. B. `07.07.2026` (Wochentag im Tooltip via `FORMAT`) |
| `wald_index_tag0`   | Waldbrandindex 1–5 (für die Einfärbung der Karte)  |
| `wald_stufe_tag0`   | Kurzbezeichnung, z. B. `mittel`                    |
| `wald_farbe_tag0`   | Hex-Farbe, z. B. `#ff8c39`                         |
| `grasland_index_tag0` | Graslandfeuerindex 1–5                           |
| `grasland_stufe_tag0` | Kurzbezeichnung, z. B. `mittel`                  |
| `grasland_farbe_tag0` | Hex-Farbe, z. B. `#ff8c39`                       |

Kurzbezeichnung und Farbe pro Stufe stammen aus
[`danger_levels.json`](danger_levels.json). Die farbigen Tooltip-Vorlagen (Wald,
Grasland, beides) liegen in [`tooltip.html`](tooltip.html).

Die Spaltennamen bleiben von Tag zu Tag stabil (die Datumswerte wandern mit),
sodass eine bestehende Datawrapper-Konfiguration nicht bricht. Für eine
Symbol-Karte färbt man typischerweise nach `wald_index_tag0` (bzw.
`grasland_index_tag0`) und referenziert die übrigen Spalten im Tooltip.
