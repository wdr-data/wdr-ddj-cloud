"""EGLV (Emschergenossenschaft/Lippeverband) Pegelstände scraper.

Fetches current water level data from EGLV stations and returns StationRow
objects compatible with the LANUK Karte output format.

Uses a static station list with pre-computed WGS84 coordinates (converted
from UTM32N using convert_eglv_coords.py).
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests
from pydantic import BaseModel, ConfigDict, model_validator

from ddj_cloud.scrapers.lanuk_karte.common import WARNSTUFE_COLORS, StationRow
from ddj_cloud.utils.date_and_time import BERLIN, local_now

logger = logging.getLogger(__name__)

MEASUREMENTS_URL = "https://pegel.eglv.de/measurements/"
REQUEST_DELAY = 0.5  # seconds between per-station requests

CACHE_DIR = Path(__file__).parent / "cache"

# Static station list with pre-computed WGS84 coordinates.
# Source: pegelstaende-pipeline-nrw/tasks/shared-code/src/shared/stationen.py
# Coordinates converted from UTM32N (EPSG:25832) via convert_eglv_coords.py.
# fmt: off
_STATIONS: list[tuple[str, str, str, float, float]] = [
    # (station_id, pegelname,                                            gewaesser,               latitude,     longitude)
    ("10104", "Econova Allee",                                      "Berne",                    51.49522673,  6.95239405),
    ("10135", "E Posener Straße (neu)",                             "Borbecker Mühlenbach",     51.44490827,  6.95548101),
    ("11038", "HRB Borbecker MB, Ablauf, unterhalb Bergmühle",      "Borbecker Mühlenbach",     51.47420364,  6.97276621),
    ("12036", "HRB Borbecker Mühlenbach, Beckenpegel, Staubauwerk", "Borbecker Mühlenbach",     51.47291568,  6.97130671),
    ("10085", "Nöggerathstraße",                                    "Borbecker Mühlenbach",     51.45739928,  6.95630501),
    ("10140", "BOT Gungstraße, Fußgängerbrücke",                    "Boye",                     51.52441520,  6.99504012),
    ("10141", "Braukstraße, B224",                                  "Boye",                     51.52173264,  6.99154719),
    ("10139", "GLA Brücke Welheimer Straße",                        "Boye",                     51.54435172,  6.98450058),
    ("22049", "An den Höfen, Hünxe (HRB)",                          "Bruckhauser Mühlenbach",   51.59603814,  6.75088750),
    ("21119", "HÜN HRB Zur alten Mühle, Ablauf",                    "Bruckhauser Mühlenbach",   51.60272381,  6.75603937),
    ("22119", "Zur alten Mühle, HRB Beckenpegel",                   "Bruckhauser Mühlenbach",   51.61100795,  6.76642994),
    ("22101", "RRB Brüggerbach, Ulfkotter Strasse",                  "Brüggerbach",              51.60877619,  7.03438495),
    ("20132", "DAT Stemmbrückenstraße",                             "Dattelner Mühlenbach",     51.64728527,  7.31857894),
    ("22047", "HRB Dattelner Mühlenbach, Becken neu",               "Dattelner Mühlenbach",     51.64752672,  7.32511109),
    ("20032", "Natroper Weg, Brücke",                               "Dattelner Mühlenbach",     51.66807991,  7.36361295),
    ("20043", "Wiesenstraße",                                       "Dattelner Mühlenbach",     51.64738118,  7.33104373),
    ("10042", "Westring in Castrop-Rauxel Bladenhorst",             "Deininghauser Bach",       51.56785230,  7.28082595),
    ("10128", "HER Brücke Am Berg",                                 "Dorneburger Mühlenbach",   51.52944209,  7.14874268),
    ("10109", "Sonnenblumenweg, Kleingartenanlage",                  "Dorneburger Mühlenbach",   51.51814258,  7.18906362),
    ("11108", "Ablauf Phönixsee, Brücke zum Magazin",               "Emscher",                  51.49348620,  7.50354825),
    ("10107", "Adelenstraße",                                       "Emscher",                  51.48881140,  7.53080629),
    ("10119", "Adenauerallee",                                      "Emscher",                  51.54473685,  7.08282866),
    ("10124", "Am Stadthafen",                                      "Emscher",                  51.56075889,  7.20926360),
    ("10103", "Bahnstraße, Ultraschallanlage",                      "Emscher",                  51.52776470,  6.79402587),
    ("10099", "Brücke Konrad-Adenauer-Straße",                      "Emscher",                  51.55720522,  6.72044862),
    ("16137", "DIN Hagelstraße",                                    "Emscher",                  51.56129740,  6.69800971),
    ("16136", "DIN Heerstraße",                                     "Emscher",                  51.56044205,  6.70742117),
    ("10132", "DO Brücke Sölder Straße",                            "Emscher",                  51.49944368,  7.58763636),
    ("11133", "DO HRB Ellinghausen Ablauf, Brücke Gut Königsmühle", "Emscher",                  51.56345858,  7.41239093),
    ("12133", "DO HRB Ellinghausen, Brücke Ellinghauser Straße",    "Emscher",                  51.55856914,  7.41506477),
    ("10113", "Dortmund-Dorstfeld, Brücke Dorstfelder Hellweg",     "Emscher",                  51.51223452,  7.42727938),
    ("10026", "Dortmund-Mengede",                                   "Emscher",                  51.57714248,  7.36576510),
    ("10146", "DO Schweizer Allee",                                 "Emscher",                  51.49240881,  7.56259167),
    ("10145", "Emscher OL, Kirchstrasse",                           "Emscher",                  51.49954812,  7.61730580),
    ("10101", "Essener Straße",                                     "Emscher",                  51.50335731,  6.93622063),
    ("10144", "HER Wiedehopfstraße",                                "Emscher",                  51.55089279,  7.13873382),
    ("11036", "HRB Borbecker MB Ablauf, Durchlass",                 "Emscher",                  51.47404361,  6.97238620),
    ("12147", "HRB Landwehrbach Überlaufschwelle",                  "Emscher",                  51.56365009,  7.26946752),
    ("10115", "HRB Mengede, Ablaufpegel, JVA Meisenhof",            "Emscher",                  51.58846598,  7.35047810),
    ("12114", "HRB Mengede, Beckenpegel, oh. Staubauwerk",          "Emscher",                  51.58391645,  7.35602823),
    ("12111", "HRB Nagelpötchen, Beckenpegel",                      "Emscher",                  51.49481203,  7.53885993),
    ("12112", "HRB Vieselerhofstraße, Beckenpegel",                 "Emscher",                  51.49449631,  7.56992087),
    ("12108", "Phönixsee, Beckenpegel",                             "Emscher",                  51.48881447,  7.51358923),
    ("10143", "Schwarzbach, Rotthauser Straße",                     "Emscher",                  51.48072024,  7.08100001),
    ("11118", "BO Goldhammer Bach, Ablauf Blücherstraße, Brücke",   "Goldhamer Bach",           51.49522608,  7.15933451),
    ("12118", "HRB Goldhammer Bach, Beckenpegel",                   "Goldhammer Bach",          51.49457119,  7.16109487),
    ("20020", "Am Strandbad",                                       "Hammbach",                 51.67634757,  6.96576066),
    ("20017", "Rosenstraße",                                        "Hammbach",                 51.68378463,  6.96688017),
    ("20114", "Polsumer Straße",                                    "Hasseler Mühlenbach",      51.61837092,  7.04828770),
    ("21109", "HRB Kortelbach, Ablaufpegel",                        "Heerener Mühlenbach",      51.56987190,  7.71687285),
    ("22109", "HRB Kortelbach, Beckenpegel",                        "Heerener Mühlenbach",      51.56950104,  7.71784963),
    ("10122", "Feldstraße",                                         "Hellbach",                 51.56812901,  7.20400006),
    ("22081", "HRB Herringer Bach, Becken",                         "Herringer Bach",           51.65130373,  7.77519287),
    ("23083", "Oberhalb Drosselbauwerk",                            "Herringer Bach",           51.65404595,  7.76894373),
    ("24083", "Unterhalb Drosselbauwerk",                           "Herringer Bach",           51.65455944,  7.76732878),
    ("10142", "GE Holzbach, im Eichkamp",                           "Holzbach",                 51.56489371,  7.13011415),
    ("10131", "DO Brücke Hörder Hafenstraße",                       "Hörder Bach",              51.49191226,  7.50385880),
    ("10127", "GE-Brücke Reckfeldstraße",                           "Hüller Bach",              51.53885823,  7.12726327),
    ("12030", "HRB Herne-Röhlinghausen, Becken",                    "Hüller Bach",              51.50759492,  7.14799691),
    ("10030", "HRB Herne-Röhlinghausen, Zulauf",                    "Hüller Bach",              51.50516282,  7.15005903),
    ("10047", "Willy-Brandt-Allee, B 226",                          "Hüller Bach",              51.54862956,  7.11560488),
    ("21100", "HRB Do-Scharnhorst, Ablauf",                         "Körne",                    51.53761801,  7.53655199),
    ("22100", "HRB Scharnhorst, Becken",                            "Körne",                    51.53831737,  7.53022467),
    ("20100", "HRB Scharnhorst, Zulauf",                            "Körne",                    51.53761792,  7.53083336),
    ("10130", "HER Schachtstraße",                                  "Landwehrbach",             51.55823397,  7.24389990),
    ("10020", "Westring in Castrop-Rauxel, unterhalb SKU",          "Landwehrbach",             51.56540420,  7.27455510),
    ("20122", "An der Rauschenburg, (Vinnum neu)",                   "Lippe",                    51.68013052,  7.36373121),
    ("20004", "Dorsten, Borkener Straße",                           "Lippe",                    51.66866948,  6.96421062),
    ("20001", "Fusternberg",                                        "Lippe",                    51.65193444,  6.64270586),
    ("28085", "Haltern, Recklinghäuser Straße",                     "Lippe",                    51.73058136,  7.18647168),
    ("20012", "HAM Radbodstraße",                                   "Lippe",                    51.68140420,  7.77533361),
    ("20084", "Heintroper Straße, Brücke B475",                     "Lippe",                    51.66425372,  8.03402346),
    ("20002", "Krudenburg",                                         "Lippe",                    51.65039738,  6.75762107),
    ("20045", "Lippestraße, Klg. Selm-Bork",                        "Lippe",                    51.65412169,  7.44893971),
    ("23133", "Lippe Wehr Buddenburg OW",                           "Lippe",                    51.61866738,  7.47908847),
    ("24133", "Lippe Wehr Buddenburg UW",                           "Lippe",                    51.61877026,  7.47844432),
    ("23009", "Lünen-Beckinghausen / Wehr OP",                      "Lippe",                    51.61725408,  7.55891503),
    ("24009", "Lünen-Beckinghausen, Wehr UP",                       "Lippe",                    51.61709626,  7.55857220),
    ("20008", "Lünen, Graf-Adolf-Straße",                           "Lippe",                    51.61548754,  7.52123505),
    ("20041", "Rünthe, Kamener Straße, B 233",                      "Lippe",                    51.64849409,  7.64285756),
    ("26038", "Wehr Buddenburg UP Berggaten",                       "Lippe",                    51.61983282,  7.47553311),
    ("23014", "Wehr Hamm-Heessen, OP",                              "Lippe",                    51.69812634,  7.84660321),
    ("24014", "Wehr Hamm-Heessen, UP",                              "Lippe",                    51.69819247,  7.84615285),
    ("23011", "WER Wehr Stockum OW",                                "Lippe",                    51.66984349,  7.70643866),
    ("24011", "WER Wehr Stockum UW",                                "Lippe",                    51.66969355,  7.70615981),
    ("20093", "Zeche Auguste Victoria (AV)",                        "Lippe",                    51.71135456,  7.12562844),
    ("20129", "LUEN Brücke Lanstroper Strasse",                     "Lüserbach",                51.58843751,  7.55345184),
    ("20130", "UN Bruecke Dortmunder Strasse L 663",                "Massener Bach",            51.56252380,  7.65099497),
    ("12117", "HRB Oespeler Bach, Beckenpegel",                     "Oespeler Bach",            51.50485425,  7.38193005),
    ("22103", "RRB Pawigbach",                                      "Pawigbach",                51.59621390,  7.03883479),
    ("22105", "HRB 1, Pelkumer Bach, Zulauf Pumpwerk",              "Pelkumer Bach",            51.64231336,  7.71260322),
    ("20106", "Verbindungsweg HRB 2 und HRB 3",                     "Pelkumer Bach",            51.64396076,  7.71923615),
    ("20107", "Zulauf HRB III Pelkumer Bach",                       "Pelkumer Bach",            51.64298566,  7.72341371),
    ("22102", "HRB Picksmühlenbach, Pawikerstr.",                    "Picksmühlenbach",          51.59493897,  7.03969986),
    ("20131", "DORS PW Erdbach Polsumer Weg",                       "Rapphofs Mühlenbach",      51.64688558,  7.00309526),
    ("21018", "HRB Rapphofs Mühlenbach, Ablauf",                    "Rapphofs Mühlenbach",      51.62207096,  7.02970087),
    ("22018", "HRB Rapphofs Mühlenbach, Becken",                    "Rapphofs Mühlenbach",      51.62295577,  7.03123456),
    ("20018", "HRB Rapphofs Mühlenbach, Zulauf",                    "Rapphofs Mühlenbach",      51.62213402,  7.03266140),
    ("20118", "Pumpwerk Galgenbach",                                "Rapphofs Mühlenbach",      51.65873269,  6.98230075),
    ("10120", "Brücke Huckarder Straße",                            "Roßbach",                  51.53099917,  7.41911843),
    ("31004", "Kirchstraße, Ablauf Rotbachsee uh. Mühle",           "Rotbach",                  51.56601377,  6.77523484),
    ("32004", "Rotbachsee, Beckenpegel",                            "Rotbach",                  51.56825382,  6.78145212),
    ("30004", "Schlägerheide, Zulauf Rotbachsee",                   "Rotbach",                  51.58084599,  6.82645848),
    ("31005", "Zum Freibad, uh. Staubauwerk",                       "Rotbach",                  51.56728796,  6.77801741),
    ("16021", "Ostenbergstraße",                                    "Rüpingsbach",              51.48558437,  7.42754050),
    ("11116", "HRB Schmechtingsbach, Ablaufpegel",                  "Schmechtingsbach",         51.50871695,  7.37463674),
    ("12116", "HRB Schmechtingsbach, Beckenpegel",                  "Schmechtingsbach",         51.50785968,  7.37507074),
    ("20123", "DORS Brücke Gelsenkirchener Straße",                 "Schölsbach",               51.64759866,  6.97249362),
    ("10125", "GE Brücke Aldenhofstraße",                           "Schwarzbach",              51.51334086,  7.05026599),
    ("10126", "GE Schwarzmühlenstraße",                             "Schwarzbach",              51.50241548,  7.07975252),
    ("20022", "Brücke Preußenstraße",                               "Seseke",                   51.60253080,  7.55098354),
    ("21113", "HRB Bönen, Ablaufpegel, Mündung Rexebach",           "Seseke",                   51.58542836,  7.73807930),
    ("22113", "HRB Bönen, Beckenpegel",                             "Seseke",                   51.58687748,  7.75063988),
    ("20113", "HRB Bönen, Zulaufpegel",                             "Seseke",                   51.58006122,  7.77109719),
    ("20021", "Sesekedamm, Brücke Ostenallee",                      "Seseke",                   51.59103133,  7.66872948),
    ("20085", "Zufahrt zur Kläranlage Kamen, Lünener Straße",       "Seseke",                   51.58728724,  7.64224289),
    ("20115", "Hullerner Damm, Tennisplätze",                       "Stever",                   51.73971962,  7.20018916),
    ("20116", "Oberhalb Kanaldüker",                                "Weierbach",                51.67883405,  7.05244311),
    ("20117", "Durchlass Luner Weg",                                "Wienbach",                 51.67959095,  6.97565598),
]
# fmt: on


# -- Pydantic models --


class EGLVThreshold(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    value: float


class EGLVResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    gauge_measurements: list[tuple[str, float]]
    thresholds: list[EGLVThreshold] = []

    @model_validator(mode="after")
    def check_measurements_not_empty(self) -> "EGLVResponse":
        if not self.gauge_measurements:
            msg = "gauge_measurements is empty"
            raise ValueError(msg)
        return self

    @property
    def latest_measurement(self) -> tuple[datetime, float]:
        ts_str, value = self.gauge_measurements[-1]
        return datetime.fromisoformat(ts_str), value

    @property
    def thresholds_by_name(self) -> dict[str, float]:
        return {t.name: t.value for t in self.thresholds}


# -- Fetcher --


def _fetch_station(session: requests.Session, station_id: str) -> tuple[EGLVResponse, bool]:
    """Fetch and validate the EGLV API response for one station.

    Returns (parsed_response, from_cache).
    Raises on HTTP error or validation failure.
    """
    cache_filename = f"eglv_{station_id}.json"
    cached = CACHE_DIR / cache_filename

    if cached.exists():
        logger.info("Using cached %s", cache_filename)
        raw = json.loads(cached.read_text())
        from_cache = True
    else:
        response = session.get(
            MEASUREMENTS_URL,
            params={"serial": station_id, "unit_name": "Wasserstand"},
            timeout=30,
            verify=False,
        )
        response.raise_for_status()
        raw = response.json()

        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cached.write_text(json.dumps(raw))
        except Exception:
            pass  # read-only on Lambda

        from_cache = False

    return EGLVResponse.model_validate(raw), from_cache


def run(session: requests.Session) -> list[StationRow]:
    now = local_now()
    rows: list[StationRow] = []

    for station_id, pegelname, gewaesser, lat, lon in _STATIONS:
        try:
            station_data, from_cache = _fetch_station(session, station_id)
        except Exception:
            logger.exception("Failed to fetch EGLV water level for %s (%s)", pegelname, station_id)
            continue

        thresholds = station_data.thresholds_by_name
        timestamp, value = station_data.latest_measurement
        timestamp = timestamp.astimezone(BERLIN)  # Normalize to Berlin time

        if not thresholds:
            logger.warning("No thresholds for %s (%s), skipping", pegelname, station_id)
            continue

        mnw = thresholds.get("MNW")
        mw = thresholds.get("MW")
        mhw = thresholds.get("MHW")

        if mw is None:
            logger.warning("No MW threshold for %s (%s), skipping", pegelname, station_id)
            continue

        warnstufe = 1 if value <= mw else 2

        display_wasserstand = f"{value:.0f} cm"
        display_messzeitpunkt = timestamp.strftime("%d.%m.%Y, %H:%M Uhr")

        stats_parts = []
        if mnw is not None:
            stats_parts.append(f"MNW: {mnw:.0f}")
        if mw is not None:
            stats_parts.append(f"MW: {mw:.0f}")
        if mhw is not None:
            stats_parts.append(f"MHW: {mhw:.0f}")
        display_stats = (" · ".join(stats_parts) + " cm") if stats_parts else ""

        rows.append(
            StationRow(
                station_id=station_id,
                station_name=pegelname,
                gewaesser=gewaesser,
                station_type="Gewässerkundlicher Pegel",  # TODO: check if EGLV has "Infopegel"
                latitude=lat,
                longitude=lon,
                wasserstand_cm=value,
                messzeitpunkt=timestamp,
                info_1=None,
                info_2=None,
                info_3=None,
                mhw=mhw,
                mnw=mnw,
                mw=mw,
                warnstufe=warnstufe,
                warnstufe_color=WARNSTUFE_COLORS[warnstufe],
                url_pegel=f"https://pegel.eglv.de/Zeitreihe/{station_id}/",
                abrufdatum=now,
                quelle="EGLV",
                display_wasserstand=display_wasserstand,
                display_messzeitpunkt=display_messzeitpunkt,
                display_info="Keine Informationswerte vorhanden",
                display_stats=display_stats,
            )
        )

        if not from_cache:
            time.sleep(REQUEST_DELAY)

    logger.info("Fetched %d EGLV stations", len(rows))
    return rows
