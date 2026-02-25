#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyproj"]
# ///
"""One-shot script to convert EGLV station UTM32N (EPSG:25832) coordinates to
WGS84 lat/lon (EPSG:4326).

Run with:
    uv run ddj_cloud/scrapers/lanuk_karte/convert_eglv_coords.py
"""

from pyproj import Transformer

# Source: pegelstaende-pipeline-nrw/tasks/shared-code/src/shared/stationen.py
# fmt: off
STATIONS = [
    ("10104", "Econova Allee",                                     "Berne",                    357861.99,          5706886.249999999),
    ("10135", "E Posener Straße (neu)",                            "Borbecker Mühlenbach",     357919.9999999999,  5701285.0),
    ("11038", "HRB Borbecker MB, Ablauf, unterhalb Bergmühle",     "Borbecker Mühlenbach",     359211.28,          5704509.169999998),
    ("12036", "HRB Borbecker Mühlenbach, Beckenpegel, Staubauwerk","Borbecker Mühlenbach",     359105.96,          5704368.76),
    ("10085", "Nöggerathstraße",                                   "Borbecker Mühlenbach",     358016.02,          5702672.36),
    ("10140", "BOT Gungstraße, Fußgängerbrücke",                   "Boye",                     360910.9999999999,  5710049.999999998),
    ("10141", "Braukstraße, B224",                                 "Boye",                     360660.5200000001,  5709758.349999999),
    ("10139", "GLA Brücke Welheimer Straße",                       "Boye",                     360241.0,           5712287.0),
    ("22049", "An den Höfen, Hünxe (HRB)",                         "Bruckhauser Mühlenbach",   344220.38,          5718506.55),
    ("21119", "HÜN HRB Zur alten Mühle, Ablauf",                   "Bruckhauser Mühlenbach",   344600.0,           5719239.0),
    ("22119", "Zur alten Mühle, HRB Beckenpegel",                  "Bruckhauser Mühlenbach",   345347.65,          5720138.12),
    ("22101", "RRB Brüggerbach, Ulfkotter Strasse",                 "Brüggerbach",              363892.39999999997, 5719356.859999999),
    ("20132", "DAT Stemmbrückenstraße",                            "Dattelner Mühlenbach",     383668.36721078656, 5723148.232334573),
    ("22047", "HRB Dattelner Mühlenbach, Becken neu",              "Dattelner Mühlenbach",     384120.89,          5723164.7),
    ("20032", "Natroper Weg, Brücke",                              "Dattelner Mühlenbach",     386835.73,          5725389.96),
    ("20043", "Wiesenstraße",                                      "Dattelner Mühlenbach",     384530.9499999999,  5723139.119999998),
    ("10042", "Westring in Castrop-Rauxel Bladenhorst",            "Deininghauser Bach",       380848.75,          5714375.769999998),
    ("10128", "HER Brücke Am Berg",                                "Dorneburger Mühlenbach",   371587.0,           5710328.0),
    ("10109", "Sonnenblumenweg, Kleingartenanlage",                 "Dorneburger Mühlenbach",   374352.55999999994, 5709001.48),
    ("11108", "Ablauf Phönixsee, Brücke zum Magazin",              "Emscher",                  396115.02,          5705766.469999999),
    ("10107", "Adelenstraße",                                      "Emscher",                  397996.77,          5705208.28),
    ("10119", "Adenauerallee",                                     "Emscher",                  367059.80999999994, 5712146.53),
    ("10124", "Am Stadthafen",                                     "Emscher",                  375870.0,           5713706.000000001),
    ("10103", "Bahnstraße, Ultraschallanlage",                     "Emscher",                  346978.85999999987, 5710823.759999998),
    ("10099", "Brücke Konrad-Adenauer-Straße",                     "Emscher",                  341977.69,          5714253.850000001),
    ("16137", "DIN Hagelstraße",                                   "Emscher",                  340436.70306172385, 5714757.607140264),
    ("16136", "DIN Heerstraße",                                    "Emscher",                  341086.0,           5714641.999999998),
    ("10132", "DO Brücke Sölder Straße",                           "Emscher",                  401965.0,           5706313.0),
    ("11133", "DO HRB Ellinghausen Ablauf, Brücke Gut Königsmühle","Emscher",                  389956.0,           5713680.999999999),
    ("12133", "DO HRB Ellinghausen, Brücke Ellinghauser Straße",   "Emscher",                  390129.54061080853, 5713133.250735616),
    ("10113", "Dortmund-Dorstfeld, Brücke Dorstfelder Hellweg",    "Emscher",                  390865.46,          5707962.350000001),
    ("10026", "Dortmund-Mengede",                                  "Emscher",                  386758.3699999999,  5715273.8999999985),
    ("10146", "DO Schweizer Allee",                                "Emscher",                  400211.3164261627,  5705564.523639612),
    ("10145", "Emscher OL, Kirchstrasse",                          "Emscher",                  404024.5439825374,  5706285.296560735),
    ("10101", "Essener Straße",                                    "Emscher",                  356764.8899999999,  5707821.88),
    ("10144", "HER Wiedehopfstraße",                               "Emscher",                  370953.4858049339,  5712730.950931001),
    ("11036", "HRB Borbecker MB Ablauf, Durchlass",                "Emscher",                  359184.3979533644,  5704492.1055620015),
    ("12147", "HRB Landwehrbach Überlaufschwelle",                 "Emscher",                  380050.52269529435, 5713927.040626269),
    ("10115", "HRB Mengede, Ablaufpegel, JVA Meisenhof",           "Emscher",                  385727.56,          5716556.91),
    ("12114", "HRB Mengede, Beckenpegel, oh. Staubauwerk",         "Emscher",                  386100.65,          5716042.319999999),
    ("12111", "HRB Nagelpötchen, Beckenpegel",                     "Emscher",                  398569.20999999996, 5705864.390000001),
    ("12112", "HRB Vieselerhofstraße, Beckenpegel",                "Emscher",                  400724.64,          5705786.699999999),
    ("12108", "Phönixsee, Beckenpegel",                            "Emscher",                  396801.49000000005, 5705232.749999998),
    ("10143", "Schwarzbach, Rotthauser Straße",                    "Emscher",                  366746.3240354175,  5705031.236388591),
    ("11118", "BO Goldhammer Bach, Ablauf Blücherstraße, Brücke",  "Goldhamer Bach",           372225.93828821136, 5706504.61852393),
    ("12118", "HRB Goldhammer Bach, Beckenpegel",                  "Goldhammer Bach",          372346.29840974894, 5706428.723022182),
    ("20020", "Am Strandbad",                                      "Hammbach",                 359350.2899999999,  5727000.839999999),
    ("20017", "Rosenstraße",                                       "Hammbach",                 359450.72,          5727825.689999999),
    ("20114", "Polsumer Straße",                                   "Hasseler Mühlenbach",      364883.4999999999,  5720398.0),
    ("21109", "HRB Kortelbach, Ablaufpegel",                       "Heerener Mühlenbach",      411072.57000000007, 5713980.000000001),
    ("22109", "HRB Kortelbach, Beckenpegel",                       "Heerener Mühlenbach",      411139.53999999986, 5713937.569999999),
    ("10122", "Feldstraße",                                        "Hellbach",                 375525.3,           5714534.519999998),
    ("22081", "HRB Herringer Bach, Becken",                        "Herringer Bach",           415266.07,          5722966.699999998),
    ("23083", "Oberhalb Drosselbauwerk",                           "Herringer Bach",           414838.9,           5723278.929999998),
    ("24083", "Unterhalb Drosselbauwerk",                          "Herringer Bach",           414728.1499999999,  5723337.92),
    ("10142", "GE Holzbach, im Eichkamp",                          "Holzbach",                 370395.72021806386, 5714303.111568924),
    ("10131", "DO Brücke Hörder Hafenstraße",                      "Hörder Bach",              396132.9999999998,  5705590.999999999),
    ("10127", "GE-Brücke Reckfeldstraße",                          "Hüller Bach",              370124.0,           5711413.0),
    ("12030", "HRB Herne-Röhlinghausen, Becken",                   "Hüller Bach",              371473.7799999999,  5707899.889999997),
    ("10030", "HRB Herne-Röhlinghausen, Zulauf",                   "Hüller Bach",              371610.04,          5707625.82),
    ("10047", "Willy-Brandt-Allee, B 226",                         "Hüller Bach",              369343.54,          5712520.34),
    ("21100", "HRB Do-Scharnhorst, Ablauf",                        "Körne",                    398504.19999999995, 5710627.85),
    ("22100", "HRB Scharnhorst, Becken",                           "Körne",                    398066.95999999985, 5710714.419999999),
    ("20100", "HRB Scharnhorst, Zulauf",                           "Körne",                    398107.6099999999,  5710635.789999998),
    ("10130", "HER Schachtstraße",                                 "Landwehrbach",             378264.0,           5713367.0),
    ("10020", "Westring in Castrop-Rauxel, unterhalb SKU",         "Landwehrbach",             380407.74,          5714113.77),
    ("20122", "An der Rauschenburg, (Vinnum neu)",                  "Lippe",                    386873.94,          5726729.88),
    ("20004", "Dorsten, Borkener Straße",                          "Lippe",                    359219.31999999995, 5726150.019999999),
    ("20001", "Fusternberg",                                       "Lippe",                    336928.95000000007, 5724958.03),
    ("28085", "Haltern, Recklinghäuser Straße",                    "Lippe",                    374759.1318748852,  5732629.757913659),
    ("20012", "HAM Radbodstraße",                                  "Lippe",                    415331.94,          5726314.029999999),
    ("20084", "Heintroper Straße, Brücke B475",                    "Lippe",                    433190.95000000007, 5724138.4),
    ("20002", "Krudenburg",                                        "Lippe",                    344872.26,          5724536.759999999),
    ("20045", "Lippestraße, Klg. Selm-Bork",                       "Lippe",                    392703.18999999994, 5723708.929999998),
    ("23133", "Lippe Wehr Buddenburg OW",                          "Lippe",                    394706.5777961477,  5719722.306349612),
    ("24133", "Lippe Wehr Buddenburg UW",                          "Lippe",                    394662.2238021295,  5719734.675265462),
    ("23009", "Lünen-Beckinghausen / Wehr OP",                     "Lippe",                    400229.63568870473, 5719453.138219494),
    ("24009", "Lünen-Beckinghausen, Wehr UP",                      "Lippe",                    400205.5553573105,  5719436.055642975),
    ("20008", "Lünen, Graf-Adolf-Straße",                          "Lippe",                    397617.1,           5719308.8),
    ("20041", "Rünthe, Kamener Straße, B 233",                     "Lippe",                    406105.4899999999,  5722816.039999999),
    ("26038", "Wehr Buddenburg UP Berggaten",                      "Lippe",                    394463.15610118903, 5719857.039883797),
    ("23014", "Wehr Hamm-Heessen, OP",                             "Lippe",                    420288.4291751777,  5728093.493597983),
    ("24014", "Wehr Hamm-Heessen, UP",                             "Lippe",                    420257.4213122062,  5728101.340440176),
    ("23011", "WER Wehr Stockum OW",                               "Lippe",                    410546.25163656136, 5725110.494987852),
    ("24011", "WER Wehr Stockum UW",                               "Lippe",                    410526.6732316412,  5725094.162009991),
    ("20093", "Zeche Auguste Victoria (AV)",                       "Lippe",                    370502.76,          5730597.86),
    ("20129", "LUEN Brücke Lanstroper Strasse",                    "Lüserbach",                399788.0,           5716256.0),
    ("20130", "UN Bruecke Dortmunder Strasse L 663",               "Massener Bach",            406492.0,           5713245.0),
    ("12117", "HRB Oespeler Bach, Beckenpegel",                    "Oespeler Bach",            387700.61000000004, 5707210.24),
    ("22103", "RRB Pawigbach",                                     "Pawigbach",                364163.01,          5717951.65),
    ("22105", "HRB 1, Pelkumer Bach, Zulauf Pumpwerk",             "Pelkumer Bach",            410918.5699999999,  5722041.34),
    ("20106", "Verbindungsweg HRB 2 und HRB 3",                    "Pelkumer Bach",            411380.72999999986, 5722216.479999999),
    ("20107", "Zulauf HRB III Pelkumer Bach",                      "Pelkumer Bach",            411667.88,          5722102.9799999995),
    ("22102", "HRB Picksmühlenbach, Pawikerstr.",                   "Picksmühlenbach",          364219.12,          5717808.27),
    ("20131", "DORS PW Erdbach Polsumer Weg",                      "Rapphofs Mühlenbach",      361841.8077978798,  5723653.376967198),
    ("21018", "HRB Rapphofs Mühlenbach, Ablauf",                   "Rapphofs Mühlenbach",      363607.94999999995, 5720843.979999998),
    ("22018", "HRB Rapphofs Mühlenbach, Becken",                   "Rapphofs Mühlenbach",      363716.7599999998,  5720939.509999999),
    ("20018", "HRB Rapphofs Mühlenbach, Zulauf",                   "Rapphofs Mühlenbach",      363813.0599999999,  5720845.469999998),
    ("20118", "Pumpwerk Galgenbach",                               "Rapphofs Mühlenbach",      360439.6500000001,  5725010.32),
    ("10120", "Brücke Huckarder Straße",                           "Roßbach",                  390344.26999999996, 5710061.259999999),
    ("31004", "Kirchstraße, Ablauf Rotbachsee uh. Mühle",          "Rotbach",                  345804.93,          5715116.35),
    ("32004", "Rotbachsee, Beckenpegel",                           "Rotbach",                  346243.35,          5715352.34),
    ("30004", "Schlägerheide, Zulauf Rotbachsee",                  "Rotbach",                  349403.8099999999,  5716658.86),
    ("31005", "Zum Freibad, uh. Staubauwerk",                      "Rotbach",                  346002.06999999995, 5715252.17),
    ("16021", "Ostenbergstraße",                                   "Rüpingsbach",              390819.90999999986, 5704998.359999999),
    ("11116", "HRB Schmechtingsbach, Ablaufpegel",                 "Schmechtingsbach",         387204.0,           5707650.999999998),
    ("12116", "HRB Schmechtingsbach, Beckenpegel",                 "Schmechtingsbach",         387231.9999999998,  5707554.999999997),
    ("20123", "DORS Brücke Gelsenkirchener Straße",                "Schölsbach",               359727.0000000001,  5723791.0),
    ("10125", "GE Brücke Aldenhofstraße",                          "Schwarzbach",              364709.0,           5708715.0),
    ("10126", "GE Schwarzmühlenstraße",                            "Schwarzbach",              366723.0,           5707446.0),
    ("20022", "Brücke Preußenstraße",                              "Seseke",                   399648.0800000001,  5717826.659999999),
    ("21113", "HRB Bönen, Ablaufpegel, Mündung Rexebach",          "Seseke",                   412572.09,          5715684.44),
    ("22113", "HRB Bönen, Beckenpegel",                            "Seseke",                   413445.03,          5715830.65),
    ("20113", "HRB Bönen, Zulaufpegel",                            "Seseke",                   414849.51,          5715048.599999999),
    ("20021", "Sesekedamm, Brücke Ostenallee",                     "Seseke",                   407778.96,          5716392.74),
    ("20085", "Zufahrt zur Kläranlage Kamen, Lünener Straße",      "Seseke",                   405936.5,           5716010.11),
    ("20115", "Hullerner Damm, Tennisplätze",                      "Stever",                   375731.4499999999,  5733622.52),
    ("20116", "Oberhalb Kanaldüker",                               "Weierbach",                365350.45000000007, 5727113.91),
    ("20117", "Durchlass Luner Weg",                               "Wienbach",                 360044.4,           5727342.489999999),
]
# fmt: on

def main():
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)

    print("    # id         pegelname                                              gewaesser                   latitude        longitude")
    for station_id, pegelname, gewaesser, rechtswert, hochwert in STATIONS:
        lon, lat = transformer.transform(rechtswert, hochwert)
        print(
            f'    ("{station_id}", {lat:.8f}, {lon:.8f}),  # {pegelname} / {gewaesser}'
        )


if __name__ == "__main__":
    main()
