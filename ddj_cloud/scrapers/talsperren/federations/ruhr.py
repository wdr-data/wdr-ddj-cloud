from typing import Iterable
import bs4
import datetime as dt
import requests
import re

from ..common import ReservoirRecord, Federation, TZ_BERLIN, skip_errors_and_none


class RuhrFederation(Federation):
    name = "Ruhrverband"

    url = "https://www.talsperrenleitzentrale-ruhr.de/online-daten/talsperren/"

    reservoirs = {
        "Biggetalsperre": {
            "capacity": 171.70,
        },
        "Ennepetalsperre": {
            "capacity": 12.60,
        },
        "Fürwiggetalsperre": {
            "capacity": 1.67,
        },
        "Hennetalsperre": {
            "capacity": 38.40,
        },
        "Listertalsperre": {
            "capacity": 21.60,
        },
        "Möhnetalsperre": {
            "capacity": 134.50,
        },
        "Sorpetalsperre": {
            "capacity": 70.37,
        },
        "Stausee Ahausen": {
            "capacity": 0.84,
        },
        "Versetalsperre": {
            "capacity": 32.80,
        },
    }

    def __init__(self) -> None:
        super().__init__()

    def _get_html(self) -> str:
        return requests.get(self.url).text

    def _parse_coord_div(self, div: bs4.Tag) -> ReservoirRecord:
        name: str = div["title"]  # type: ignore
        assert name, "No title found"
        assert name in self.reservoirs, f"Unknown reservoir name: {name}"

        match_content = re.search(r"Stauinhalt: ([\d\.]+) Mio. ?m³", div.text)
        assert match_content, "No match found for content"
        content_mio_m3 = float(match_content.group(1))

        match_ts: list[str] = re.findall(r"\d\d\.\d\d\.\d\d\d\d um \d\d:\d\d Uhr", div.text)
        assert match_ts, "No match found for timestamp"
        ts = dt.datetime.strptime(match_ts[1], "%d.%m.%Y um %H:%M Uhr").replace(tzinfo=TZ_BERLIN)

        return ReservoirRecord(
            self.name,
            name,
            ts,
            self.reservoirs[name]["capacity"],
            content_mio_m3,
        )

    def get_data(self, **kwargs) -> Iterable[ReservoirRecord]:
        html = self._get_html()
        soup = bs4.BeautifulSoup(html, "lxml")

        coords_div: bs4.Tag = soup.find("div", {"id": "dam-coordinates"})  # type: ignore
        assert coords_div, "div with id `dam-coordinates` not found"

        coord_divs: bs4.ResultSet[bs4.Tag] = coords_div.find_all("div", recursive=False)  # type: ignore

        return skip_errors_and_none(self._parse_coord_div, coord_divs)
