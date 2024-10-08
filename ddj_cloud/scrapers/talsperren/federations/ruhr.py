import datetime as dt
import re
from collections.abc import Iterable

import bs4
import requests

from ddj_cloud.scrapers.talsperren.common import (
    TZ_BERLIN,
    Federation,
    ReservoirRecord,
    apply_guarded,
)


class RuhrFederation(Federation):
    name = "Ruhrverband"

    url = "https://www.talsperrenleitzentrale-ruhr.de/online-daten/talsperren/"

    reservoirs = {
        "Biggetalsperre": {
            "capacity_mio_m3": 171.70,
            "lon": 7.887853,
            "lat": 51.111176,
        },
        "Ennepetalsperre": {
            "capacity_mio_m3": 12.60,
            "lon": 7.409321,
            "lat": 51.241241,
        },
        "Fürwiggetalsperre": {
            "capacity_mio_m3": 1.67,
            "lon": 7.687793,
            "lat": 51.149712,
        },
        "Hennetalsperre": {
            "capacity_mio_m3": 38.40,
            "lon": 8.273851,
            "lat": 51.335423,
        },
        "Listertalsperre": {
            "capacity_mio_m3": 21.60,
            "lon": 7.837567,
            "lat": 51.094307,
        },
        "Möhnetalsperre": {
            "capacity_mio_m3": 134.50,
            "lon": 8.059335,
            "lat": 51.489704,
        },
        "Sorpetalsperre": {
            "capacity_mio_m3": 70.37,
            "lon": 7.968285,
            "lat": 51.350979,
        },
        "Stausee Ahausen": {
            "capacity_mio_m3": 0.84,
            "lon": 7.954430,
            "lat": 51.138287,
        },
        "Versetalsperre": {
            "capacity_mio_m3": 32.80,
            "lon": 7.685332,
            "lat": 51.193043,
        },
    }

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
            federation_name=self.name,
            name=name,
            ts_measured=ts,
            capacity_mio_m3=self.reservoirs[name]["capacity_mio_m3"],
            content_mio_m3=content_mio_m3,
        )

    def get_data(
        self,
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ReservoirRecord]:
        html = self._get_html()
        soup = bs4.BeautifulSoup(html, "lxml")

        coords_div: bs4.Tag = soup.find("div", {"id": "dam-coordinates"})  # type: ignore
        assert coords_div, "div with id `dam-coordinates` not found"

        coord_divs: bs4.ResultSet[bs4.Tag] = coords_div.find_all("div", recursive=False)

        return apply_guarded(self._parse_coord_div, coord_divs)
