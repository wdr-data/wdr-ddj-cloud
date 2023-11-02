import re
import datetime as dt
from typing import Iterable, Optional

import requests
import bs4

from ..common import ReservoirRecord, Federation, TZ_BERLIN, skip_errors_and_none


class WupperFederation(Federation):
    name = "Wupperverband"

    url = "https://hochwasserportal.wupperverband.de/Talsperren/"

    reservoirs = {
        "Bever-Talsperre": {
            "capacity": 23.75,
        },
        "Brucher-Talsperre": {
            "capacity": 3.37,
        },
        "Eschbachtalsperre": {
            "capacity": 1.05,
        },
        "Große Dhünn-Talsperre": {
            "capacity": 81.00,
        },
        "Vorsperre Große Dhünn": {
            "capacity": 7.37,
        },
        "Herbringhauser Talsperre": {
            "capacity": 2.90,
        },
        "Kerspe-Talsperre": {
            "capacity": 14.90,
        },
        "Lingese-Talsperre": {
            "capacity": 2.60,
        },
        "Neyetalsperre": {
            "capacity": 5.98,
        },
        "Panzer-Talsperre": {
            "capacity": 0.19,
        },
        "Ronsdorfer Talsperre": {
            "capacity": 0.12,
        },
        "Schevelinger-Talsperre": {
            "capacity": 0.31,
        },
        "Wupper-Talsperre": {
            "capacity": 25.60,
        },
        "Stausee Beyenburg": {
            "capacity": 0.47,
        },
    }

    renames = {
        "Bever-Talsperre": "Bevertalsperre",
        "Brucher-Talsperre": "Bruchertalsperre",
        "Große Dhünn-Talsperre": "Große Dhünntalsperre",
        "Kerspe-Talsperre": "Kerspetalsperre",
        "Lingese-Talsperre": "Lingesetalsperre",
        "Neyetalsperre": "Neye-Talsperre",
        "Panzer-Talsperre": "Panzer-Talsperre",
        "Schevelinger-Talsperre": "Schevelinger Talsperre",
        "Wupper-Talsperre": "Wuppertalsperre",
    }

    ignore_reservoirs = [
        "Stauanlage Dahlhausen",
    ]

    def __init__(self) -> None:
        super().__init__()

    def _get_html(self) -> str:
        return requests.get(self.url).text

    def _parse_row_div(self, div: bs4.Tag) -> Optional[ReservoirRecord]:
        name: str = div.find("div", {"data-type": "Name"}).text.strip()  # type: ignore
        assert name, "No title found"

        if name in self.ignore_reservoirs:
            return None

        assert name in self.reservoirs, f"Unknown reservoir name: {name}"
        name_renamed = self.renames.get(name, name)

        content_div: bs4.Tag = div.find("div", {"data-type": "Speicherinhalt"})  # type: ignore
        content_span: bs4.Tag = content_div.find("span").find("span")  # type: ignore
        assert content_span, "No content span found"
        content_match = re.search(r"([\d\.]+) hm³", content_span.text)
        assert content_match, "No match found for content"
        content_mio_m3 = float(content_match.group(1))

        ts_small: bs4.Tag = content_div.find("small", recursive=True)  # type: ignore
        assert ts_small, "No timestamp small found"
        assert ts_small.text.strip(), "Timestamp small has no content"
        ts = dt.datetime.strptime(ts_small.text.strip(), "%d.%m.%y %H:%M").replace(tzinfo=TZ_BERLIN)

        return ReservoirRecord(
            self.name,
            name_renamed,
            ts,
            self.reservoirs[name]["capacity"],
            content_mio_m3,
        )

    def get_data(self, **kwargs) -> Iterable[ReservoirRecord]:
        html = self._get_html()
        soup = bs4.BeautifulSoup(html, "lxml")

        coords_div: bs4.Tag = soup.find("div", {"class": "list talsperre"})  # type: ignore
        assert coords_div, "div with class `list talsperre` not found"

        # return [self._parse_row_div(div) for div in coords_div.children if isinstance(div, bs4.Tag)]
        row_divs: bs4.ResultSet[bs4.Tag] = coords_div.find_all("div", {"class": "row"}, recursive=False)  # type: ignore
        return skip_errors_and_none(
            self._parse_row_div,
            row_divs,
        )
