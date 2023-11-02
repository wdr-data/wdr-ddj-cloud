from typing import Generator, Iterable, Optional
import datetime as dt
import re

import bs4
import dateparser
import requests
import sentry_sdk

from ..common import ReservoirRecord, Federation, TZ_BERLIN, skip_errors_and_none


class GelsenwasserFederation(Federation):
    name = "Gelsenwasser"

    reservoirs = {
        "Talsperren Haltern und Hullern": {
            "url": "https://www.gelsenwasser.de/themen/unsere-talsperren",
            "capacity": 31.50,
        },
    }

    def __init__(self) -> None:
        super().__init__()

    def _get_html(self, url: str) -> str:
        return requests.get(url).text

    def _get_reservoir_records(
        self,
        name: str,
    ) -> Generator[ReservoirRecord, None, None]:
        url = self.reservoirs[name]["url"]
        html = self._get_html(url)
        soup = bs4.BeautifulSoup(html, "lxml")

        body_main: bs4.Tag = soup.find("main")  # type: ignore
        assert body_main, "No body main found"

        body_text = body_main.text.strip()

        # Find timestamp from heading
        # Example heading: Aktueller Füllstand unserer Talsperren - Stand 30. Oktober 2023
        ts_match = re.search(
            r"Aktueller Füllstand unserer Talsperren - Stand ([\d\.]+ \w+ \d+)",
            body_text,
        )
        assert ts_match, "Could not find heading with timestamp"
        ts = dateparser.parse(ts_match.group(1), languages=["de"])
        assert ts, f"Could not parse timestamp: {ts_match.group(1)}"
        ts = ts.replace(tzinfo=TZ_BERLIN)

        # This one is under construction, so try to get the current capacity from the text
        # Example (in heading): Spei­cher­volumen von 31,5 Mio. Ku­bik­metern
        # May contain &shy;
        capacity = self.reservoirs[name]["capacity"]
        capacity_match = re.search(r"Speichervolumen von ([\d,]+) Mio. Kubikmetern", body_text)
        if capacity_match:
            capacity = float(capacity_match.group(1).replace(",", "."))
        else:
            sentry_sdk.capture_message("Could not find capacity in text")

        # Find content
        # Example: Gesamt über alle Becken: 28,2 Mio. Kubikmeter / ca. 90 %
        content_match = re.search(
            r"Gesamt über alle Becken: ([\d,]+) Mio. Kubikmeter",
            body_text,
        )
        assert content_match, "Could not find content"
        content_mio_m3 = float(content_match.group(1).replace(",", "."))

        yield ReservoirRecord(
            self.name,
            name,
            ts,
            capacity,
            content_mio_m3,
        )

    def get_data(self, **kwargs) -> list[ReservoirRecord]:
        return [
            record
            for name in self.reservoirs.keys()
            for record in self._get_reservoir_records(name)
        ]
