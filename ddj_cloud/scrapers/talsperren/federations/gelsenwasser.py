import re
from collections.abc import Generator, Iterable
from functools import lru_cache

import bs4
import dateparser
import requests
import sentry_sdk

from ddj_cloud.scrapers.talsperren.common import (
    TZ_BERLIN,
    Federation,
    ReservoirMeta,
    ReservoirRecord,
    apply_guarded,
)


class GelsenwasserReservoirMeta(ReservoirMeta):
    url: str


@lru_cache
def _get_html(url: str) -> str:
    return requests.get(url).text


class GelsenwasserFederation(Federation):
    name = "Gelsenwasser"

    reservoirs: dict[str, GelsenwasserReservoirMeta] = {  # type: ignore
        "Talsperren Haltern und Hullern": {
            "url": "https://www.gelsenwasser.de/themen/unsere-talsperren",
            "capacity_mio_m3": 31.50,
            # Mittig
            "lat": 51.7426053,
            "lon": 7.2485421,
        },
        "Talsperre Haltern Nordbecken": {
            "url": "https://www.gelsenwasser.de/themen/unsere-talsperren",
            "capacity_mio_m3": 17.00,
            "lat": 51.7491653,
            "lon": 7.2207341,
        },
        "Talsperre Haltern Südbecken": {
            "url": "https://www.gelsenwasser.de/themen/unsere-talsperren",
            "capacity_mio_m3": 3.50,
            "lat": 51.7378838,
            "lon": 7.210882,
        },
        "Talsperre Hullern": {
            "url": "https://www.gelsenwasser.de/themen/unsere-talsperren",
            "capacity_mio_m3": 11.00,
            "lat": 51.7457183,
            "lon": 7.2882871,
        },
    }

    def _get_reservoir_records(
        self,
        name: str,
    ) -> Generator[ReservoirRecord, None, None]:
        url = self.reservoirs[name]["url"]
        html = _get_html(url)
        soup = bs4.BeautifulSoup(html, "lxml")

        body_main: bs4.Tag = soup.find("main")  # type: ignore
        assert body_main, "No body main found"

        body_text = body_main.text.strip()
        body_text = body_text.replace("­", "")  # Remove soft hyphens

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
        # May contain &shy; (already stripped)
        capacity = self.reservoirs[name]["capacity_mio_m3"]

        if name == "Talsperren Haltern und Hullern":
            capacity_match = re.search(r"Speichervolumen von ([\d,]+) Mio. Kubikmetern", body_text)
            if capacity_match:
                capacity = float(capacity_match.group(1).replace(",", "."))
            else:
                sentry_sdk.capture_message("Could not find capacity in text")

        # Find content
        if name == "Talsperren Haltern und Hullern":
            # Example: Gesamt über alle Becken: 28,2 Mio. Kubikmeter / ca. 90 %
            content_match = re.search(
                r"Gesamt über alle Becken: ([\d,]+) Mio. Kubikmeter",
                body_text,
            )
            assert content_match, "Could not find content"
            content_mio_m3 = float(content_match.group(1).replace(",", "."))
        else:
            # Example: Tal­sperre Haltern Nord­becken: 39,18 Meter ü. NHN / 94 %
            # May contain &shy; (already stripped)
            content_match = re.search(
                name + r": ([\d,]+) Meter ü. NHN / ([\d,]+) %",
                body_text,
            )
            assert content_match, "Could not find content"
            percent_filled = float(content_match.group(2).replace(",", "."))
            content_mio_m3 = percent_filled / 100 * capacity

        yield ReservoirRecord(
            federation_name=self.name,
            name=name,
            ts_measured=ts,
            capacity_mio_m3=capacity,
            content_mio_m3=content_mio_m3,
        )

    def get_data(
        self,
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(self._get_reservoir_records, self.reservoirs.keys()):
            yield from records
