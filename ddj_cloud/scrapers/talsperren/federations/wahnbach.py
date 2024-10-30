import re
from collections.abc import Generator, Iterable

import bs4
import dateparser
import requests

from ddj_cloud.scrapers.talsperren.common import (
    TZ_BERLIN,
    Federation,
    ReservoirMeta,
    ReservoirRecord,
    apply_guarded,
)


class WahnbachReservoirMeta(ReservoirMeta):
    url: str


class WahnbachReservoirFederation(Federation):
    name = "Wahnbachtalsperrenverband"

    reservoirs: dict[str, WahnbachReservoirMeta] = {  # type: ignore
        "Wahnbachtalsperre": {
            "url": "https://www.wahnbach.de/die-wahnbachtalsperre/zahlen-und-fakten/pegelstand-stausee.html",
            "capacity_mio_m3": 40.92,
            "lat": 50.8049,
            "lon": 7.2838,
            "main_purpose": "Trinkwasserversorgung; Flussregulierung",
        },
    }

    def _get_html(self, url: str) -> str:
        return requests.get(url, timeout=10).text

    def _get_reservoir_records(
        self,
        name: str,
    ) -> Generator[ReservoirRecord, None, None]:
        url = self.reservoirs[name]["url"]
        html = self._get_html(url)
        soup = bs4.BeautifulSoup(html, "lxml")

        body_div: bs4.Tag = soup.find("div", attrs={"class": "ce-bodytext"})  # type: ignore
        assert body_div, "No body div found"

        # Find headings and parse them and the following nodes
        headings: bs4.ResultSet[bs4.Tag] = body_div.find_all("h5")
        assert len(headings) > 1, "No headings found"

        for heading in headings:
            heading_text = heading.text.strip()
            if not heading_text:
                print("Skipping empty heading")
                continue
            elif heading_text == "Bisherige Tages-Spitzenabgaben:":
                print("Skipping heading with historical data")
                continue

            # Parse heading
            heading_match = re.search(r"(?:Aktuelle )?Daten vom (.*?):", heading_text)
            assert heading_match, f"Could not parse heading: {heading_text}"
            heading_ts = dateparser.parse(heading_match.group(1), languages=["de"])
            assert heading_ts, f"Could not parse heading timestamp: {heading_match.group(1)}"
            ts = heading_ts.replace(tzinfo=TZ_BERLIN)

            # Parse following nodes
            following_nodes = heading.find_next_siblings(limit=1)
            assert following_nodes, "No following nodes found"
            assert len(following_nodes) >= 1, "Not enough following nodes found"

            # Parse content
            content_match = re.search(
                r"Stauinhalt:\s([\d,]+)\sMio.\s?m[3³]",
                following_nodes[0].text.strip(),
            )
            assert content_match, "No match found for content"
            content_mio_m3 = float(content_match.group(1).replace(",", "."))

            # # Parse percentage
            # percentage_match = re.search(r"Füllungsgrad: ([\d,]+) %", following_nodes[2].text)
            # assert percentage_match, "No match found for percentage"
            # percentage = float(percentage_match.group(1).replace(",", "."))

            yield ReservoirRecord(
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
        for records in apply_guarded(self._get_reservoir_records, self.reservoirs.keys()):
            yield from records


"""
Example HTML:

<div class="ce-bodytext">
    <h5><br> Aktuelle Daten vom 22. Januar 2024:</h5>
    <p>Staupegel: 122,88 mNN<br> Stauinhalt: 38,516 Mio.&nbsp;m<sup>3</sup><br>
    Füllungsgrad: 94,16 %</p>
    <h5><br> Daten vom 15. Januar 2024:</h5>
    <p>Staupegel: 123,12 mNN<br> Stauinhalt: 38,890 Mio.&nbsp;m<sup>3</sup><br>
    Füllungsgrad: 95,29 %</p>
    <h5><br> Daten vom 08. Januar 2024:</h5>
    <p>Staupegel: 123,21 mNN<br> Stauinhalt: 39,155 Mio.&nbsp;m<sup>3</sup><br>
    Füllungsgrad: 95,72 %</p>
    <p>&nbsp;</p>
    <h5>Bisherige Tages-Spitzenabgaben:</h5>
    <p>(Eine normale Abgabemenge liegt bei zirka 130.000 m³/d.)</p>
    <p>193 400 m³ am 03. August 1990<br> 189.450 m³ am 07. August 2020<br>
    189.062 m³ am 06. August 2020</p>
    <p>&nbsp;</p>
</div>


OLD:
<div class="ce-bodytext">
    <h5>Aktuelle Daten vom 30. Oktober 2023:</h5>
    <p>Staupegel: 119,93 mNN</p>
    <p>Stauinhalt:&nbsp;33,059 Mio. m<sup>3</sup></p>
    <p>Füllungsgrad: 80,82 %</p>
    <p>&nbsp;</p>
    <h5>Daten vom 23. Oktober 2023:</h5>
    <p>Staupegel: 119,55 mNN</p>
    <p>Stauinhalt:&nbsp;32,389 Mio. m<sup>3</sup></p>
    <p>Füllungsgrad: 79,18 %</p>
    <p>&nbsp;</p>
    <h5>Daten vom 16. Oktober 2023:</h5>
    <p>Staupegel: 119,63 mNN</p>
    <p>Stauinhalt:&nbsp;32,530 Mio. m<sup>3</sup></p>
    <p>Füllungsgrad: 79,52 %</p>
    <h5>&nbsp;</h5>
    <h5>&nbsp;</h5>
    <h5>Bisherige Tages-Spitzenabgaben:</h5>
    <p>(Eine normale Abgabemenge liegt bei zirka 130.000 m³/d.)</p>
    <p>193 400 m³ am 03. August 1990<br> 189.450 m³ am 07. August 2020<br>
    189.062 m³ am 06. August 2020</p>
    <p>&nbsp;</p>
</div>
"""
