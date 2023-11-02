from typing import Generator, Iterable
import re

import bs4
import dateparser
import requests

from ..common import ReservoirRecord, Federation, TZ_BERLIN, apply_guarded


class WahnbachReservoirFederation(Federation):
    name = "Wahnbachtalsperrenverband"

    reservoirs = {
        "Wahnbachtalsperre": {
            "url": "https://www.wahnbach.de/die-wahnbachtalsperre/zahlen-und-fakten/pegelstand-stausee.html",
            "capacity": 40.92,
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

        body_div: bs4.Tag = soup.find("div", attrs={"class": "ce-bodytext"})  # type: ignore
        assert body_div, "No body div found"

        # Find headings and parse them and the following nodes
        headings: bs4.ResultSet[bs4.Tag] = body_div.find_all("h5")  # type: ignore
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
            following_nodes = heading.find_next_siblings(limit=3)
            assert following_nodes, "No following nodes found"
            assert len(following_nodes) >= 3, "Not enough following nodes found"

            # Parse content
            content_match = re.match(
                r"Stauinhalt:\s([\d,]+)\sMio.\s?m[3³]",
                following_nodes[1].text.strip(),
            )
            assert content_match, "No match found for content"
            content_mio_m3 = float(content_match.group(1).replace(",", "."))

            # # Parse percentage
            # percentage_match = re.search(r"Füllungsgrad: ([\d,]+) %", following_nodes[2].text)
            # assert percentage_match, "No match found for percentage"
            # percentage = float(percentage_match.group(1).replace(",", "."))

            yield ReservoirRecord(
                self.name,
                name,
                ts,
                self.reservoirs[name]["capacity"],
                content_mio_m3,
                # fill_ratio=percentage / 100.0,
            )

    def get_data(self, **kwargs) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(self._get_reservoir_records, self.reservoirs.keys()):
            yield from records


"""
Example HTML:

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
    <p>193 400 m³ am 03. August 1990<br> 189.450 m³ am 07. August 2020<br> 189.062 m³ am 06. August 2020</p>
    <p>&nbsp;</p>
</div>

"""
