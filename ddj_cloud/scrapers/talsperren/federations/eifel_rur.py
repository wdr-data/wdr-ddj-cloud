import datetime as dt
from typing import Iterable, Optional

import bs4
import requests

from ..common import ReservoirMeta, ReservoirRecord, Federation, TZ_BERLIN, apply_guarded


class EifelRurReservoirMeta(ReservoirMeta):
    id: int


class EifelRurFederation(Federation):
    name = "Wasserverband Eifel-Rur"

    reservoirs: dict[str, EifelRurReservoirMeta] = {
        "Oleftalsperre": {
            "id": 261,
            "capacity_mio_m3": 19.30,
            "lat": 50.4952,
            "lon": 6.4216,
        },
        "Rurtalsperre Gesamt": {
            "id": 291,
            "capacity_mio_m3": 203.20,
            "lat": 50.637222,
            "lon": 6.441944,
        },
        "Rurtalsperre Obersee": {
            "id": 265,
            "capacity_mio_m3": 17.91,
            "lat": 50.6056,
            "lon": 6.3925,
        },
        "Rurtalsperre Hauptsee": {
            "id": 271,
            "capacity_mio_m3": 185.30,
            "lat": 50.637222,
            "lon": 6.441944,
        },
        "Urfttalsperre": {
            "id": 280,
            "capacity_mio_m3": 45.51,
            "lat": 50.6029,
            "lon": 6.4195,
        },
        "Wehebachtalsperre": {
            "id": 286,
            "capacity_mio_m3": 25.10,
            "lat": 50.7550,
            "lon": 6.3401,
        },
        "Stauanlage Heimbach": {
            "id": 354,
            "capacity_mio_m3": 1.21,
            "lat": 50.6285,
            "lon": 6.4792,
        },
        # "Stauanlage Obermaubach": {
        #     "id": ,
        #     "capacity_mio_m3": 1.65,
        #     "lat": 50.7143,
        #     "lon": 6.4483,
        # },
    }

    def _build_url(
        self,
        zr_id: int,
        *,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
    ) -> str:
        start_ts = (
            start.timestamp() if start else (dt.datetime.now() - dt.timedelta(days=14)).timestamp()
        )
        end_ts = end.timestamp() if end else dt.datetime.now().timestamp()

        return f"https://server.wver.de/pegeldaten/table_data.php?zr_id={zr_id}&timestamp_from={int(start_ts)}&timestamp_to={int(end_ts)}"

    def _get_html(self, url: str) -> str:
        return requests.get(url).text

    def _get_reservoir_records(
        self,
        name: str,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
    ) -> list[ReservoirRecord]:
        url = self._build_url(self.reservoirs[name]["id"], start=start, end=end)
        html = self._get_html(url)
        data = bs4.BeautifulSoup(html, "lxml")

        table = data.find("table")
        assert table, "No table found"

        rows = table.find_all("tr")  # type: ignore
        assert len(rows) > 1, "No data found"

        return [
            ReservoirRecord(
                federation_name=self.name,
                name=name,
                ts_measured=dt.datetime.fromisoformat(row["timestamp"]).replace(tzinfo=TZ_BERLIN),
                capacity_mio_m3=self.reservoirs[name]["capacity_mio_m3"],
                content_mio_m3=float(row["value"]),
            )
            for row in rows[1:]
            if float(row["value"]) >= 0  # Negative values seem to be errors
        ]

    def get_data(
        self,
        *,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
    ) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(
            lambda name: self._get_reservoir_records(name, start=start, end=end),
            self.reservoirs.keys(),
        ):
            yield from records
