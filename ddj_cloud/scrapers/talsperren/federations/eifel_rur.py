import datetime as dt
from collections.abc import Iterable
from typing import NotRequired
from urllib.parse import quote

import requests

from ddj_cloud.scrapers.talsperren.common import (
    TZ_BERLIN,
    Federation,
    ReservoirMeta,
    ReservoirRecord,
    apply_guarded,
)

BASE_URL = "https://wver.de/karten_messwerte/Messdatenportal/Messdaten/"


class EifelRurReservoirMeta(ReservoirMeta):
    station_name: str
    skip: NotRequired[bool]


class EifelRurFederation(Federation):
    name = "Wasserverband Eifel-Rur"

    reservoirs: dict[str, EifelRurReservoirMeta] = {  # type: ignore
        "Oleftalsperre": {
            "station_name": "Oleftalsperre OW",
            "capacity_mio_m3": 19.30,
            "lat": 50.4952,
            "lon": 6.4216,
            "main_purpose": "Trinkwasserversorgung",
        },
        # Doesn't seem to have data anymore
        "Rurtalsperre Gesamt": {
            "station_name": "",
            "skip": True,
            "capacity_mio_m3": 203.20,
            "lat": 50.637222,
            "lon": 6.441944,
            "main_purpose": "Trinkwasserversorgung; Flussregulierung",
        },
        "Rurtalsperre Obersee": {
            "station_name": "Rurtalsperre Obersee OW",
            "capacity_mio_m3": 17.77,
            "lat": 50.6056,
            "lon": 6.3925,
            "main_purpose": "Trinkwasserversorgung",
        },
        "Rurtalsperre Hauptsee": {
            "station_name": "Rurtalsperre Hauptsee OW",
            "capacity_mio_m3": 184.83,
            "lat": 50.637222,
            "lon": 6.441944,
            "main_purpose": "Flussregulierung",
        },
        "Urfttalsperre": {
            "station_name": "Urfttalsperre OW",
            "capacity_mio_m3": 45.51,
            "lat": 50.6029,
            "lon": 6.4195,
            "main_purpose": "Flussregulierung",
        },
        "Wehebachtalsperre": {
            "station_name": "Wehebachtalsperre OW",
            "capacity_mio_m3": 25.06,
            "lat": 50.7550,
            "lon": 6.3401,
            "main_purpose": "Trinkwasserversorgung; Flussregulierung",
        },
        "Stauanlage Heimbach": {
            "station_name": "Stb. Heimbach OW",
            "capacity_mio_m3": 1.21,
            "lat": 50.6285,
            "lon": 6.4792,
            "main_purpose": "Flussregulierung",
        },
        "Stauanlage Obermaubach": {
            "station_name": "Stb. Obermaubach OW",
            "capacity_mio_m3": 1.65,
            "lat": 50.7143,
            "lon": 6.4483,
            "main_purpose": "Flussregulierung",
        },
    }

    def _build_url(self, station_name: str) -> str:
        return f"{BASE_URL}{quote(station_name)}TalsperreninhaltTag.Mittel.json"

    def _get_json(self, url: str):
        return requests.get(url).json()

    def _get_reservoir_records(self, name: str) -> list[ReservoirRecord]:
        if self.reservoirs[name].get("skip", False):
            return []

        url = self._build_url(self.reservoirs[name]["station_name"])
        json_data = self._get_json(url)

        assert "data" in json_data, "No data found in JSON response"
        assert json_data.get("ts_unitname") == "Millionen Kubikmeter", (
            f"Unexpected unit: {json_data.get('ts_unitname')}"
        )

        return [
            ReservoirRecord(
                federation_name=self.name,
                name=name,
                ts_measured=dt.datetime.fromisoformat(entry[0]).astimezone(TZ_BERLIN),
                capacity_mio_m3=self.reservoirs[name]["capacity_mio_m3"],
                content_mio_m3=float(entry[1]),
            )
            for entry in json_data["data"]
            if entry[1] is not None and float(entry[1]) >= 0
        ]

    def get_data(
        self,
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(
            self._get_reservoir_records,
            self.reservoirs.keys(),
        ):
            yield from records
