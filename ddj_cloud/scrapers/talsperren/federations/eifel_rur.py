import datetime as dt
from collections.abc import Iterable
from typing import Literal, NotRequired

import requests

from ddj_cloud.scrapers.talsperren.common import (
    TZ_BERLIN,
    Federation,
    ReservoirMeta,
    ReservoirRecord,
    apply_guarded,
)


class EifelRurReservoirMeta(ReservoirMeta):
    id: int
    skip: NotRequired[bool]


class EifelRurFederation(Federation):
    name = "Wasserverband Eifel-Rur"

    reservoirs: dict[str, EifelRurReservoirMeta] = {  # type: ignore
        "Oleftalsperre": {
            "id": 6,
            "capacity_mio_m3": 19.30,
            "lat": 50.4952,
            "lon": 6.4216,
        },
        # Doesn't seem to have data anymore
        "Rurtalsperre Gesamt": {
            "id": 14,
            "skip": True,
            "capacity_mio_m3": 203.20,
            "lat": 50.637222,
            "lon": 6.441944,
        },
        "Rurtalsperre Obersee": {
            "id": 13,
            "capacity_mio_m3": 17.77,
            "lat": 50.6056,
            "lon": 6.3925,
        },
        "Rurtalsperre Hauptsee": {
            "id": 12,
            "capacity_mio_m3": 184.83,
            "lat": 50.637222,
            "lon": 6.441944,
        },
        "Urfttalsperre": {
            "id": 16,
            "capacity_mio_m3": 45.51,
            "lat": 50.6029,
            "lon": 6.4195,
        },
        "Wehebachtalsperre": {
            "id": 17,
            "capacity_mio_m3": 25.06,
            "lat": 50.7550,
            "lon": 6.3401,
        },
        "Stauanlage Heimbach": {
            "id": 2,
            "capacity_mio_m3": 1.21,
            "lat": 50.6285,
            "lon": 6.4792,
        },
        "Stauanlage Obermaubach": {
            "id": 5,
            "capacity_mio_m3": 1.65,
            "lat": 50.7143,
            "lon": 6.4483,
        },
    }

    def _build_url(
        self,
        id: int,
        *,
        days: Literal[3, 7, 30] = 30,
    ) -> str:
        return f"https://wver.de/wp-json/pegel/verlauf/type=cluster&id={id}&days={days}"

    def _get_json(self, url: str):
        return requests.get(url).json()

    def _get_reservoir_records(self, name: str) -> list[ReservoirRecord]:
        if self.reservoirs[name].get("skip", False):
            return []

        url = self._build_url(self.reservoirs[name]["id"])
        json_data = self._get_json(url)

        assert "sensoren" in json_data, "No sensors found in JSON data"

        content_sensor = next(
            (sensor for sensor in json_data["sensoren"] if sensor["name"] == "Stauinhalt"),
            None,
        )
        assert content_sensor, "No content sensor found"

        return [
            ReservoirRecord(
                federation_name=self.name,
                name=name,
                ts_measured=dt.datetime.fromisoformat(entry["timestamp"]).replace(tzinfo=TZ_BERLIN),
                capacity_mio_m3=self.reservoirs[name]["capacity_mio_m3"],
                content_mio_m3=float(entry["value"]),
            )
            for entry in content_sensor["pegelverlauf"]
            if float(entry["value"]) >= 0  # Negative values seem to be errors
        ]

    def get_data(
        self,
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(
            lambda name: self._get_reservoir_records(name),
            self.reservoirs.keys(),
        ):
            yield from records
