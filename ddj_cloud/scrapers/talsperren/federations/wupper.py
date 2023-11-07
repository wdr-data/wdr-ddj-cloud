import datetime as dt
from typing import Iterable

import requests

from ..common import TZ_UTC, ReservoirMeta, ReservoirRecord, Federation, apply_guarded


class WupperReservoirMeta(ReservoirMeta):
    ajax_id: str


class WupperFederation(Federation):
    name = "Wupperverband"

    reservoirs: dict[str, WupperReservoirMeta] = {
        "Bever-Talsperre": {
            "ajax_id": "SBE$-T",
            "capacity_mio_m3": 23.75,
        },
        "Brucher-Talsperre": {
            "ajax_id": "SBR$-T",
            "capacity_mio_m3": 3.37,
        },
        "Eschbachtalsperre": {
            "ajax_id": "SET$-T",
            "capacity_mio_m3": 1.05,
        },
        "Große Dhünn-Talsperre": {
            "ajax_id": "SHA$-T",
            "capacity_mio_m3": 81.00,
        },
        "Vorsperre Große Dhünn": {
            "ajax_id": "SVO$-T",
            "capacity_mio_m3": 7.37,
        },
        "Herbringhauser Talsperre": {
            "ajax_id": "SHT$-T",
            "capacity_mio_m3": 2.90,
        },
        "Kerspe-Talsperre": {
            "ajax_id": "SKT$-T",
            "capacity_mio_m3": 14.90,
        },
        "Lingese-Talsperre": {
            "ajax_id": "SLI$-T",
            "capacity_mio_m3": 2.60,
        },
        "Neyetalsperre": {
            "ajax_id": "SNE$-T",
            "capacity_mio_m3": 5.98,
        },
        "Panzer-Talsperre": {
            "ajax_id": "SPAZ-T",
            "capacity_mio_m3": 0.19,
        },
        "Ronsdorfer Talsperre": {
            "ajax_id": "SRO$-T",
            "capacity_mio_m3": 0.12,
        },
        "Schevelinger-Talsperre": {
            "ajax_id": "SSE$-T",
            "capacity_mio_m3": 0.31,
        },
        "Wupper-Talsperre": {
            "ajax_id": "SWU$-T",
            "capacity_mio_m3": 25.60,
        },
        "Stausee Beyenburg": {
            "ajax_id": "SBY$-T",
            "capacity_mio_m3": 0.47,
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

    def _build_url(self, ajax_id: str) -> str:
        return f"https://hochwasserportal.wupperverband.de/?ajax=MapData&id={ajax_id}"

    def _get_reservoir_records(self, name: str) -> list[ReservoirRecord]:
        url = self._build_url(self.reservoirs[name]["ajax_id"])
        response = requests.get(url).json()
        content_data = response["Speicherinhalt"]
        columns: list[str] = content_data["lines"]
        assert all(
            [column in columns for column in ["time", "Speicherinhalt"]]
        ), f"Unexpected column names: '{columns}' does not contain 'time' and 'Speicherinhalt'"

        timestamp_idx = columns.index("time")
        value_idx = columns.index("Speicherinhalt")

        return [
            ReservoirRecord(
                federation_name=self.name,
                name=self.renames.get(name, name),
                ts_measured=dt.datetime.fromtimestamp(row[timestamp_idx], TZ_UTC),
                capacity_mio_m3=self.reservoirs[name]["capacity_mio_m3"],
                content_mio_m3=row[value_idx],
            )
            for row in content_data["data"]
            if row[value_idx] is not None
        ]

    def get_data(self, **kwargs) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(self._get_reservoir_records, self.reservoirs.keys()):
            yield from records
