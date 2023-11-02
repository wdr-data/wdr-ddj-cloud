import datetime as dt

import requests

from ..common import TZ_UTC, ReservoirRecord, Federation


class WupperFederation(Federation):
    name = "Wupperverband"

    reservoirs = {
        "Bever-Talsperre": {
            "ajax_id": "SBE$-T",
            "capacity": 23.75,
        },
        "Brucher-Talsperre": {
            "ajax_id": "SBR$-T",
            "capacity": 3.37,
        },
        "Eschbachtalsperre": {
            "ajax_id": "SET$-T",
            "capacity": 1.05,
        },
        "Große Dhünn-Talsperre": {
            "ajax_id": "SHA$-T",
            "capacity": 81.00,
        },
        "Vorsperre Große Dhünn": {
            "ajax_id": "SVO$-T",
            "capacity": 7.37,
        },
        "Herbringhauser Talsperre": {
            "ajax_id": "SHT$-T",
            "capacity": 2.90,
        },
        "Kerspe-Talsperre": {
            "ajax_id": "SKT$-T",
            "capacity": 14.90,
        },
        "Lingese-Talsperre": {
            "ajax_id": "SLI$-T",
            "capacity": 2.60,
        },
        "Neyetalsperre": {
            "ajax_id": "SNE$-T",
            "capacity": 5.98,
        },
        "Panzer-Talsperre": {
            "ajax_id": "SPAZ-T",
            "capacity": 0.19,
        },
        "Ronsdorfer Talsperre": {
            "ajax_id": "SRO$-T",
            "capacity": 0.12,
        },
        "Schevelinger-Talsperre": {
            "ajax_id": "SSE$-T",
            "capacity": 0.31,
        },
        "Wupper-Talsperre": {
            "ajax_id": "SWU$-T",
            "capacity": 25.60,
        },
        "Stausee Beyenburg": {
            "ajax_id": "SBY$-T",
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
                self.name,
                self.renames.get(name, name),
                dt.datetime.fromtimestamp(row[timestamp_idx], TZ_UTC),
                self.reservoirs[name]["capacity"],
                row[value_idx],
            )
            for row in content_data["data"]
            if row[value_idx] is not None
        ]

    def get_data(self, **kwargs) -> list[ReservoirRecord]:
        return [
            record
            for name in self.reservoirs.keys()
            for record in self._get_reservoir_records(name)
        ]
