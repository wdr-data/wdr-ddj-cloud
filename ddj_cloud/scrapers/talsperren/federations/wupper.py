import datetime as dt
from typing import Iterable

import requests

from ..common import TZ_UTC, ReservoirMeta, ReservoirRecord, Federation, apply_guarded


class WupperReservoirMeta(ReservoirMeta):
    ajax_id: str


class WupperFederation(Federation):
    name = "Wupperverband"

    reservoirs: dict[str, WupperReservoirMeta] = {
        "Bevertalsperre": {
            "ajax_id": "SBE$-T",
            "capacity_mio_m3": 23.76,
            "lat": 51.1415,
            "lon": 7.3704,
        },
        "Bruchertalsperre": {
            "ajax_id": "SBR$-T",
            "capacity_mio_m3": 3.38,
            "lat": 51.0814,
            "lon": 7.5576,
        },
        "Eschbachtalsperre": {
            "ajax_id": "SET$-T",
            "capacity_mio_m3": 1.05,
            "lat": 51.1585,
            "lon": 7.2364,
        },
        "Große Dhünntalsperre": {
            "ajax_id": "SHA$-T",
            "capacity_mio_m3": 72.08,
            "lat": 51.0714,
            "lon": 7.2122,
        },
        "Vorsperre Große Dhünn": {
            "ajax_id": "SVO$-T",
            "capacity_mio_m3": 7.45,
            "lat": 51.0729,
            "lon": 7.2394,
        },
        "Herbringhauser Talsperre": {
            "ajax_id": "SHT$-T",
            "capacity_mio_m3": 2.90,
            "lat": 51.2289,
            "lon": 7.2742,
        },
        "Kerspetalsperre": {
            "ajax_id": "SKT$-T",
            "capacity_mio_m3": 14.88,
            "lat": 51.1232,
            "lon": 7.4945,
        },
        "Lingesetalsperre": {
            "ajax_id": "SLI$-T",
            "capacity_mio_m3": 2.68,
            "lat": 51.0984,
            "lon": 7.5316,
        },
        "Neyetalsperre": {
            "ajax_id": "SNE$-T",
            "capacity_mio_m3": 5.98,
            "lat": 51.1370,
            "lon": 7.3930,
        },
        "Panzertalsperre": {
            "ajax_id": "SPAZ-T",
            "capacity_mio_m3": 0.19,
            "lat": 51.1804,
            "lon": 7.2754,
        },
        "Ronsdorfer Talsperre": {
            "ajax_id": "SRO$-T",
            "capacity_mio_m3": 0.12,
            "lat": 51.2192,
            "lon": 7.1835,
        },
        "Schevelinger Talsperre": {
            "ajax_id": "SSE$-T",
            "capacity_mio_m3": 0.344,
            "lat": 51.1341,
            "lon": 7.4326,
        },
        "Wuppertalsperre": {
            "ajax_id": "SWU$-T",
            "capacity_mio_m3": 25.09,
            "lat": 51.1992,
            "lon": 7.3032,
        },
        "Stausee Beyenburg": {
            "ajax_id": "SBY$-T",
            "capacity_mio_m3": 0.47,
            "lat": 51.2483,
            "lon": 7.2981,
        },
    }

    renames = {
        "Bever-Talsperre": "Bevertalsperre",
        "Brucher-Talsperre": "Bruchertalsperre",
        "Große Dhünn-Talsperre": "Große Dhünntalsperre",
        "Kerspe-Talsperre": "Kerspetalsperre",
        "Lingese-Talsperre": "Lingesetalsperre",
        "Neye-Talsperre": "Neyetalsperre",
        "Panzer-Talsperre": "Panzertalsperre",
        "Schevelinger-Talsperre": "Schevelinger Talsperre",
        "Wupper-Talsperre": "Wuppertalsperre",
    }

    ignore_reservoirs = [
        "Stauanlage Dahlhausen",
    ]

    def _build_url(self, ajax_id: str) -> str:
        return f"https://hwpsn.wupperverband.de/?ajax=MapData&id={ajax_id}&stationGroupId=null"

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
