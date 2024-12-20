import datetime as dt
from collections.abc import Iterable

import requests

from ddj_cloud.scrapers.talsperren.common import (
    TZ_UTC,
    Federation,
    ReservoirMeta,
    ReservoirRecord,
    apply_guarded,
)


class WupperReservoirMeta(ReservoirMeta):
    ajax_id: str


class WupperFederation(Federation):
    name = "Wupperverband"

    reservoirs: dict[str, WupperReservoirMeta] = {  # type: ignore
        "Bevertalsperre": {
            "ajax_id": "SBE$-T",
            "capacity_mio_m3": 23.76,
            "lat": 51.1415,
            "lon": 7.3704,
            "main_purpose": "Flussregulierung",
        },
        "Bruchertalsperre": {
            "ajax_id": "SBR$-T",
            "capacity_mio_m3": 3.38,
            "lat": 51.0814,
            "lon": 7.5576,
            "main_purpose": "Flussregulierung",
        },
        "Eschbachtalsperre": {
            "ajax_id": "SET$-T",
            "capacity_mio_m3": 1.05,
            "lat": 51.1585,
            "lon": 7.2364,
            "main_purpose": "Trinkwasserversorgung",
        },
        "Große Dhünntalsperre": {
            "ajax_id": "SHA$-T",
            "capacity_mio_m3": 72.08,
            "lat": 51.0714,
            "lon": 7.2122,
            "main_purpose": "Trinkwasserversorgung; Flussregulierung",
        },
        "Vorsperre Große Dhünn": {
            "ajax_id": "SVO$-T",
            "capacity_mio_m3": 7.45,
            "lat": 51.0729,
            "lon": 7.2394,
            "main_purpose": "Trinkwasserversorgung",
        },
        "Herbringhauser Talsperre": {
            "ajax_id": "SHT$-T",
            "capacity_mio_m3": 2.90,
            "lat": 51.2289,
            "lon": 7.2742,
            "main_purpose": "Trinkwasserversorgung",
        },
        "Kerspetalsperre": {
            "ajax_id": "SKT$-T",
            "capacity_mio_m3": 14.88,
            "lat": 51.1232,
            "lon": 7.4945,
            "main_purpose": "Trinkwasserversorgung",
        },
        "Lingesetalsperre": {
            "ajax_id": "SLI$-T",
            "capacity_mio_m3": 2.68,
            "lat": 51.0984,
            "lon": 7.5316,
            "main_purpose": "Flussregulierung",
        },
        "Neyetalsperre": {
            "ajax_id": "SNE$-T",
            "capacity_mio_m3": 5.98,
            "lat": 51.1370,
            "lon": 7.3930,
            "main_purpose": "Flussregulierung",
        },
        "Panzertalsperre": {
            "ajax_id": "SPAZ-T",
            "capacity_mio_m3": 0.19,
            "lat": 51.1804,
            "lon": 7.2754,
            "main_purpose": "Flussregulierung",
        },
        "Ronsdorfer Talsperre": {
            "ajax_id": "SRO$-T",
            "capacity_mio_m3": 0.12,
            "lat": 51.2192,
            "lon": 7.1835,
            "main_purpose": "Flussregulierung",
        },
        "Schevelinger Talsperre": {
            "ajax_id": "SSE$-T",
            "capacity_mio_m3": 0.344,
            "lat": 51.1341,
            "lon": 7.4326,
            "main_purpose": "Flussregulierung",
        },
        "Wuppertalsperre": {
            "ajax_id": "SWU$-T",
            "capacity_mio_m3": 25.09,
            "lat": 51.1992,
            "lon": 7.3032,
            "main_purpose": "Flussregulierung",
        },
        "Stausee Beyenburg": {
            "ajax_id": "SBY$-T",
            "capacity_mio_m3": 0.47,
            "lat": 51.2483,
            "lon": 7.2981,
            "main_purpose": "Flussregulierung",
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

        # If no data is available, content_data is an empty list
        if len(content_data) == 0:
            return []

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

    def get_data(
        self,
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(self._get_reservoir_records, self.reservoirs.keys()):
            yield from records
