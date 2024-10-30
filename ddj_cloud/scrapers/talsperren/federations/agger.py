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


class AggerReservoirMeta(ReservoirMeta):
    url: str


class AggerFederation(Federation):
    name = "Aggerverband"

    reservoirs: dict[str, AggerReservoirMeta] = {  # type: ignore
        "Aggertalsperre": {
            "url": "https://gis.aggerverband.de/public/pegel/aggertalsperre_cm.json",
            "capacity_mio_m3": 17.06,
            "lat": 51.0359,
            "lon": 7.6330,
            "main_purpose": "Flussregulierung",
        },
        "Genkeltalsperre": {
            "url": "https://gis.aggerverband.de/public/pegel/genkeltalsperre_cm.json",
            "capacity_mio_m3": 8.19,
            "lat": 51.0618,
            "lon": 7.6262,
            "main_purpose": "Trinkwasserversorgung",
        },
        "Wiehltalsperre": {
            "url": "https://gis.aggerverband.de/public/pegel/wiehltalsperre_cm.json",
            "capacity_mio_m3": 31.85,
            "lat": 50.9473,
            "lon": 7.6706,
            "main_purpose": "Trinkwasserversorgung",
        },
    }

    def _get_reservoir_records(self, name: str) -> list[ReservoirRecord]:
        data = requests.get(self.reservoirs[name]["url"]).json()
        columns: list[str] = data[0]["columns"].split(",")
        assert len(data) == 1, "Expected exactly one data set"
        assert all(
            [column in columns for column in ["Timestamp", "Value"]]
        ), f"Unexpected column names: '{data[0]['columns']}' does not contain 'Timestamp' and 'Value'"

        timestamp_idx = columns.index("Timestamp")
        value_idx = columns.index("Value")

        return [
            ReservoirRecord(
                federation_name=self.name,
                name=name,
                ts_measured=dt.datetime.fromtimestamp(row[timestamp_idx] / 1000, TZ_UTC),
                capacity_mio_m3=self.reservoirs[name]["capacity_mio_m3"],
                content_mio_m3=row[value_idx],
            )
            for row in data[0]["data"]
            # Negative/null values seem to be errors
            if row[value_idx] is not None and row[value_idx] >= 0
        ]

    def get_data(
        self,
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ReservoirRecord]:
        for records in apply_guarded(self._get_reservoir_records, self.reservoirs.keys()):
            yield from records
