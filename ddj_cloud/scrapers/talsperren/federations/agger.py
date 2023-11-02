import datetime as dt

import requests

from ..common import ReservoirRecord, Federation, TZ_UTC


class AggerFederation(Federation):
    name = "Aggerverband"

    reservoirs = {
        "Aggertalsperre": {
            "url": "https://gis.aggerverband.de/public/pegel/aggertalsperre_cm.json",
            "capacity": 17.06,
        },
        "Genkeltalsperre": {
            "url": "https://gis.aggerverband.de/public/pegel/genkeltalsperre_cm.json",
            "capacity": 8.20,
        },
        "Wiehltalsperre": {
            "url": "https://gis.aggerverband.de/public/pegel/wiehltalsperre_cm.json",
            "capacity": 31.85,
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
                self.name,
                name,
                dt.datetime.fromtimestamp(row[timestamp_idx] / 1000, TZ_UTC),
                self.reservoirs[name]["capacity"],
                row[value_idx],
            )
            for row in data[0]["data"]
        ]

    def get_data(self, **kwargs) -> list[ReservoirRecord]:
        return [
            record
            for name in self.reservoirs.keys()
            for record in self._get_reservoir_records(name)
        ]
