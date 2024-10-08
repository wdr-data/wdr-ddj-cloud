import json

import requests as r
from datawrapper import Datawrapper as DatawrapperOriginal


class Datawrapper(DatawrapperOriginal):
    def add_data_json(self, chart_id: str, data: dict) -> r.Response:
        """Add data to a specified chart as json.

        Parameters
        ----------
        chart_id : str
            ID of chart, table or map to add data to.
        data : pd.DataFrame
            A pandas dataframe containing the data to be added.

        Returns
        -------
        requests.Response
            A requests.Response
        """

        _header = self._auth_header
        _header["content-type"] = "text/csv"
        _data = json.dumps(data)

        return r.put(
            url=f"{self._CHARTS_URL}/{chart_id}/data",
            headers=_header,
            data=_data.encode("utf-8"),
        )
