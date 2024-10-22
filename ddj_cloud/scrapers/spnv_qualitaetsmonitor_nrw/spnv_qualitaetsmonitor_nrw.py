from collections.abc import Iterable
from typing import cast

import pandas as pd
import requests

from ddj_cloud.utils.storage import upload_dataframe

from .models import Filters, Results

BASE_URL = "https://infoportal.mobil.nrw"


def _load_filters():
    response = requests.get(f"{BASE_URL}/QmFilterShow.html")
    response.raise_for_status()

    response_json = response.json()
    assert isinstance(response_json, list), "Unexpected response type for QmFilterShow"

    return Filters.from_json(response_json)


def _list_param_raw(target: str, items: Iterable[str]):
    return f"tx_cpqualitymonitor_ajaxlistfilter[filter][{target}]", ",".join(items)


def _list_param(target: Filters.Target):
    return _list_param_raw(target.target, [str(item.title) for item in target.items])


def _load_year(targets: list[Filters.Target], year: int):
    url = f"{BASE_URL}/QmAjaxListFilter.html"
    post_data = dict(
        (
            _list_param_raw("year", [str(year)]),
            *map(_list_param, targets),
        )
    )

    response = requests.post(url, data=post_data)
    response.raise_for_status()

    response_json = response.json()
    assert isinstance(response_json, dict), "Unexpected response type for QmAjaxListFilter"
    assert "data" in response_json, "No data in response"

    for result in response_json["data"]:
        yield Results.Data.model_validate(result)


def _to_quarter_rows(data: Results.Data, year: int):
    quarterly_columns = (
        "overall_ranking",
        "complexity",
        "punctuality",
        "reliability",
        "train_formation",
        "passengers",
    )
    for quarter in range(4):
        # Skip quarters where data is unavailable
        if data.overall_ranking.quarters[quarter] == 0:
            continue

        base_data = {
            "year": year,
            "quarter": quarter,
            **data.model_dump(),
        }
        quarterly_data = {
            column: cast(Results.Column, getattr(data, column)).quarters[quarter]
            for column in quarterly_columns
        }

        yield base_data | quarterly_data


def run():
    filters_data = _load_filters()

    targets_without_year: list[Filters.Target] = []
    years_available: list[int] = []

    for target in filters_data.targets:
        match target.target:
            case "year":
                years_available = [item.title for item in target.items]
            case _:
                targets_without_year.append(target)

    assert len(years_available) > 0, "No years available"

    rows: list[dict] = []
    for year in years_available:
        for result in _load_year(targets_without_year, year):
            rows.extend(_to_quarter_rows(result, year))

    df = pd.DataFrame(rows)
    upload_dataframe(df, "spnv_qualitaetsmonitor_nrw/data.csv")
