# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "python-dotenv~=1.0",
#     "requests~=2.32",
#     "pydantic~=2.0",
# ]
# ///
"""One-off codegen script to (re)generate ``stations.json``.

The DWD station coordinates come from an authenticated WFS endpoint. The
``DWD_BRANDGEFAHRENINDIZES_BASIC_AUTH`` env var is the same Basic-Auth
credential that the public DWD station map
(https://www.dwd.de/DE/fachnutzer/landwirtschaft/appl/stationskarte/_node.html)
ships in its frontend JavaScript. DWD may rotate it at any time, so we do NOT
depend on this endpoint at scraper runtime. Instead we cache the resulting
coordinates in ``stations.json`` and refresh it manually by running this script:

    uv run ddj_cloud/scrapers/dwd_brandgefahrenindizes/update_stations.py

If the request starts failing with an auth error, grab a fresh
``Authorization: Basic ...`` header from the browser dev tools on the station
map page and update ``DWD_BRANDGEFAHRENINDIZES_BASIC_AUTH`` in ``.env``.
"""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

STATIONS_URL = "https://maps.dwd.de/geoserver/dwd/ows"

STATIONS_PARAMS = {
    "service": "wfs",
    "version": "1.0.0",
    "request": "GetFeature",
    "outputFormat": "json",
    "typeName": "ISABEL_stationen",
    "propertyname": "STATIONSNAME,STATIONSKENNUNG,GEOGR_BREITE,GEOGR_LAENGE",
    # Include every real station regardless of operator, so stations.json stays a
    # superset of whatever appears in the forecast tables; only exclude the junk
    # "TEST" entry. The per-operator allowlist used by the official website
    # silently dropped stations run by e.g. "Stadt Balingen".
    "cql_filter": "BETREIBER <> 'TEST'",
}

OUTPUT_PATH = Path(__file__).parent / "stations.json"
ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
AUTH_ENV_VAR = "DWD_BRANDGEFAHRENINDIZES_BASIC_AUTH"


class _StationProperties(BaseModel):
    stationsname: str = Field(alias="STATIONSNAME")
    stationskennung: str = Field(alias="STATIONSKENNUNG")
    latitude: float = Field(alias="GEOGR_BREITE")
    longitude: float = Field(alias="GEOGR_LAENGE")


class _StationFeature(BaseModel):
    properties: _StationProperties


class _StationFeatureCollection(BaseModel):
    features: list[_StationFeature]


class StationInfo(BaseModel):
    stationskennung: str
    latitude: float
    longitude: float


def _get_auth_header() -> str:
    load_dotenv(ENV_PATH)
    value = os.environ.get(AUTH_ENV_VAR, "").strip()
    if not value:
        msg = (
            f"Missing {AUTH_ENV_VAR}. Add the DWD station-map Basic Auth value "
            "to .env before regenerating stations.json."
        )
        raise SystemExit(msg)

    if value.lower().startswith("basic "):
        return value
    return f"Basic {value}"


def main() -> None:
    resp = requests.get(
        STATIONS_URL,
        params=STATIONS_PARAMS,
        headers={
            "Authorization": _get_auth_header(),
            "Referer": "https://www.dwd.de/",
        },
        timeout=60,
    )
    resp.raise_for_status()

    collection = _StationFeatureCollection.model_validate_json(resp.content)

    stations: dict[str, StationInfo] = {}
    skipped = 0
    for feature in collection.features:
        props = feature.properties
        name = props.stationsname.strip()
        if not name:
            skipped += 1
            continue
        stations[name] = StationInfo(
            stationskennung=props.stationskennung.strip(),
            latitude=props.latitude,
            longitude=props.longitude,
        )

    # Sort by name for a stable, review-friendly diff.
    stations = dict(sorted(stations.items()))

    with OUTPUT_PATH.open("w", encoding="utf-8") as fp:
        json.dump(
            {name: info.model_dump() for name, info in stations.items()},
            fp,
            ensure_ascii=False,
            indent=2,
        )
        fp.write("\n")

    print(f"Wrote {len(stations)} stations to {OUTPUT_PATH} (skipped {skipped})")


if __name__ == "__main__":
    try:
        main()
    except ValidationError as exc:
        msg = f"Failed to parse station response:\n{exc}"
        raise SystemExit(msg) from exc
