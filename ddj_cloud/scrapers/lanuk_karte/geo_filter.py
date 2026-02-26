"""Geographic filtering: keep only stations inside NRW."""

import json
from pathlib import Path

from shapely.geometry import Point, shape
from shapely.ops import unary_union

_geojson_path = (
    Path(__file__).parents[2] / "data" / "geo" / "nrw_borders" / "dvg2bld_nw_wgs84.geojson"
)

with open(_geojson_path) as _f:
    _nrw_geojson = json.load(_f)

if _nrw_geojson["type"] == "FeatureCollection":
    _nrw_polygon = unary_union([shape(f["geometry"]) for f in _nrw_geojson["features"]])
else:
    _nrw_polygon = shape(_nrw_geojson["geometry"])


def is_in_nrw(lat: float, lon: float) -> bool:
    return _nrw_polygon.contains(Point(lon, lat))
