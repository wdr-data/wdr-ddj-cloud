"""One-off script: convert dvg2bld_nw.shp (ETRS89/UTM32N) to WGS84 GeoJSON.

Run from the repo root:
    uv run ddj_cloud/data/geo/nrw_borders/convert_to_wgs84.py
"""

# /// script
# dependencies = ["geopandas"]
# ///

from pathlib import Path

import geopandas as gpd

HERE = Path(__file__).parent
SHP_PATH = HERE / "dvg2bld_nw.shp"
OUT_PATH = HERE / "dvg2bld_nw_wgs84.geojson"


def main():
    gdf = gpd.read_file(SHP_PATH)
    gdf_wgs84 = gdf.to_crs("EPSG:4326")
    gdf_wgs84.to_file(OUT_PATH, driver="GeoJSON")

    print(f"Written to {OUT_PATH}")
    print(f"Features: {len(gdf_wgs84)}")
    print(f"Geometry types: {gdf_wgs84.geometry.geom_type.unique().tolist()}")
    print(f"Bounds: {gdf_wgs84.total_bounds}")


if __name__ == "__main__":
    main()
