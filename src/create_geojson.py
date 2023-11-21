import logging
import os
import geopandas as gpd

from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)


def run_create_geojson() -> None:
    logging.info("Load shapefile....")
    sa2 = gpd.read_file(
        os.path.join(
            Path(os.path.abspath("")),
            "geo/sa2-2018/statistical-area-2-2018-clipped-generalised.shp",
        )
    )
    regions = gpd.read_file(
        os.path.join(
            Path(os.path.abspath("")),
            "geo/regional_council-2018/regional-council-2018-generalised.shp",
        )
    )
    logging.info("Bring shapefile to stardard CRS system....")
    regions.to_crs("EPSG:4236", inplace=True)
    regions.rename(columns={"REGC2018_1": "region"}, inplace=True)

    sa2.to_crs("EPSG:4326", inplace=True)
    sa2.rename(columns={"SA22018__1": "region"}, inplace=True)

    logging.info("Write shapefile to stardard CRS system....")
    # out_gpd = gpd.pd.concat(
    #     [regions[["region", "geometry"]], sa2[["region", "geometry"]]],
    #     ignore_index=True,
    # )
    # out_gpd.to_file("test.geojson", driver="GeoJSON")
    regions[["region", "geometry"]].to_file(
        "regional_council.geojson", driver="GeoJSON"
    )
    sa2[["region", "geometry"]].to_file("sa2.geojson", driver="GeoJSON")


if __name__ == "__main__":
    run_create_geojson()
