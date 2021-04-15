from pathlib import Path
import pandas as pd
import geopandas as gpd

from fy21_sidewalk_data import GDRIVE_BASE


def filter_src_file(filepath: Path) -> gpd.GeoDataFrame:
    """
    Read source geofile, omitting features in the list of IDs omit
    """
    ids_to_omit = [
        10,
        21,
        39,
        41,
        56,
        93,
        96,
        105,
        253,
    ]
    gdf = gpd.read_file(filepath)
    return gdf[~gdf["dvrpc_id"].isin(ids_to_omit)]


def read_delta_file(filepath: Path) -> gpd.GeoDataFrame:
    """
    Read geofile, rename ID column, and project to 26918
    """
    return gpd.read_file(filepath).rename(columns={"dvrpc_ID": "dvrpc_id"}).to_crs(epsg=26918)


def merge_files(list_of_gdfs: list, output_path: Path) -> None:
    """
    Merge a list of geodataframes, and write to file
    """
    gdf = pd.concat(list_of_gdfs)
    gdf.to_file(output_path)


def main():

    project_dir = GDRIVE_BASE / "projects/RideScore/inputs"
    updated_data_dir = project_dir / "updated_stations"
    output_dir = GDRIVE_BASE / "projects/Sidewalk Gaps/data-to-import"

    # Read raw data
    src_gdf = filter_src_file(project_dir / "PassengerRailStations.shp")

    # Read delta files
    sw_gdf = read_delta_file(updated_data_dir / "SW_NewNodes.shp")
    osm_gdf = read_delta_file(updated_data_dir / "OSM_NewNodes.shp")

    # Merge / write to disk
    merge_files([src_gdf, sw_gdf], output_dir / "station_pois_for_sw.shp")
    merge_files([src_gdf, osm_gdf], output_dir / "station_pois_for_osm.shp")


if __name__ == "__main__":
    main()
