"""
Create a point shapefile of the pedestrian count locations
from an Excel file with lat/long values.
"""
import pandas as pd
import geopandas as gpd

from fy21_university_city import GDRIVE_FOLDER


def main():
    input_xlsx_filepath = GDRIVE_FOLDER / "ExistingConditions" / "UCD_All_Ped_Counts_Daily_Avg.xlsx"
    output_shapefile = GDRIVE_FOLDER / "Data/GIS/Ped_Count_Locations/Ped_Count_Locations.shp"

    # Read excel file
    df = pd.read_excel(input_xlsx_filepath, sheet_name="locations")

    # Convert to geodataframe, set/update EPSG, and write to file
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat))
    gdf.set_crs(epsg=4326, inplace=True)
    gdf.to_crs(epsg=26918, inplace=True)
    gdf.to_file(output_shapefile)


if __name__ == "__main__":
    main()
