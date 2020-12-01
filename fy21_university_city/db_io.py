from pathlib import Path
import pandas as pd

import postgis_helpers as pGIS
from postgis_helpers import PostgreSQL

from philly_transit_data import TransitData

DB_NAME = "ucity"

GDRIVE_FOLDER = Path("/Volumes/GoogleDrive/Shared drives/U_City_FY_21")


def db_connection(db_name: str = DB_NAME) -> PostgreSQL:
    return PostgreSQL(db_name, verbosity="minimal")


def setup(db_name: str = DB_NAME):

    db = db_connection(db_name)

    # Load APC data
    apc_csv = GDRIVE_FOLDER / "Data/2019_Spring_APC_Stop_by_Route.csv"
    df = pd.read_csv(apc_csv)
    df.drop(columns=["Unnamed: 0"], inplace=True)
    db.import_dataframe(df, "apc_raw", if_exists="replace")

    # Load regional transit stops
    transit_data = TransitData()
    stops, lines = transit_data.all_spatial_data()
    db.import_geodataframe(stops, "transit_stops")

    # Load study area bounds
    bounds_shp = GDRIVE_FOLDER / "Data/GIS/Draft_Study_Area_Extent/U_CIty_Study_Area_Dissolve_2.shp"
    db.import_geodata("study_bounds", bounds_shp)


def export_shp(table_to_export: str, db_name: str = DB_NAME):

    db = db_connection(db_name)

    export_folder = GDRIVE_FOLDER / "Data/GIS/Analysis_Exports"

    db.export_shapefile(table_to_export, export_folder)
