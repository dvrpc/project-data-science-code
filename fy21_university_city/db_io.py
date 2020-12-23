import pandas as pd

from postgis_helpers import PostgreSQL

from philly_transit_data import TransitData

from fy21_university_city import DB_NAME, GDRIVE_FOLDER


def db_connection(db_name: str = DB_NAME) -> PostgreSQL:
    return PostgreSQL(db_name, verbosity="minimal")


def setup(db_name: str = DB_NAME):

    db = db_connection(db_name)

    all_tables = db.all_tables_as_list()

    # Load APC data
    if "apc_raw" not in all_tables:
        apc_csv = GDRIVE_FOLDER / "Data/2019_Spring_APC_Stop_by_Route.csv"
        df = pd.read_csv(apc_csv)
        df.drop(columns=["Unnamed: 0"], inplace=True)
        db.import_dataframe(df, "apc_raw", if_exists="replace")

    # Load regional transit stops
    if "transit_stops" not in all_tables:
        transit_data = TransitData()
        stops, lines = transit_data.all_spatial_data()
        db.import_geodataframe(stops, "transit_stops")

    # Load study area bounds
    if "study_bounds" not in all_tables:
        bounds_shp = (
            GDRIVE_FOLDER
            / "Data/GIS/Draft_Study_Area_Extent"
            / "U_CIty_Study_Area_Dissolve_2.shp"
        )
        db.import_geodata("study_bounds", bounds_shp)

    # Load the 2013 HTS Trip table
    if "hts_2013_trips" not in all_tables:
        hts_xlsx = (
            GDRIVE_FOLDER
            / "Data/PublicUse "
            / "export_from_access_db_4_Trip_Public.xlsx"
        )
        df = pd.read_excel(hts_xlsx)
        db.import_dataframe(df, "hts_2013_trips")


def export_shp(table_to_export: str, db_name: str = DB_NAME):

    db = db_connection(db_name)

    export_folder = GDRIVE_FOLDER / "Data/Analysis_Exports"

    db.export_shapefile(table_to_export, export_folder)


def export_table(table_to_export: str, db_name: str = DB_NAME):

    db = db_connection(db_name)

    export_folder = GDRIVE_FOLDER / "Data/Analysis_Exports"

    df = db.query_as_df(f"SELECT * FROM {table_to_export}")

    filepath = export_folder / f"{table_to_export}.xlsx"
    df.to_excel(filepath)
