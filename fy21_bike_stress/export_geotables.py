"""
This script illustrates the process of saving every
spatial table in a PostGIS database to shapefile.

"""
import psycopg2
from pathlib import Path
from geopandas import GeoDataFrame

# Step 1: Change these parameters to point towards your database
# --------------------------------------------------------------
UN = "postgres"
PW = "password"
HOST = "localhost"
PORT = 5432
DB_NAME = "ucity"

URI = f"postgresql://{UN}:{PW}@{HOST}:{PORT}/{DB_NAME}"

# Step 2: Define where you want to save the resulting shapefiles
# --------------------------------------------------------------
SHP_FOLDER = Path("/Users/aaron/data")


# Step 3A: Manually define a list of tables you want to export
#          To do so, update the first "table_list" and
#                    remove the 2nd one that is set to None
# ------------------------------------------------------------

table_list = ["first_table", "second_table"]
table_list = None


# Step 3B: Get a list of all spatial tables in the database
#          if none is defined
# ----------------------------------------------------------

if not table_list:

    sql_all_spatial_tables = """
        SELECT f_table_name AS tblname
        FROM geometry_columns
    """

    connection = psycopg2.connect(URI)
    cursor = connection.cursor()
    cursor.execute(sql_all_spatial_tables)

    spatial_tables = cursor.fetchall()
    tables = [x[0] for x in spatial_tables]

    cursor.close()
    connection.close()


# Step 4: Iterate over the list of spatial tables, save each to shapefile
# -----------------------------------------------------------------------

print(f"Saving {len(spatial_tables)} spatial tables to shapefile")

for table in tables:

    print("\t-> Saving", table)
    query = f"""
        SELECT * FROM {table}
    """

    connection = psycopg2.connect(URI)

    gdf = GeoDataFrame.from_postgis(query,
                                    connection,
                                    geom_col="geom")

    connection.close()

    shp_path = SHP_FOLDER / f"{table}.shp"
    gdf.to_file(shp_path)
