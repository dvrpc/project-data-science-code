"""
This script illustrates the process of saving every
spatial table in a PostGIS database to shapefile.

"""
import psycopg2
from pathlib import Path
from geopandas import GeoDataFrame

# Change these parameters to point towards your database
UN = "postgres"
PW = "password"
HOST = "localhost"
PORT = 5432
DB_NAME = "ucity"

# Define where you want to save the resulting shapefiles
SHP_FOLDER = Path(".")


URI = f"postgresql://{UN}:{PW}@{HOST}:{PORT}/{DB_NAME}"


# Get a list of all spatial tables
sql_all_spatial_tables = """
    SELECT f_table_name AS tblname
    FROM geometry_columns
"""

connection = psycopg2.connect(URI)
cursor = connection.cursor()
cursor.execute(sql_all_spatial_tables)

spatial_tables = cursor.fetchall()

cursor.close()
connection.close()


# Iterate over the list of spatial tables, save each to shapefile

print(f"Saving {len(spatial_tables)} spatial tables to shapefile")

for table in spatial_tables:

    table = table[0]

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
