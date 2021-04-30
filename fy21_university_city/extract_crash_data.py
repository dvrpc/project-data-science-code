"""
Aggregate collisions within the study area
"""
# from pathlib import Path
import pg_data_etl as pg

from fy21_university_city import GDRIVE_FOLDER
from fy21_university_city.db_io import db_connection
from fy21_university_city.process_bus_data import prep_data


# def copy_crashes_between_databases(src_db: pg.Database, target_db: pg.Database) -> None:
#     src_db.copy_table_to_another_db("transportation.crash_pennsylvania", target_db)


def get_crash_data_from_server_into_geojson():
    gis_db = pg.Database("gis", **pg.connections()["dvrpc_gis"])

    query = """
        select *
        from transportation.crash_pennsylvania cp 
        where county = '67'
        and not st_isempty(shape)
    """

    philly_crashes = gis_db.query(query, geo=True, query_kwargs={"geom_col": "shape"})

    philly_crashes.gdf.to_file("philly_crashes", driver="GeoJSON")


# def main():

#     prep_data(db_connection())

#     gis_folder = GDRIVE_FOLDER / "Data/GIS"

#     gis_db = pg.Database("gis", **pg.connections()["dvrpc_gis"])
#     db = pg.Database("ucity", **pg.connections()["localhost"])

#     db.create_db()

#     if "transportation.crash_pennsylvania" not in db.spatial_table_list():
#         copy_crashes_between_databases(gis_db, db)

#     # if not db.check_projection("transportation.crash_pennsylvania", 26918):
#     #     db.project_spatial_table(
#     #         "transportation.crash_pennsylvania", old_epsg=4326, new_epsg=26918, geom_type="POINT"
#     #     )

#     # Import study area shapefile
#     study_area_shp = gis_folder / "Draft_Study_Area_Extent/U_CIty_Study_Area_Dissolve_2.shp"
#     db.shp2pgsql(study_area_shp, 26918, "study_area")

#     # Filter crashes to study area
#     crash_query = """
#         select *
#         from transportation.crash_pennsylvania cp
#         where
#         st_intersects(st_transform(geom, 26918), (select st_collect(geom) from study_area))
#     """
#     db.make_geotable_from_query(crash_query, "study_area_crashes", "POINT", 4326)
#     db.project_spatial_table("study_area_crashes", 4326, 26918, "POINT")

#     db.pg_dump()


if __name__ == "__main__":
    # main()
    get_crash_data_from_server_into_geojson()
