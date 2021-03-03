"""
process_urbansim_data.py
------------------------

Import regional parcel data and CSV with upcoming development projects.

Join the two tables together, and then save result to database & shapefile.
"""

import pg_data_etl as pg

from fy21_university_city import GDRIVE_FOLDER


if __name__ == "__main__":

    # Connect to PostgreSQL database and ensure it exists
    db = pg.Database("ucity_urbansim", **pg.connections["localhost"])
    db.create_db()

    # Import parcel shapefile if not already in db
    if "public.parcels" not in db.spatial_table_list():
        parcel_shp_path = GDRIVE_FOLDER / "GIS" / "parcels" / "parcels.shp"
        db.shp2pgsql(parcel_shp_path, 26918, "parcels")

    # Import project table if not already in db
    if "public.projects" not in db.table_list():
        development_table_path = GDRIVE_FOLDER / "GIS" / "all_devprojects_2021_03_02.csv"
        db.import_tabular_file(development_table_path, "projects")

    # Join the two tables together, and save the result in the database
    join_query = """
        with exploded_projects as (
            select unnest(string_to_array(parcel_id, ';'))::int AS join_id, *
            from projects
        )
        select parcels.geom, exploded_projects.*
        from parcels
        inner join exploded_projects
        on parcels.primary_id = exploded_projects.join_id
    """
    db.make_geotable_from_query(join_query, "project_parcels", "MULTIPOLYGON", 26918)

    # Export the joined data to GoogleDrive as a shapefile
    output_shp = GDRIVE_FOLDER / "GIS" / "parcels_post_join" / "project_parcels.shp"
    db.ogr2ogr_export("project_parcels", output_shp)
