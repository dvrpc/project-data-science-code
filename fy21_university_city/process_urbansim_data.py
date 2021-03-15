"""
process_urbansim_data.py
------------------------

Import regional parcel data and CSV with upcoming development projects.

Join the two tables together, and then save result to database & shapefile.
"""
import geopandas as gpd
import pg_data_etl as pg
from tqdm import tqdm

from fy21_university_city import GDRIVE_FOLDER


def main():
    # Connect to PostgreSQL database and ensure it exists
    db = pg.Database("ucity_urbansim", **pg.connections["localhost"])
    db.create_db()

    print("Importing data")

    # Import parcel shapefile if not already in db
    if "public.parcels" not in db.spatial_table_list():
        parcel_shp_path = GDRIVE_FOLDER / "GIS" / "parcels" / "parcels.shp"
        db.shp2pgsql(parcel_shp_path, 26918, "parcels")

    # Import project table if not already in db
    if "public.projects" not in db.table_list():
        development_table_path = GDRIVE_FOLDER / "GIS" / "all_devprojects_2021_03_15.csv"
        db.import_tabular_file(development_table_path, "projects")

    print("Joining data")

    query = """
    drop table if exists grouped_projects;
    create table grouped_projects as
        with temptable as (
            select
                project_id,
                parcel_id,
                case when building_type like '%%family%%' then 'Residential'
                    when building_type like '%%etail%%' then 'Retail'
                    else 'Other uses' end as typology,
                case when start_year <= 2045 then 'Model timeline'
                    else 'Post model (2046+)' end as timeline,
                residential_units,
                non_res_sqft
            from projects
            where start_year > 2020
        )
        select
            parcel_id,
            typology,
            timeline,
            sum(residential_units) as res_units,
            sum(non_res_sqft) as non_res_sqft
        from temptable
        group by parcel_id, typology, timeline
    """
    db.execute_via_psycopg2(query)
    db.execute_via_psycopg2(
        "select addgeometrycolumn('grouped_projects', 'geom', 26918, 'POINT', 2)"
    )

    parcel_ids = db.query_via_psycopg2("SELECT parcel_id FROM grouped_projects")

    for pid in tqdm(parcel_ids, total=len(parcel_ids)):

        pid = pid[0]

        if ";" not in pid:
            query = f"""
                update grouped_projects set geom = (
                    select st_centroid(geom) from parcels where primary_id = {pid}
                )
                where parcel_id = '{pid}'
            """
        else:
            query = f"""
                update grouped_projects set geom = (
                    select st_centroid(st_union(geom)) from parcels
                    where primary_id in (select unnest(string_to_array('{pid}', ';'))::int)
                )
                where parcel_id = '{pid}'
            """
        db.execute_via_psycopg2(query)

    print("Exporting to shp")

    # Export the joined data to GoogleDrive as a shapefile
    output_folder = GDRIVE_FOLDER / "GIS" / "parcels_post_join"
    output_shp = output_folder / "grouped_projects.shp"
    db.ogr2ogr_export("grouped_projects", output_shp)

    # Create a version of the data that includes parcels within the study area, as centroids
    study_bounds_shp = (
        GDRIVE_FOLDER
        / "Data"
        / "GIS"
        / "Draft_Study_Area_Extent"
        / "U_CIty_Study_Area_Dissolve_2.shp"
    )
    bounds_gdf = gpd.read_file(study_bounds_shp)
    project_gdf = gpd.read_file(output_shp)

    # Filter parcels to those that intersect the study bounds
    intersection_filter = project_gdf.intersects(bounds_gdf.unary_union)
    filtered_projects = project_gdf[intersection_filter]

    # Save intersecting parcels to shp
    filtered_projects.to_file(output_folder / "study_area_grouped_projects.shp")


if __name__ == "__main__":
    main()
