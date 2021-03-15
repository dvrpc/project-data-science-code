"""
process_urbansim_data.py
------------------------

Import regional parcel data and CSV with upcoming development projects.

Join the two tables together, and then save result to database & shapefile.
"""
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

    if "public.study_area" not in db.spatial_table_list():
        study_bounds_shp = (
            GDRIVE_FOLDER
            / "Data"
            / "GIS"
            / "Draft_Study_Area_Extent"
            / "U_CIty_Study_Area_Dissolve_2.shp"
        )
        db.shp2pgsql(study_bounds_shp, 26918, "study_area")

    print("Joining data")

    query = """
    drop table if exists grouped_projects;
    drop table if exists projects_to_map;

    create table projects_to_map as
        select
            project_id,
            name,
            address,
            parcel_id,
            case when building_type like '%%family%%' then 'Residential'
                when building_type like '%%etail%%' then 'Retail'
                else 'Other uses' end as typology,
            case when start_year <= 2035 then 'Short-term'
                when start_year <= 2045 then 'Long-term'
                else 'Post model (2046+)' end as timeline,
            residential_units,
            non_res_sqft
        from projects
        where start_year > 2020;

    create table grouped_projects as
        select
            string_agg(project_id::text, ';') as project_ids,
            string_agg(name, ';') as names,
            string_agg(address, ';') as addresses,
            parcel_id,
            typology,
            timeline,
            sum(residential_units) as res_units,
            sum(non_res_sqft) as non_res_sqft
        from projects_to_map
        group by parcel_id, typology, timeline;
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

    output_folder = GDRIVE_FOLDER / "GIS" / "parcels_post_join"
    output_shp = output_folder / "study_area_grouped_projects.shp"

    q = """
        select *
        from grouped_projects
        where st_within(geom, (select st_collect(geom) from study_area sa))
    """
    output_data = db.query(q, geo=True)
    output_data.gdf.to_file(output_shp)


if __name__ == "__main__":
    main()
