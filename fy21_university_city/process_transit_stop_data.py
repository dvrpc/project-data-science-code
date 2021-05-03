from pathlib import Path

import pg_data_etl as pg

from fy21_university_city.db_io import db_connection

db = pg.Database("ucity", **pg.connections()["localhost"])

GIS_FOLDER = Path(r"U:\FY2021\Corridor\U_City\SHP")
SEPTA_FOLDER = GIS_FOLDER / "SEPTA"
STUDY_AREA_PATH = GIS_FOLDER / "Draft_Study_Area_Extent" / "U_CIty_Study_Area_Dissolve_2.shp"

table_lookup = {
    "afcd4481-572b-43cf-8764-e8af2294633f202041-1-1c875ug.6ov4": "mfl",
    "SEPTA_-_Regional_Rail_Stations": "regional_rail",
    "4f34c6b9-ba39-4c63-9f54-8d44c7320d5c202047-1-1yyct9k.fqq8": "bus",
    "SEPTA_-_Trolley_Stops": "trolley",
}

table_columns_to_query = {
    # tablename: (sum_col, id_col)
    "mfl": ("average_we", "concat(stop_id, '.', route)"),
    "bus": ("wk_b_a", "concat(stop_id, '.', route)"),
    "trolley": ("weekday_to", "concat(stop_id, '.', route)"),
    "regional_rail": ("weekday_to + weekday__3", "concat(stop_id, '.', line_name)"),
}


def _load_data():
    """
    - Load septa station data
    """
    db.create_db()

    if f"public.study_area" not in db.spatial_table_list():
        print(f"Importing study_area")
        db.import_geo_file(STUDY_AREA_PATH, "study_area")

    for shp in SEPTA_FOLDER.rglob("*.shp"):
        if shp.stem in table_lookup:
            sql_tablename = table_lookup[shp.stem]

            if f"public.{sql_tablename}" not in db.spatial_table_list():
                print(f"Importing {sql_tablename}")
                db.import_geo_file(shp, sql_tablename)


def main():

    _load_data()

    for sql_tablename in table_lookup.values():
        print(sql_tablename)

        column_to_sum, id_column = table_columns_to_query[sql_tablename]

        query = f"""
            with clustered_data as (
                select
                    ST_ClusterDBSCAN(st_transform(geom, 26918), eps := 40, minpoints := 1) OVER() AS clst_id,
                    *
                FROM
                    {sql_tablename}
                where
                    st_intersects(
                        st_transform(geom, 26918),
                        (select st_collect(geom) from study_area)
                    )
                order by clst_id
            )
            select
                clst_id,
                array_agg({id_column}) as stops,
                sum({column_to_sum}) as trips,
                st_centroid(st_collect(geom)) as geom
            from clustered_data
            group by clst_id
        """

        transit_stops_in_study_area = db.query(query, geo=True).gdf

        transit_stops_in_study_area["stops"] = transit_stops_in_study_area["stops"].astype(str)

        output_filepath = SEPTA_FOLDER / f"clustered_{sql_tablename}_stops.shp"
        transit_stops_in_study_area.to_file(output_filepath)


if __name__ == "__main__":
    main()
