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

    query = """
        with all_transit_stops as (
            (select stop_id, 'mfl' as mode, sum(average_we) as trips, geom from mfl group by stop_id, geom order by stop_id)
            union
            (select stop_id, 'bus' as mode, sum(wk_b_a) as trips, geom from bus group by stop_id, geom order by stop_id)
            union
            (select stop_id, 'trolley' as mode, sum(weekday_to) as trips, geom from trolley group by stop_id, geom order by stop_id)
            union
            (select stop_id, 'regional rail' as mode, sum(weekday_to + weekday__3) as trips, geom from regional_rail group by stop_id, geom order by stop_id)
        )
        select * from all_transit_stops
        where st_intersects(st_transform(geom, 26918), (select st_collect(geom) from study_area))
    """

    transit_stops_in_study_area = db.query(query, geo=True)

    output_filepath = SEPTA_FOLDER / "merged_stops.shp"
    transit_stops_in_study_area.gdf.to_file(output_filepath)


if __name__ == "__main__":
    main()
