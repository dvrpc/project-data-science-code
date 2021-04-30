import pg_data_etl as pg

from fy21_university_city import GDRIVE_FOLDER
from fy21_university_city.db_io import db_connection
from fy21_university_city.process_bus_data import prep_data

db = pg.Database("ucity", **pg.connections()["localhost"])

GIS_FOLDER = GDRIVE_FOLDER / "Data/GIS"


def _load_data():
    """
    - Load crash points and tabular FLAG data
    """

    if "public.philly_crashes" not in db.spatial_table_list():
        filepath = GIS_FOLDER / "philly_crashes.geojson"
        db.import_geo_file(filepath, "philly_crashes")

    if "public.pa_crash_flags" not in db.table_list():
        filepath = GIS_FOLDER / "crash_pa_flag_202104291218.csv"
        db.import_tabular_file(filepath, "philly_crash_flags")


def main():
    """
    - Load data into SQL
    - Query the points/attributes relevant to this project
    - Save result to shapefile
    """

    _load_data()

    query = """
        select
            c.crn,
            c.crash_year,
            b.study_area as ucity_zone,
            f.fatal_or_maj_inj as ksi,
            f.bicycle as bike,
            f.pedestrian as ped,
            f.non_interstate,
            c.geom 
        from philly_crashes c
        left join
            philly_crash_flags f
            on f.crn = c.crn
        right join
            study_bounds b 
            on st_intersects(
                st_transform(c.geom, 26918),
                b.geom
            )
        where c.crash_year != '2014'
    """
    db.make_geotable_from_query(query, "study_area_crashes", "POINT", 4326)

    output_filepath = GIS_FOLDER / "study_area_crashes_with_flags.shp"

    db.ogr2ogr_export("study_area_crashes", output_filepath)


if __name__ == "__main__":
    main()
