"""
Aggregate collisions within the study area
"""
import pg_data_etl as pg

from fy21_university_city import GDRIVE_FOLDER


def main(db: pg.Database):

    if "public.pa_crashes" not in db.spatial_table_list():
        shp_filepath = GDRIVE_FOLDER / "Data/GIS/PA Crashes/PA_Crashes.shp"
        db.shp2pgsql(shp_filepath, 4326, "pa_crashes")


if __name__ == "__main__":
    db = pg.Database("ucity", **pg.connections()["localhost"])
    main(db)
