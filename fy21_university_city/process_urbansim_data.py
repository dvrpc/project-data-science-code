"""
process_urbansim_data.py
------------------------

Import regional parcel data and CSV with upcoming development projects.

TODO: export parcels joined with developments, clipped to a buffer around a point
"""

import pg_data_etl as pg

from fy21_university_city import GDRIVE_FOLDER

if __name__ == "__main__":
    db = pg.Database("ucity_urbansim", **pg.connections["localhost"])
    db.create_db()

    parcel_shp_path = GDRIVE_FOLDER / "GIS" / "parcels" / "parcels.shp"

    development_table_path = GDRIVE_FOLDER / "GIS" / "all_devprojects_2021_03_02.csv"

    db.shp2pgsql(parcel_shp_path, 26918, "parcels")

    db.import_tabular_file(development_table_path, "projects")
