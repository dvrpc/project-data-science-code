import pandas as pd

# from config import GDRIVE_FOLDER, db

from fy21_university_city.db_io import db_connection


def main():

    db = db_connection()

    # Get a clean table with all non-calculated attributes
    query = """
        select route_id, stop_id, stop_name, stop_lat, stop_lon
        from apc_raw
    """
    df = db.query_as_df(query)

    print(df)