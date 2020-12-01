import pandas as pd

from fy21_university_city.db_io import db_connection


def main():

    db = db_connection()

    # Get a clean table with all non-calculated attributes
    query = """
        select
            route_id,
            stop_id,
            stop_name,
            st_setsrid(st_point(stop_lon, stop_lat), 4326) as geom
        from
            apc_raw
        where
            stop_lat is not null
          and
            stop_lon is not null
        group by
            route_id, stop_id, stop_name, stop_lat, stop_lon
    """
    gdf = db.query_as_geo_df(query)

    print(gdf)

    time_series = [
        ("am", [6, 10]),
        ("pm", [3, 7]),
        ("allday", [0, 25]),
    ]

    for timename, bookends in time_series:

        start_hr = bookends[0]
        end_hr = bookends[1]

        query = f"""
            select
                route_id,
                stop_id,
                sum(hourly_ons) as {timename}_ons,
                sum(hourly_offs) as {timename}_offs
            from
                apc_raw
            where
                hour_bin >= {start_hr}
              and
                hour_bin < {end_hr}
            group by
                route_id,
                stop_id
        """

        new_df = db.query_as_df(query)

        gdf = pd.merge(gdf, new_df, how="left", on=["route_id", "stop_id"])

    # Save the resulting geodataframe back to the database
    db.import_geodataframe(gdf, "apc_processed")

    # Add a column to flag stops in the main + secondary study areas
    db.table_add_or_nullify_column("apc_processed", "ucity", "TEXT")

    update_query = """
            UPDATE apc_processed SET ucity = CASE
                WHEN st_within(geom, (select st_transform(geom, 4326) from study_bounds where study_area = 1)) THEN 'main study area'
                WHEN st_within(geom, (select st_transform(geom, 4326) from study_bounds where study_area = 2)) THEN 'secondary study area' END
    """
    db.execute(update_query)
