import os
import geopandas as gpd
from pg_data_etl import Database
from dotenv import load_dotenv
from pandas_profiling import ProfileReport

from fy22_delco_trails.sql_queries import (
    parks,
    ipd,
    employment,
    destinations,
    septa,
    population,
)


def add_data_to_df(df: gpd.GeoDataFrame, idx: int, colname: str, value) -> None:
    """Set a value to a specific row / column, and add the column first if it doesn't exist"""

    colname = colname.lower().replace(" ", "").replace(",", "_").replace("-", "_")

    if colname not in df.columns:
        df[colname] = None

    df.at[idx, colname] = value

    return None


load_dotenv()
LOCAL_ANALYSIS_DATABASE_URL = os.getenv("LOCAL_ANALYSIS_DATABASE_URL")
db = Database.from_uri(LOCAL_ANALYSIS_DATABASE_URL)


def main():
    gdf = db.gdf("SELECT original_gid as uid, votes, geom FROM votes_from_webapp")

    for idx, row in gdf.iterrows():
        uid = str(row.uid)
        print(f"Analyzing ID # {uid}")

        pop_result = db.query_as_singleton(population.replace("OBJECTID_PLACEHOLDER", uid))
        add_data_to_df(gdf, idx, "population", pop_result)

        nearest_park_dist = db.query_as_singleton(parks.replace("OBJECTID_PLACEHOLDER", uid))
        add_data_to_df(gdf, idx, "park_dist", nearest_park_dist)

        ipd_class = db.query_as_singleton(ipd.replace("OBJECTID_PLACEHOLDER", uid))
        add_data_to_df(gdf, idx, "ipd_class", ipd_class)

        emp_numbers = db.query_as_list_of_lists(employment.replace("OBJECTID_PLACEHOLDER", uid))[0]
        add_data_to_df(gdf, idx, "num_employers", emp_numbers[0])
        add_data_to_df(gdf, idx, "num_jobs", float(emp_numbers[1]))

        poi_numbers = db.query_as_list_of_lists(destinations.replace("OBJECTID_PLACEHOLDER", uid))
        add_data_to_df(gdf, idx, "number_of_destination_types", len(poi_numbers))
        for placetype in poi_numbers:
            add_data_to_df(gdf, idx, f"places_{placetype[1]}", placetype[0])

        transit_numbers = db.query_as_list_of_lists(septa.replace("OBJECTID_PLACEHOLDER", uid))
        add_data_to_df(gdf, idx, "number_of_transit_routes", len(transit_numbers))
        for result in transit_numbers:
            add_data_to_df(gdf, idx, f"stops_{result[1]}", result[0])

    # Write spatial result to disk
    gdf = gdf.to_crs(4326)
    gdf.to_file("results.geojson", driver="GeoJSON")

    # Make a pandas profile report
    df = gdf.drop(columns=["geom"])
    profile = ProfileReport(df, title="Trailhead Profiling Report", explorative=True)
    profile.to_file("trailhead_profile.html")


if __name__ == "__main__":
    main()
