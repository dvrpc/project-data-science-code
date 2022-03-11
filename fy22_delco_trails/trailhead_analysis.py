import os
from pg_data_etl import Database
from dotenv import load_dotenv

from fy22_delco_trails.sql_queries import (
    parks,
    ipd,
    employment,
    destinations,
    septa,
    population,
)

load_dotenv()
ANALYSIS_DATABASE_URL = os.getenv("ANALYSIS_DATABASE_URL")
db = Database.from_uri(ANALYSIS_DATABASE_URL)

all_ids = db.gdf("SELECT original_gid as uid, geom FROM votes_from_webapp")

ids = [105]

for idx, row in all_ids.iterrows():
    uid = str(row.uid)
    print(f"uid: {uid} ---------------------")

    nearest_park_dist = db.query_as_singleton(parks.replace("OBJECTID_PLACEHOLDER", uid))
    print("Nearest park is", nearest_park_dist, "meters away")

    print(parks.replace("OBJECTID_PLACEHOLDER", uid))

    ipd_class = db.query_as_singleton(ipd.replace("OBJECTID_PLACEHOLDER", uid))
    print("IPD class is", ipd_class)

    emp_numbers = db.query_as_list_of_lists(employment.replace("OBJECTID_PLACEHOLDER", uid))[0]
    print(emp_numbers[0], "employers have a total of", emp_numbers[1], "employees")

    poi_numbers = db.query_as_list_of_lists(destinations.replace("OBJECTID_PLACEHOLDER", uid))
    for placetype in poi_numbers:
        print(placetype[0], "place of type", placetype[1], "exists")

    transit_numbers = db.query_as_list_of_lists(septa.replace("OBJECTID_PLACEHOLDER", uid))
    for result in transit_numbers:
        print(result[0], "stops of line", result[1], "exists")

    pop_result = db.query_as_singleton(population.replace("OBJECTID_PLACEHOLDER", uid))
    print("Population within 2mile network buffer is", pop_result)


if __name__ == "__main__":
    pass
