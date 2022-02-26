import googlemaps
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import json

load_dotenv()
api_key = os.getenv("google_api")
gmaps = googlemaps.Client(key=api_key)
GDRIVE_FOLDER = Path(os.getenv("GDRIVE_FOLDER"))

now = datetime.now()

origins = GDRIVE_FOLDER / "origin.csv"
destinations = GDRIVE_FOLDER / "attractions.csv"
dummydata = GDRIVE_FOLDER / "dummydata.csv"
origin_df = pd.read_csv(origins)
destinations_df = pd.read_csv(destinations)
dummy_df = pd.read_csv(dummydata)
dummy_df["Distance"] = " "
dummy_df["Duration"] = " "
destinations_df["Distance"] = " "
destinations_df["Duration"] = " "

# hardcoded here since my origin doesn't change, but if yours does you can tweak the function.
origin = (39.946196, -75.139832)
list_of_dicts = []


def distance_duration(destination):
    """measures distance and duration between two points.
    in this case my origin is always the same so i've defined it separately above.
    appends the returned dictionary to a list of dictionaries."""

    matrix = gmaps.distance_matrix(
        origin,
        destination,
        mode="driving",
        units="imperial",
        departure_time=now,
    )
    list_of_dicts.append(matrix)


def distance_duration_iteration():
    """loops through the destinations file and runs the distance duration function"""
    for idx, row in dummy_df.iterrows():
        lat = row["Latitude"]
        lon = row["Longitude"]
        destination = (lat, lon)
        distance_duration(destination)


def unpack_dicts(list_of_dicts):
    """function to crack into the nested dictionary structure that the api returns"""
    df = pd.DataFrame()
    df["Address"] = ""
    current_index = 0
    for dict in list_of_dicts:
        destinations = pd.Series(json.dumps(dict["destination_addresses"][0]))
        json_object = json.dumps(dict["rows"][0].get("elements")[0])
        df_dict = pd.read_json(json_object)
        df_dict = df_dict.drop(labels=["value"], axis=0)
        df = pd.concat([df, df_dict], ignore_index=True, axis=0)
        df.at[current_index, "Address"] = destinations[0]
        current_index += 1

    return df


unpack_dicts(list_of_dicts)

distance_duration_iteration()
print(list_of_dicts)


# todo: add target datetime variable instead of just using datetimen now
# add functionality to discern between transit/driving, perhaps return both in one dataframe
# add fare functionality
