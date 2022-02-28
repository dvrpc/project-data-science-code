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
# dummydata = GDRIVE_FOLDER / "dummydata.csv"
origin_df = pd.read_csv(origins)
destinations_df = pd.read_csv(destinations)
# dummy_df = pd.read_csv(dummydata)
# dummy_df["Distance"] = " "
# dummy_df["Duration"] = " "
destinations_df["Distance"] = " "
destinations_df["Duration"] = " "

# hardcoded here since my origin doesn't change, but if yours does you can tweak the function.
origin = (39.946196, -75.139832)
list_of_dicts = []


def distance_duration(destination):
    """measures distance and duration between two points.
    in this case my origin is always the same so i've defined it separately above.
    appends the returned dictionary to a list of dictionaries."""

    matrix = gmaps.directions(
        origin,
        destination,
        mode="driving",
        units="imperial",
        departure_time=now,
    )
    list_of_dicts.append(matrix)


def distance_duration_iteration():
    """loops through the destinations file and runs the distance duration function"""
    for idx, row in destinations_df.iterrows():
        lat = row["Latitude"]
        lon = row["Longitude"]
        destination = (lat, lon)
        distance_duration(destination)


def unpack_dicts(output):
    """function to crack into the nested dictionary structure that the api returns"""
    labels = ["distance", "duration", "duration_in_traffic", "lat", "lng"]
    df = pd.DataFrame(columns=labels)
    for item in list_of_dicts:
        destination_lat = item[0]["legs"][0]["end_location"]["lat"]
        destination_lng = item[0]["legs"][0]["end_location"]["lng"]
        distance = item[0]["legs"][0]["distance"]["text"]
        duration = item[0]["legs"][0]["duration"]["text"]
        duration_in_traffic = item[0]["legs"][0]["duration_in_traffic"]["text"]
        d = {
            "distance": [distance],
            "duration": [duration],
            "duration_in_traffic": [duration_in_traffic],
            "lat": [destination_lat],
            "lng": [destination_lng],
        }
        list_df = pd.DataFrame(data=d)
        df = pd.concat([list_df, df], axis=0, ignore_index=True)
    return df


def df_to_csv(df):
    df.to_csv(GDRIVE_FOLDER / "travel_times.csv", sep=",")


distance_duration_iteration()
df_to_csv(unpack_dicts(list_of_dicts))


# todo: add target datetime variable instead of just using datetimen now
# add functionality to discern between transit/driving, perhaps return both in one dataframe
# add fare functionality
