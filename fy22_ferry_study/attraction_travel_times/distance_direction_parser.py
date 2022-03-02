import googlemaps
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

load_dotenv()
api_key = os.getenv("google_api")
gmaps = googlemaps.Client(key=api_key)
GDRIVE_FOLDER = Path(os.getenv("GDRIVE_FOLDER"))
origins = GDRIVE_FOLDER / "origin.csv"
destinations = GDRIVE_FOLDER / "attractions.csv"
origin_df = pd.read_csv(origins)
destinations_df = pd.read_csv(destinations)
now = datetime.now()


origin = (
    39.946196,
    -75.139832,
)  # hardcoded here since my origin doesn't change, but if yours does you can tweak the function to accept origins.


def distance_duration_iteration():
    '''returns a list of origin/destination dicts for driving or transit, respectively'''
    driving_list = []
    transit_list = []

    def distance_duration(destination):
        output = gmaps.directions(
            origin,
            destination,
            mode,
            units="imperial",
            departure_time=now,
        )

        if mode == "driving":
            driving_list.append(output)

        if mode == "transit":
            transit_list.append(output)

    """loops through the destinations file and runs the distance duration function"""
    for idx, row in destinations_df.iterrows():
        lat = row["Latitude"]
        lon = row["Longitude"]
        destination = (lat, lon)
        distance_duration(destination)

    if mode == "driving":
        print("returning driving list")
        return driving_list
    if mode == "transit":
        print("returning transit list")
        return transit_list


def unpack_dicts(list_of_dicts):
    """function to crack into the nested dictionary structure that the google api returns"""
    labels = ["distance", "duration", "lat", "lng"]
    df = pd.DataFrame(columns=labels)

    try:
        for item in list_of_dicts:
            destination_lat = item[0]["legs"][0]["end_location"]["lat"]
            destination_lng = item[0]["legs"][0]["end_location"]["lng"]
            distance = item[0]["legs"][0]["distance"]["text"]
            duration = item[0]["legs"][0]["duration"]["text"]
            d = {
                "distance": [distance],
                "duration": [duration],
                "lat": [destination_lat],
                "lng": [destination_lng],
            }
            list_df = pd.DataFrame(data=d)
            df = pd.concat([list_df, df], axis=0, ignore_index=True)
    except:
        for item in list_of_dicts[0]:
            destination_lat = item["legs"][0]["end_location"]["lat"]
            destination_lng = item["legs"][0]["end_location"]["lng"]
            distance = item["legs"][0]["distance"]["text"]
            duration = item["legs"][0]["duration"]["text"]
            d = {
                "distance": [distance],
                "duration": [duration],
                "lat": [destination_lat],
                "lng": [destination_lng],
            }
            list_df = pd.DataFrame(data=d)
            df = pd.concat([list_df, df], axis=0, ignore_index=True)

    return df


def df_to_csv(df):
    df.to_csv(GDRIVE_FOLDER / f"{mode}_travel_times.csv", sep=",")


if __name__ == "__main__":
    # mode = "driving"  # accepts driving, walking, transit, cycling
    # unpacked_driving = unpack_dicts(distance_duration_iteration())
    # df_to_csv(unpacked_driving)
    mode = "transit"  # change mode to transit and rerun
    unpacked_transit = unpack_dicts(distance_duration_iteration())
    df_to_csv(unpacked_transit)
