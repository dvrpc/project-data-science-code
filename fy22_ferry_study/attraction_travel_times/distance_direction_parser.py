import googlemaps
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from shapely.geometry import MultiLineString
import polyline
import geopandas as gpd

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


def distance_duration_iteration(mode):
    """returns a list of origin/destination dicts for driving or transit, respectively"""
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


def save_list_in_memory(transpo_list):
    '''saves list from distance_duration_iteration into memory to avoid repeated calls to the API'''
    transpo_list = transpo_list
    return transpo_list

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


def df_to_csv(df, mode):
    df.to_csv(GDRIVE_FOLDER / f"{mode}_travel_times.csv", sep=",")

def unpack_geometries(transpo_list):
    """accepts either the transit list or the driving list from the distance_duration_iteration function and parses accordingly"""
    polylines = []

    if transpo_list == driving_list: 
        mode = "driving"
    if transpo_list == transit_list:
        mode = "transit" # necessary to set mode for f string at end of function

    try:
        for row in transpo_list:
            if not item:
                pass  # just appending blank items here if there's no polyline
            else:
                print(item[0])
                coords = [
                    polyline.decode(
                        item[0]["overview_polyline"]["points"], geojson=True
                    )
                ]
                line = MultiLineString(coords)
                polylines.append(line)
    except:
        for item in transpo_list:
            if not item:
                pass  # just appending blank items here if there's no polyline
            else:
                coords = [
                    polyline.decode(
                        item[0]["overview_polyline"]["points"], geojson=True
                    )
                ]
                line = MultiLineString(coords)
                polylines.append(line)

    df = pd.DataFrame(polylines)

    print(df)

    df.columns = ["strings"]
    gdf = gpd.GeoDataFrame(df, crs="epsg:4326", geometry="strings")
    gdf.to_file(GDRIVE_FOLDER / f"{mode}_polylines.geojson", driver="GeoJSON")

if __name__ == "__main__":
    #holds lists in memory so api isn't repeatedly called 
    driving_list = save_list_in_memory(distance_duration_iteration("driving"))
    transit_list = save_list_in_memory(distance_duration_iteration("transit"))
    #runs all functions for driving times/durations/geometries
    unpacked_driving = unpack_dicts(distance_duration_iteration("driving"))
    df_to_csv(unpacked_driving, "driving")
    unpack_geometries(driving_list)
    #runs the same for transit
    unpacked_transit = unpack_dicts(distance_duration_iteration("transit"))
    df_to_csv(unpacked_transit, "transit")
    unpack_geometries(transit_list)
    