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


<<<<<<< HEAD
def distance_duration(destination,mode,name):
    driving_list = []
    transit_list = []
    output = gmaps.directions(
        origin,
        destination,
        mode,
        units="imperial",
        departure_time=now,
    )
    names = {"name": name}
    output.insert(0, names)

    if mode == "driving":
        driving_list.append(output)
        return driving_list

    if mode == "transit":
        transit_list.append(output)
        return transit_list
        

def distance_duration_iteration(mode):
    """returns a list of origin/destination dicts for driving or transit, respectively"""
=======
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
        names = {"name": name}
        output.insert(0, names)

        if mode == "driving":
            driving_list.append(output)

        if mode == "transit":
            transit_list.append(output)

    """loops through the destinations file and runs the distance duration function"""
>>>>>>> 9aaaff9d42319a98b4c54f18208a4f84ed04d7e9
    for idx, row in destinations_df.iterrows():
        lat = row["Latitude"]
        lon = row["Longitude"]
        name = row["Name"]
        destination = (lat, lon)
<<<<<<< HEAD
        distance_duration(destination,mode,name)
=======
        distance_duration(destination)

    if mode == "driving":
        print("returning driving list")
        return driving_list
    if mode == "transit":
        print("returning transit list")
        return transit_list


def save_list_in_memory(transpo_list):
    """saves list from distance_duration_iteration into memory to avoid repeated calls to the API"""
    transpo_list = transpo_list
    return transpo_list

>>>>>>> 9aaaff9d42319a98b4c54f18208a4f84ed04d7e9

def unpack_dicts(driving_list, transit_list):
    """function to crack into the nested dictionary structure that the google api returns"""
    df = pd.DataFrame()
    transit_list_of_dicts = []
    driving_list_of_dicts = []

    for item in driving_list:
        destination_lat = item[1]["legs"][0]["end_location"]["lat"]
        destination_lng = item[1]["legs"][0]["end_location"]["lng"]
        distance = item[1]["legs"][0]["distance"]["text"]
        duration = item[1]["legs"][0]["duration"]["text"]
        name = item[0]["name"]
        d = {
            "name": [name],
            "lat": [destination_lat],
            "lng": [destination_lng],
            "d_distance": [distance],
            "d_duration": [duration],
        }
        driving_list_of_dicts.append(d)

    for item in transit_list:
        if len(item) == 2:
            destination_lat = item[1]["legs"][0]["end_location"]["lat"]
            destination_lng = item[1]["legs"][0]["end_location"]["lng"]
            distance = item[1]["legs"][0]["distance"]["text"]
            duration = item[1]["legs"][0]["duration"]["text"]
            name = item[0]["name"]
            d = {
                "t_distance": [distance],
                "t_duration": [duration],
            }
            transit_list_of_dicts.append(d)
        else:
            d = {
                "t_distance": 0,
                "t_duration": 0,
            }
            transit_list_of_dicts.append(d)

    df1 = pd.DataFrame(driving_list_of_dicts)
    df2 = pd.DataFrame(transit_list_of_dicts)
    frames = [df1, df2]
    df = pd.concat(frames, axis=1)
    return df


def df_to_csv(df):
    df.to_csv(GDRIVE_FOLDER / "attraction_travel_times.csv", sep=",")


def unpack_geometries(transpo_list):
    """accepts either the transit list or the driving list from the distance_duration_iteration function and parses accordingly"""
    polylines = []

    if transpo_list == driving_list:
        mode = "driving"
    if transpo_list == transit_list:
        mode = "transit"  # necessary to set mode for f string at end of function

    for item in transpo_list:
        if len(item) == 2:
            overview_line = [
                polyline.decode(item[1]["overview_polyline"]["points"], geojson=True)
            ]
        line = MultiLineString(overview_line)
        polylines.append(line)
    else:
        pass

    df = pd.DataFrame(polylines)
    df.columns = ["strings"]
    gdf = gpd.GeoDataFrame(df, crs="epsg:4326", geometry="strings")
    gdf.to_file(GDRIVE_FOLDER / f"{mode}_polylines.geojson", driver="GeoJSON")


if __name__ == "__main__":
    # holds lists in memory so api isn't repeatedly called  (more useful for jupyter notebook than this script)
<<<<<<< HEAD
    driving_list = distance_duration_iteration("driving")
    transit_list = distance_duration_iteration("transit")
=======
    driving_list = save_list_in_memory(distance_duration_iteration("driving"))
    transit_list = save_list_in_memory(distance_duration_iteration("transit"))
>>>>>>> 9aaaff9d42319a98b4c54f18208a4f84ed04d7e9

    # unpacks dictionaries returned by API
    unpacked_dictionaries = unpack_dicts(driving_list, transit_list)

    # creates csv of distances and durations for points
    df_to_csv(unpacked_dictionaries)

    # unpacks geometries as geojson
    unpack_geometries(driving_list)
    unpack_geometries(transit_list)

<<<<<<< HEAD
    #todo
=======
    # todo: combine dataframes in unpack function so one df is returned with both driving and travel times instead of two csvs
>>>>>>> 9aaaff9d42319a98b4c54f18208a4f84ed04d7e9
    # create function to unpack details of trip rather than just overview line
