import googlemaps
from datetime import datetime
import responses
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

load_dotenv()
api_key = os.getenv("google_api")
gmaps = googlemaps.Client(key=api_key)
GDRIVE_FOLDER = Path(os.getenv("GDRIVE_FOLDER"))

now = datetime.now()

origins = GDRIVE_FOLDER / "origin.csv"
destinations = GDRIVE_FOLDER / "attractions.csv"
origin_df = pd.read_csv(origins)
destinations_df = pd.read_csv(destinations)
print(origin_df.head())
print(destinations_df.head(20))


# responses.add(
#     responses.GET,
#     "https://maps.googleapis.com/maps/api/distancematrix/json",
#     body='{"status":"OK","rows":[]}',
#     status=200,
#     content_type="application/json",
# )




# matrix = gmaps.distance_matrix(origins, destinations, mode="transit",units="imperial", departure_time=now)

# print(matrix)
