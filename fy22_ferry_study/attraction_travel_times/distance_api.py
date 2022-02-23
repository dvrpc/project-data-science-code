import googlemaps
from datetime import datetime
import responses
import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv('google_api') 
gmaps = googlemaps.Client(key = api_key)

# Request directions via public transit
now = datetime.now()

responses.add(
    responses.GET,
    "https://maps.googleapis.com/maps/api/distancematrix/json",
    body='{"status":"OK","rows":[]}',
    status=200,
    content_type="application/json",
)

origins = [
    "Perth, Australia",
    "Sydney, Australia",
    "Melbourne, Australia",
    "Adelaide, Australia",
    "Brisbane, Australia",
    "Darwin, Australia",
    "Hobart, Australia",
    "Canberra, Australia",
]
destinations = [
    "Uluru, Australia",
    "Kakadu, Australia",
    "Blue Mountains, Australia",
    "Bungle Bungles, Australia",
    "The Pinnacles, Australia",
]

matrix = gmaps.distance_matrix(origins, destinations, mode="transit",units="imperial", departure_time=now)

print(matrix)