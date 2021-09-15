import requests
import csv
from pandas import *

GOOGLE_API_KEY = 'your api key from google dev console goes here'

#your csv, which at minimum should have an address column. 
csv_with_addresses = 'all_locations.csv'

#blank csv, you create this yourself beforehand. might amend code so it autogenerates. 
coordinates = 'coordinates.csv'

#open csv and put addresses into list
data = read_csv(csv_with_addresses)
csv_address_column_as_list = data['Address'].tolist()

#function that returns lat & long using google's api. passes in your address list created above
def extract_lat_long_via_address(address_or_zipcode):
    for a in address_or_zipcode:        
        lat, lng = None, None
        api_key = GOOGLE_API_KEY
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        endpoint = f"{base_url}?address={address_or_zipcode}&key={api_key}"
        # see how our endpoint includes our API key? Yes this is yet another reason to restrict the key
        r = requests.get(endpoint)
        if r.status_code not in range(200, 299):
            return None, None
        try:
            results = r.json()['results'][0]
            lat = results['geometry']['location']['lat']
            lng = results['geometry']['location']['lng']
        except:
            pass
        return lat, lng            
    
for address in csv_address_column_as_list:     
    extract_lat_long_via_address(address) 
    coords = extract_lat_long_via_address(address)
    
    with open("coordinates.csv", 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(coords)

