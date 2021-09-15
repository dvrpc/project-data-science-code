import requests 
from bs4 import BeautifulSoup
import csv 
import pandas as pd


url_list = []
address_url_list = []

f = csv.writer(open('list_of_locations2.csv', 'a', newline=''))
f.writerow(['location', 'address'])

#function tests if URL with different page number (up to 10) is valid, then adds valid pages to url_list
def urlparser():
    urlbase = "https://visitsouthjersey.com/page/" 
    urlsuffix= "/?post_type=member_org&member_org_categories=attractions&features%5Bcounty%5D=Camden&features%5Bcity%5D"
    
    #could probably do a while loop here, for while response is 200, but need to work on that.
    for page in range(1, 10):
        new_url = urlbase + str(page) + urlsuffix
        if str(requests.get(new_url)) == "<Response [200]>":
            url_list.append(new_url)
urlparser()

for url in url_list:
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    

    for link in soup.find_all('a'):
        l = str(link.get('href'))
        if l.startswith("https://visitsouthjersey.com/member-org/"):
            address_url_list.append(l)
    

location_list = []
address_list = []

for address_url in address_url_list:
    page = requests.get(address_url)
    soup = BeautifulSoup(page.text, 'html.parser')
    address = soup.find(class_='col-xs-9').contents[0]
    address = str(address.string)
    address = ' '.join(address.split())
    address_list.append(address)
    
    location = soup.find(class_='page-header').contents[1]
    location = str(location.contents[0].string)
    location = ' '.join(location.split())
    location_list.append(location)


with open('list_of_locations2.csv', 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(zip(location_list, address_list))







