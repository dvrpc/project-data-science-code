##script to pull down census data for target zip codes and clean up
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("CENSUS_KEY")  # put census key from census api in env file

#boilerplate census api references
race = "B02001_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B02001_008E" #Eventually refactor into a legible dictionary for common queries
PA = "42" 
NJ = "34"

def census_puller(trait, zip, state, year):
    """Pulls ACS 5 year estimates from Census API for a given trait, zip code, state, and year."""
    trait_list = list(trait.split(","))
    url = f"https://api.census.gov/data/{year}/acs/acs5/?get={trait}&for=zip%20code%20tabulation%20area:{zip}&in=state:{state}&key={api_key}"
    print(url)
    df = pd.read_json(url)
    new_header = df.iloc[0]  # grab the first row for the header
    df = df[1:]  # take the data less the header row
    df.columns = new_header  # set the header row as the df header
    var_lookup = pd.read_json(
        f"https://api.census.gov/data/{year}/acs/acs5/variables.json"
    )  # lookup table to make headers legible by referencing data dictionary
    var_lookup.columns = ["code"]
    for item in trait_list:
        if item in var_lookup.code:
            raw_col = var_lookup.code[item].get('label')
            clean_col = raw_col.replace("Estimate!!Total:!!", "")
            df.rename(columns= {item : clean_col}, inplace=True)
    return df.head(20)

census_puller(race, 19121, PA, 2019)




