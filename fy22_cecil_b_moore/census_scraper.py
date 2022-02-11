##script to pull down census data for target zip codes and clean up
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("CENSUS_KEY")  # put census key from census api in env file
trait = "B02001_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B02001_007E"  # Eventually refactor into a legible dictionary for common queries
zip = "19121"
state = "42"  # PA, swap for 34 if looking at NJ
year = "2019"

trait_list = list(trait.split(","))
url = f"https://api.census.gov/data/{year}/acs/acs5/?get={trait}&for=zip%20code%20tabulation%20area:{zip}&in=state:{state}&key={api_key}"
print(url)
df = pd.read_json(url)
new_header = df.iloc[0]  # grab the first row for the header
df = df[1:]  # take the data less the header row
df.columns = new_header  # set the header row as the df header
vartable = pd.read_json(
    "https://api.census.gov/data/2019/acs/acs5/profile/variables.json"
)  # lookup table
vartable.columns = ["code"]
for t in trait_list:
    print(vartable.code.DP02PR_0001E["label"])

df.head()


vartable = pd.read_json(
    "https://api.census.gov/data/2019/acs/acs5/profile/variables.json"
)
vartable.columns = ["code"]
print(vartable.code.DP02PR_0001E["label"])
vartable.head(20)
