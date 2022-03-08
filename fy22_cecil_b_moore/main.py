##Run all sql scripts in order to produce necessary tables 

import os
import psycopg2
from config import config
from config import config2
#create database (i did cbm_v3 for this run)
#create output schema

#load up zip code shapefile
os.system("shp2pgsql -s 4326 -I /Users/markmorley/Downloads/tl_Pennsylvania5Digit2009/tl_Pennsylvania5Digit2009.shp public.zips | psql -U postgres -h localhost cbm_final")


# psql
# Create database cbm_v3;
# #quit psql
# pg_restore --dbname=cbm_final -c -O --no-acl /Users/markmorley/Downloads/cbm_map-2022-02-02.tar

# psql -c "\copy outputs.in_person_survey_response_data_entry FROM ‘/Users/markmorley/Downloads/In-Person Survey Response data entry.csv’ delimiter ‘,’ csv header"

#download and edit in person csv, delete two header rows and column s (blank), delete ssecond id, ip address, and created on columns
#load up in-person data with gui (right click output schema, import, add most recent csv after cleanup)

# commands = (
#     """ALTER TABLE pins_mapuser 
#     ADD COLUMN q5_gender varchar;

#     UPDATE pins_mapuser SET q5_gender = 
#     CASE WHEN lower(q5) in ('female', 'f') then 'Female'
#     WHEN lower(q5) in ('male', 'cisgender male') then 'Male' END;""",
# )



#qaqc mapuser table



#join_longform to user to create "longform online" table

#join_online_in_peron joins together in person and online surveys using  the previous table so that demographics can be analyzed 

#count zips from joined

#conditions sum 

#pin demographics

#pin zip

#pin tag analysis

#race breakdown

#student nonstudent priorities



#zips with counts 


