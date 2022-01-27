##Run all sql scripts in order to produce necessary tables 

import os
import psycopg2
from config import config
from config import config2
#create database (i did cbm_v3 for this run)
#create output schema

#load up zip code shapefile
os.system("shp2pgsql -s 4326 -I /Users/markmorley/Downloads/tl_Pennsylvania5Digit2009/tl_Pennsylvania5Digit2009.shp outputs.zips | psql -U postgres -h localhost cbm_v3")

#download and edit in person csv, delete two header rows and column s (blank), delete ssecond id, ip address, and created on columns
#load up in-person data with gui (right click output schema, import, add most recent csv after cleanup)

commands = (
    """DROP TABLE IF EXISTS calendar;""",
    """CREATE TABLE calendar (
    service_id INT PRIMARY KEY,
    monday INT,
    tuesday INT,
    wednesday INT,
    thursday INT,
    friday INT,
    saturday INT,
    sunday INT,
    start_date VARCHAR,	
    end_date VARCHAR);""",
    """DROP TABLE IF EXISTS calendar_dates;""",
    """CREATE TABLE `calendar_dates` (
    service_id INT,
    `date` VARCHAR,
    exception_type INT,
    PRIMARY KEY (service_id, exception_type));""",
    """DROP TABLE IF EXISTS routes;""",
    """CREATE TABLE `routes` (
    route_id INT PRIMARY KEY,
    agency_id INT,
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255),
    route_type INT(2),
    KEY `agency_id` (agency_id),
    KEY `route_type` (route_type));""",
)



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


