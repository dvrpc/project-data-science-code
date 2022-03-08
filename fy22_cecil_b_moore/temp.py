import psycopg2
from config import config
from config import config2

# import os
# import pandas
# import tempfile

gtfs_filepath = (
    "http://www.ridepatco.org/developers/PortAuthorityTransitCorporation.zip"
)


def main():

    # connect to config file, which points to postgres database initially then to gtfs database later using config2
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""DROP DATABASE IF EXISTS gtfs;""")
    cur.execute("""CREATE DATABASE gtfs;""")
    cur.close()
    conn.commit()
    conn.close()

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

    # """
    # CREATE TABLE vendors (
    #     vendor_id SERIAL PRIMARY KEY,
    #     vendor_name VARCHAR(255) NOT NULL
    # )
    # """,
    conn = None

    try:
        # read the connection parameters
        params = config2()
        # connect to the PostgreSQL server with the config 2 settings, which point to the gtfs database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
