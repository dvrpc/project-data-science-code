"""
process_hts_data.py
-------------------

Use the 2012-13 Household Travel Survey data
to summarize flows between CPA geographies.

This is done by:
    - Reading the raw HTS table from SQL
    - Creating a 'TripTable' object, which has a 'Trip' object
      for every row of the raw data
    - Associating each trip with the next linked trip,
      if there is one
    - Classifying each trip as AM, PM, or 24hr period
      using the trip's departure time and the next trip's
      arrival time
    - Summarizing flows between each CPA, with a column
      for each time period's flows

Results of each step in the process are saved to the SQL database.
"""

import pandas as pd
from datetime import time

from fy21_university_city.db_io import db_connection


mode_codes = {
    1:  "Walk",
    2:  "Bike",
    3:  "Private Vehicle",
    4:  "Private Transit",
    5:  "Public Transit",
    6:  "School Bus",
    7:  "Other",
    None: "not provided"
}


def time_is_between(the_time: time, start_hr: int, end_hr: int) -> bool:
    """ Return True if 'the_time' is between a given start and end hour """
    return time(start_hr) <= the_time < time(end_hr)


def text_to_time(txt: str) -> time:
    """ Convert '12:34' into time(12, 34) """
    hour, minute = txt.split(":")
    return time(int(hour), int(minute))


class Trip:
    """ The 'Trip' class abstracts a single trip:

            - It contains a selected set of columns for each row in
              the raw table named 'hts_2013_trips'

            - If the departure time is usable, the time window of
              the trip gets classified using 'Trip.time_window()'

            - If a 'next_trip' is assigned, the 'Trip.time_window()'
              function will also consider the end time of the trip

            - For a trip to be classified as 'AM' or 'PM', the trip's
              departure time or trip end time (or both) must be between
              the period bookends.
                -> 'AM' is 7 to 10am
                -> 'PM' is 3 to 7pm
    """

    def __init__(self, sql_row: tuple):
        """ Extract attributes from the SQL row.
            Set a None placeholder for the next linked trip
        """

        # Capture the raw data
        self.person_id, self.trip_num, self.act_dur1, \
            self.o_cpa, self.d_cpa, self.mode_agg, self.arrive, \
            self.depart, self.compositeweight = sql_row

        # Make a placeholder for the next linked trip
        self.next_trip = None

        # Replace the mode code with readable text
        self.mode_agg = mode_codes[self.mode_agg]

    @property
    def identifier(self):
        """ ID is person's ID and the trip sequence number """
        return f"{self.person_id}_{self.trip_num}"

    def __repr__(self):
        """ This is what the class looks like when you print() it """
        return f"{self.person_id} trip #{self.trip_num}, {self.depart}"

    def time_is_usable(self, which="depart"):
        """ Return True if the time is formatted like 'hh:mm' """

        if which == "depart":
            txt = self.depart
        elif which == "arrive":
            txt = self.arrive

        if not txt:
            return False
        elif ":" not in txt:
            return False
        else:
            return True

    def trip_end_time(self):
        """ Return the arrival time of the next trip.
            If a next trip has not been assigned, return None
        """

        if self.next_trip:
            return self.next_trip.arrive
        else:
            return None

    def time_window(self):
        """ Using the departure time and the next trip's arrival time,
            identify whether the trip overlaps the AM and/or PM windows.

            All trips also get classified as '24 HR', regardless of whether
            or not the time value is usable. (Many are NULL, or 9999/9997/etc.)
        """

        times_to_check = []

        # Check the departure time if it's usable
        if self.time_is_usable(which="depart"):
            trip_start_time = text_to_time(self.depart)
            times_to_check.append(trip_start_time)

        # If there's a next_trip, check its arrival time (if it's usable)
        if self.next_trip:
            if self.next_trip.time_is_usable(which="arrive"):
                trip_end_time = text_to_time(self.next_trip.arrive)
                times_to_check.append(trip_end_time)

        # Check all usable times and add the classification if
        # the time is between 6-10 and/or 15-19
        windows = ['24 HR']
        for t in times_to_check:
            if time_is_between(t, 6, 10):
                windows.append("AM period (6-10)")

            if time_is_between(t, 15, 19):
                windows.append("PM period (3-7)")

        return str(set(windows))

    def raw_data(self):
        """ Return all of the attributes passed into the object as a list """
        return [self.person_id, self.trip_num, self.act_dur1,
                self.o_cpa, self.d_cpa, self.mode_agg, self.arrive,
                self.depart, self.compositeweight]

    def output_data(self):
        """ Append the raw data with the window classification
            and next trip arrival time
        """
        data = self.raw_data() + [self.trip_end_time(), self.time_window()]
        return data


class TripTable:
    """ The 'TripTable' class abstracts the entire trip dataset.

        A list of tuples is passed into the class. Using this data,
        a 'Trip' object is created for each tuple (i.e. row).

        For each person_id, all trip_num values are collected,
        and each trip gets associated with its next trip, if it exists.

        This is done via the 'next_trip' attribute in the 'Trip' class:

        >>> t1 = Trip(...)
        >>> t2 = Trip(...)
        >>> t1.next_trip = t2

    """

    def __init__(self, list_of_rows: list):
        """ Read the raw data into 'Trip' objects,
            for each Trip figure what the next linked trip is,
            and associate it back to the Trip.
        """

        # For each tuple in the list_of_rows, make a 'Trip' object
        # and then save it into a dict keyed on its identifier
        self.trips = {}
        for row in list_of_rows:
            trip = Trip(row)
            self.trips[trip.identifier] = trip

        # Get a list of each trip number, keyed on person id
        self.person_trips = {}
        for trip_id in self.trips:

            trip = self.trips[trip_id]

            if trip.person_id not in self.person_trips:
                self.person_trips[trip.person_id] = []

            self.person_trips[trip.person_id].append(trip.trip_num)

        # Assign "next" trip to each person's set of linked trips
        for person in self.person_trips:
            trip_list = self.person_trips[person]

            number_of_trips = len(trip_list)

            if number_of_trips > 1:
                for idx, trip_num in enumerate(trip_list):

                    if idx + 1 < number_of_trips:
                        this_trip = self.trips[f"{person}_{trip_num}"]

                        next_trip_num = trip_list[idx + 1]
                        next_trip_id = f"{person}_{next_trip_num}"

                        this_trip.next_trip = self.trips[next_trip_id]

    def new_table(self) -> pd.DataFrame:
        """ Extract the time window and next trip arrival time for each row,
            and return as a dataframe.
        """
        header = ["person_id", "trip_num", "act_dur1",
                  "o_cpa", "d_cpa", "mode_agg", "arrive",
                  "depart", "compositeweight", "trip_end_time",
                  "time_window"]

        data = []

        for trip_id in self.trips:
            t = self.trips[trip_id]
            row = t.output_data()
            data.append(row)

        df = pd.DataFrame(data, columns=header)
        return df


def process_hts():
    """ Process the trip table by finding the next linked trip
        and classifying whether the trip falls into the AM or PM
        peak periods.

        The resulting table is saved back to SQL named 'hts_2013_processed'
    """

    db = db_connection()

    # Get a basic table out of SQL
    query = """
        select
            person_id, trip_num, act_dur1,
            o_cpa, d_cpa, mode_agg, arrive,
            depart, compositeweight
        from
            hts_2013_trips
        order by
            person_id, trip_num
    """
    result = db.query_as_list(query)

    # Read the SQL result into the TripTable object
    trip_table = TripTable(result)

    # Extract the updated trip table
    df = trip_table.new_table()

    # Save the table back to the database
    db.import_dataframe(df, "hts_2013_processed", if_exists="replace")


def aggregate_hts(style="all_modes_combined"):
    """ Use the 'processed' version of the HTS table to summarize the flows.

        Using the 'style' parameter, you can:
            - aggregate by mode using 'by_mode'
            - aggregate without considering mode,
              using the default 'all_modes_combined'
    """

    def _use_the_right_query(style: str, query: str) -> str:
        """ Add 'mode_agg' into the query if the 'style' is 'by_mode' """
        if style == "by_mode":
            return query.replace("o_cpa, d_cpa", "o_cpa, d_cpa, mode_agg")
        else:
            return query

    db = db_connection()

    all_combos_query = """
        select
            o_cpa, d_cpa,
            count(*) as numtrips_24hr,
            sum(compositeweight) as sum_24hr
        from hts_2013_processed
        where trip_num < 97
        group by o_cpa, d_cpa
        order by sum(compositeweight) desc
    """

    am_query = """
        select
            o_cpa, d_cpa,
            count(*) as numtrips_am,
            sum(compositeweight) as sum_am
        from hts_2013_processed
        where
            trip_num < 97
        and
            time_window like '%%AM%%'
        group by o_cpa, d_cpa
    """

    pm_query = """
        select
            o_cpa, d_cpa,
            count(*) as numtrips_pm,
            sum(compositeweight) as sum_pm
        from hts_2013_processed
        where
            trip_num < 97
        and
            time_window like '%%PM%%'
        group by o_cpa, d_cpa
    """

    # Add the 'mode_agg' column if the 'style' is 'by_mode'
    all_combos_query = _use_the_right_query(style, all_combos_query)
    am_query = _use_the_right_query(style, am_query)
    pm_query = _use_the_right_query(style, pm_query)

    # Also, join on the 'mode_agg' column if we're analyzing 'by_mode'
    join_cols = ["o_cpa", "d_cpa"]
    if style == "by_mode":
        join_cols.append("mode_agg")

    # Get the 24-hour totals
    df = db.query_as_df(all_combos_query)

    # Query and join the AM trips
    df_am = db.query_as_df(am_query)
    df = pd.merge(df, df_am, how="left", on=join_cols)

    # Repeat for the PM trips
    df_pm = db.query_as_df(pm_query)
    df = pd.merge(df, df_pm, how="left", on=join_cols)

    # Save the resulting dataframe back to SQL
    new_table_name = f"hts_2013_aggregated_{style}"
    db.import_dataframe(df, new_table_name, if_exists="replace")


def main():
    """ Process the HTS data, and then aggregate twice:
        1) O to D, with all modes combined
        2) O to D, with a distinct row for each mode
    """
    process_hts()
    aggregate_hts(style="all_modes_combined")
    aggregate_hts(style="by_mode")
