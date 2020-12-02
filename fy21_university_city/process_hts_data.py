"""
Use the 2012-13 Household Travel Survey data

"""
from datetime import time
from fy21_university_city.db_io import db_connection

mode_codes = {
    1:  "Walk",
    2:  "Bike",
    3:  "Private Vehicle",
    4:  "Private Transit",
    5:  "Public Transit",
    6:  "School Bus",
    7:  "Other"
}

db = db_connection()

# Get a basic table out of SQL
query = """
    select o_cpa, d_cpa, mode_agg, arrive, depart, compositeweight
    from hts_2013_trips
"""

df = db.query_as_df(query)

df["o_time"] = "_"
df["d_time"] = "_"
df["time_class"] = "_"


def classify_time_value(txt: str) -> str:
    if not txt:
        return "Null value"
    elif ":" not in txt:
        return "Don't Know/Refused"
    else:
        return "Usable data"


def timeify(txt: str) -> time:
    hour, minute = txt.split(":")
    return time(int(hour), int(minute))


def time_is_between(the_time: time, start: int, end: int) -> bool:
    return time(start) <= the_time < time(end):

def is_am_peak(the_time: time) -> bool:
    return time_is_between(the_time, 6, 10)

def is_pm_peak(the_time: time) -> bool:
    return time_is_between(the_time, 6, 10)


for idx, row in df.iterrows():
    o = classify_time_value(row.depart)
    d = classify_time_value(row.arrive)

    # If both times are known...
    if o == "Usable data" and d == "Usable data":
        o_time = timeify(row.depart)
        d_time = timeify(row.arrive)

        if time(6) <= o_time < time(10):
            print("starts in AM peak")
        if time(6) <= o_time < time(10):
            print("ends in AM peak")


    # If only depart time is known...

    # If only arrive time is known...