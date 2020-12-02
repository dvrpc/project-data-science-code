"""
Use the 2012-13 Household Travel Survey data

"""
from datetime import time
from tqdm import tqdm

from fy21_university_city.db_io import db_connection

# mode_codes = {
#     1:  "Walk",
#     2:  "Bike",
#     3:  "Private Vehicle",
#     4:  "Private Transit",
#     5:  "Public Transit",
#     6:  "School Bus",
#     7:  "Other"
# }

db = db_connection()

# Get a basic table out of SQL
query = """
    select person_id, trip_num, act_dur1, o_cpa, d_cpa, mode_agg, arrive, depart, compositeweight
    from hts_2013_trips
    order by person_id, trip_num
"""

df = db.query_as_df(query)

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
    return time(start) <= the_time < time(end)


def is_am_peak(the_time: time) -> bool:
    return time_is_between(the_time, 6, 10)


def is_pm_peak(the_time: time) -> bool:
    return time_is_between(the_time, 15, 19)


def find_next_trip(person_id, trip_num):

    query = f"""
        SELECT arrive
        FROM hts_2013_trips
        WHERE
            person_id = {person_id}
          AND
            trip_num > {trip_num}
        ORDER BY trip_num ASC
        LIMIT 1
    """

    return db.query_as_list(query)


for idx, row in df.iterrows():

    time_at_origin = row.depart
    time_at_destination = find_next_trip(row.person_id, row.trip_num)

    o = classify_time_value(time_at_origin)
    d = classify_time_value(time_at_destination)

    time_class = "..."


    print(row.person_id, row.trip_num, row.act_dur1, time_at_origin, time_at_destination)

    # If both times are known...
    if o == "Usable data" and d == "Usable data":
        o_time = timeify(time_at_origin)
        d_time = timeify(time_at_destination)

        if is_am_peak(o_time) or is_am_peak(d_time):
            time_class = "AM period"

        if is_pm_peak(o_time) or is_pm_peak(d_time):
            time_class = "PM period"

    # If only depart time is known...
    elif o == "Usable data":
        o_time = timeify(time_at_origin)

        if is_am_peak(o_time):
            time_class = "AM period"

        if is_pm_peak(o_time):
            time_class = "PM period"

    # If only arrive time is known...
    elif d == "Usable data":
        d_time = timeify(time_at_destination)

        if is_am_peak(d_time):
            time_class = "AM period"

        if is_pm_peak(d_time):
            time_class = "PM period"

    df.at[idx, "time_class"] = time_class

db.import_dataframe(df, "hts_2013_trips_processed", if_exists="replace")