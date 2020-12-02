"""


"""

from fy21_university_city import GDRIVE_FOLDER
from fy21_university_city.db_io import db_connection


def main(qa_name: str):

    qa_folder = GDRIVE_FOLDER / "Data/QAQC"

    qa_routines = {
        "apc_with_null_latlngs": """ 
                    select
                        route_id,
                        stop_id,
                        stop_name,
                        sum(hourly_ons) as ons24hr,
                        sum(hourly_offs) as offs24hr
                    from
                        apc_raw
                    where
                        stop_lat is null
                    and
                        stop_lon is null
                    group by
                        route_id, stop_id, stop_name, stop_lat, stop_lon
                    order by route_id, (sum(hourly_ons) + sum(hourly_offs)) desc
                """,
    }

    # Exit early if an undefined QA process is provided as qa_name
    if qa_name not in qa_routines:
        print(f"'{qa_name}' is not a pre-defined QAQC process.")
        print("Options include:")
        for routine in qa_routines:
            print("\t ->", routine)
        return

    db = db_connection()

    for routine in qa_routines:
        query = qa_routines[routine]

        df = db.query_as_df(query)

        # Write to XLSX file
        filepath = qa_folder / f"{routine}.xlsx"
        df.to_excel(filepath)
