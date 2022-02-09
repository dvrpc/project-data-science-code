"""
process_model_results.py
------------------------

This script takes data exported from VISUM
and generates summaries that can subsequently
be mapped for the project's final report.

The general process for each dataset is to:
    1) Import gis data
    2) Import tabular data
    3) Join the two tables as needed
    4) Save the join results into one or many shapefiles, as needed

Datasets include:
    - highway link volumes and v/c ratios
    - transit link volumes
    - stop-level boardings by mode
    - 24hr origin-destination flows to/from the study area
"""

import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pg_data_etl import Database

from fy21_university_city import GDRIVE_FOLDER

load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("FY21_UCITY_DB")
db = Database.from_uri(DATABASE_URL)

result_folder = GDRIVE_FOLDER / "GIS/Results_for_Mark"
output_folder = GDRIVE_FOLDER / "GIS/Results_for_Mark_Crunched"

def import_geodata():
    """
    Import all shapefiles that accompany the tabular model outputs
    """

    # Import shapefiles
    shapefiles_to_import = result_folder.rglob("*.SHP")

    for shp_path in shapefiles_to_import:
        tablename = shp_path.stem.lower()
        if f"public.{tablename}" not in db.tables():
            db.import_gis(
                filepath=shp_path,
                sql_tablename=tablename,
                explode=True,
                gpd_kwargs={"if_exists": "replace"},
            )


def import_and_process_hwy_links():
    """
    Import tabular data showing hwy link volumes and v/c ratios.
    Generate shapefile summaries through a join operation.
    """
    # Import the link-level volumes
    files_to_import = result_folder.rglob("*HWY_Link_Feb.xlsx")

    for f in files_to_import:
        tablename = f.stem.lower()
        if f"public.{tablename}" not in db.tables():
            df = pd.read_excel(f, skiprows=4)
            db.import_dataframe(df, tablename=tablename)

    # Generate shapefile outputs
    query_template = """
        with link_data as (
            select
                "2045_base_vc_ratio" as base_vc,
                "2045_base_vol" as base_vol,
                "2045_moderate_vc_ratio" as mod_vc,
                "2045_moderate_vol" as mod_vol,
                "2045_high_vc_ratio" as high_vc,
                "2045_high_vol" as high_vol,
                concat('a', fromnodeno, 'b', tonodeno) as uid
            from LINK_TABLE_PLACEHOLDER 
        ),
        links as (
            select
                concat('a', fromnodeno, 'b', tonodeno) as uid,
                no as link_id,
                geom
            from u_city_link
        ),
        joined_data as (
            select d.*, l.link_id, l.geom 
            from link_data d
            left join links l on d.uid = l.uid
        )
        select 
            case
                when j.link_id in (
                    select link_id from joined_data
                    group by link_id having count(*) < 2
                )
                then 'oneway'
                else 'twoway' end as dir,
            j.*
        from joined_data j
    """

    for table in [
        "am_hwy_link_feb",
        "md_hwy_link_feb",
        "pm_hwy_link_feb",
    ]:
        query = query_template.replace("LINK_TABLE_PLACEHOLDER", table)
        print(query)
        gdf = db.gdf(query)

        output_filepath = output_folder / f"{table}.shp"

        gdf.to_file(output_filepath)


def import_and_process_transit_links():
    """
    Import tabular data showing transit link volumes.
    Generate shapefile summaries through a join operation.
    """

    # Import the link-level volumes
    files_to_import = result_folder.rglob("*Transit_Link_Feb.xlsx")

    for f in files_to_import:
        tablename = f.stem.lower()
        if f"public.{tablename}" not in db.tables():
            print(tablename)
            df = pd.read_excel(f, skiprows=4)
            db.import_dataframe(df, tablename=tablename)

    # Generate shapefile outputs
    query_template = """
        with link_data as (
            select
                concat('a', fromnodeno, 'b', tonodeno) as uid,
                "2015_put" as put_2015,
                base_put,
                mod_put,
                high_put
            from LINK_TABLE_PLACEHOLDER 
        ),
        links as (
            select
                concat('a', fromnodeno, 'b', tonodeno) as uid,
                no as link_id,
                geom
            from u_city_link
        ),
        joined_data as (
            select d.*, l.link_id, l.geom 
            from link_data d
            left join links l on d.uid = l.uid
        )
        select 
            case
                when j.link_id in (
                    select link_id from joined_data
                    group by link_id having count(*) < 2
                )
                then 'oneway'
                else 'twoway' end as dir,
            j.*
        from joined_data j
    """

    for table in [
        "am_transit_link_feb",
        "md_transit_link_feb",
        "pm_transit_link_feb",
    ]:
        query = query_template.replace("LINK_TABLE_PLACEHOLDER", table)
        print(query)
        gdf = db.gdf(query)

        output_filepath = output_folder / f"{table}.shp"

        gdf.to_file(output_filepath)


def import_and_process_transit_stops():
    """
    Import tabular data showing stop (point) boardings by mode.
    Generate shapefile summary through a join operation.
    """

    filepath = result_folder / "Stop_Boards_by_TSys_Cleaned_Feb_22.xlsx"

    if f"public.stop_boardings" not in db.tables():
        df = pd.read_excel(filepath, sheet_name="24_Hr_Boards")
        db.import_dataframe(df, tablename="stop_boardings")

    query = "select d.*, p.code, p.name, p.geom from stop_boardings d left join u_city_transit_stop p on p.no = d.stop_num"

    output_filepath = output_folder / "stop_boardings_24hr.shp"
    db.export_gis(method="ogr2ogr", table_or_sql=query, filepath=output_filepath)


def import_and_process_od():
    """
    Import tabular data showing O/D volumes to/from the study area.
    Generate shapefile summaries through a join operation.
    """

    filepath = result_folder / "Full_UC_OD_single_table_cleaned_for_postgres.xlsx"

    tabnames = ["FROM_UCITY", "TO_UCITY"]

    for tab in tabnames:
        tablename = f"public.od_{tab.lower()}"

        if tablename not in db.tables():
            print(tablename)
            df = pd.read_excel(filepath, sheet_name=tab)
            db.import_dataframe(df, tablename=tablename)

    query_template = """
        select
            z.no, z.name, z.geom,
            d.*
        from
            u_city_zone z
        full outer join
            TABLENAME_PLACEHOLDER d
        on
            d.tazid = z.no
        where
            z.geom is not null
    """

    # this is a list of tuples
    # tuples consist of (output_shp_name, query_string)
    queries = [
        ("trips_to_ucity", query_template.replace("TABLENAME_PLACEHOLDER", "od_to_ucity")),
        ("trips_from_ucity", query_template.replace("TABLENAME_PLACEHOLDER", "od_from_ucity")),
    ]

    for shp_name, query in queries:
        gdf = db.gdf(query)

        output_filepath = output_folder / f"{shp_name}.shp"

        gdf.to_file(output_filepath)


if __name__ == "__main__":
    import_geodata()
    import_and_process_hwy_links()
    import_and_process_transit_links()
    import_and_process_transit_stops()
    import_and_process_od()
