import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pg_data_etl import Database

from fy21_university_city import GDRIVE_FOLDER

load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("FY21_UCITY_DB")
db = Database.from_uri(DATABASE_URL)

result_folder = GDRIVE_FOLDER / "GIS/Results_Data_for_Aaron"
output_folder = GDRIVE_FOLDER / "GIS/Results_Data_for_Aaron_Crunched"


def import_geodata():

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
    # Import the link-level volumes
    files_to_import = result_folder.rglob("*Link_Comparison_Vals.xlsx")

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
        "am_hwy_link_comparison_vals",
        "md_hwy_link_comparison_vals",
        "pm_hwy_link_comparison_vals",
    ]:
        query = query_template.replace("LINK_TABLE_PLACEHOLDER", table)
        print(query)
        gdf = db.gdf(query)

        output_filepath = output_folder / f"{table}.shp"

        gdf.to_file(output_filepath)


def import_and_process_transit_links():
    # Import the link-level volumes
    files_to_import = result_folder.rglob("*Transit_Link_Vols.xlsx")

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
        "am_transit_link_vols",
        "md_transit_link_vols",
        "pm_transit_link_vols",
    ]:
        query = query_template.replace("LINK_TABLE_PLACEHOLDER", table)
        print(query)
        gdf = db.gdf(query)

        output_filepath = output_folder / f"{table}.shp"

        gdf.to_file(output_filepath)


if __name__ == "__main__":
    import_and_process_transit_links()
