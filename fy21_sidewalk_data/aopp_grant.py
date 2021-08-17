"""
This script takes Census GIS data and the eligibility
table from the Federal Areas of Persistent Poverty Program
to create a filtered GIS shapefile containing all tracts
that are eligible within the 9-county DVRPC region.

"""

import pandas as pd
from pathlib import Path

from pg_data_etl import Database


PA_COUNTIES = [
    "Bucks County",
    "Chester County",
    "Delaware County",
    "Montgomery County",
    "Philadelphia County",
]
NJ_COUNTIES = [
    "Burlington County",
    "Camden County",
    "Gloucester County",
    "Mercer County",
]

if __name__ == "__main__":

    db = Database.from_config("aopp_grant", "localhost")
    db.admin("CREATE")

    # Read tabular data and filter to eligible tracts in our region before writing to database
    data_source_folder = Path(
        "/Volumes/GoogleDrive/Shared drives/Sidewalk Gap Analysis/GIS data/AOPP-analysis"
    )

    df = pd.read_csv(data_source_folder / "inputs/RAISE_Persistent_Poverty.csv")

    # Filter to the county/state combinations we care about
    pa_tracts = df[(df["A. State"] == "Pennsylvania") & (df["B. County"].isin(PA_COUNTIES))]
    nj_tracts = df[(df["A. State"] == "New Jersey") & (df["B. County"].isin(NJ_COUNTIES))]

    # Merge PA and NJ tracts together, and then drop all rows that are ineligible
    all_tracts = pd.concat([pa_tracts, nj_tracts])
    all_tracts = all_tracts[all_tracts["F. CENSUS TRACT Meets Definition?"] == "Yes"]

    # Import our region's eligible tracts into the database
    db.import_dataframe(all_tracts, "eligible_tracts", df_import_kwargs={"if_exists": "replace"})

    # Import all shapefiles within all nested sub-folders
    shp_folder = data_source_folder / "inputs"
    for shp in shp_folder.rglob("*.shp"):
        print(shp)
        db.import_gis(
            filepath=shp, sql_tablename=shp.stem, explode=True, gpd_kwargs={"if_exists": "replace"}
        )

    # Generate a new table within the database that joins all the necessary information across the
    # imported eligibility table and shapefiles
    query = """
        with all_tracts as (
            select 'New Jersey' as statename, statefp as state_code, countyfp as county_code, namelsad as tractname, st_transform(geom, 4326) as geom
            from tl_2018_34_tract
            
            union
            
            select 'Pennsylvania' as statename, statefp as state_code, countyfp as county_code, namelsad, st_transform(geom, 4326) as geom
            from tl_2018_42_tract
        ),
        county_names as (
            select statefp, countyfp, namelsad as countyname
            from tl_2018_us_county tuc 
        ),
        tracts_with_county_names as (
            select tr.*, cn.*
            from all_tracts tr
            inner join county_names cn
            on tr.state_code = cn.statefp
            and tr.county_code = cn.countyfp
        )
        select et.*, s.*
        from eligible_tracts et
        left join tracts_with_county_names s
        on et.a_state = s.statename
        and et.b_county = s.countyname
        and et.c_census_tract_name = s.tractname
    """
    db.gis_make_geotable_from_query(query, "eligible_tract_geometries", "POLYGON", 4326)

    # Write the resulting spatial table to shapefile for cartography
    output_path = data_source_folder / "outputs/eligible_census_tracts.shp"
    db.export_gis(table_or_sql="eligible_tract_geometries", filepath=output_path, filetype="shp")