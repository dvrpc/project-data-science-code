import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import osmnx as ox

load_dotenv()
GDRIVE_FOLDER = os.getenv("GDRIVE_PROJECT_FOLDER")
GEOJSON_FOLDER = os.getenv("GEOJSON_FOLDER")
DB_NAME = os.getenv("DB_NAME")
HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PW = os.getenv("DB_PASSWORD")
db_connection_info = os.getenv("DATABASE_URL")
engine = create_engine(db_connection_info)
dbConnection = engine.connect()
target_network = [
    "Camden County, NJ",
    "Burlington County, NJ",
    "Mercer County, NJ",
    "Gloucester County, NJ",
]
srid = 2272
miles = (
    8.75  # target isochrone size. in this case this is 15 minutes driving at 35 mph.
)
round_miles = round(miles)
distance_threshold = (
    miles * 5280
)  # the distance, in the units of above SRID, of the isochrone using the network


def import_points(points):
    """imports target points into your postgres db, i.e. what you want isochrones around"""
    gdf = gpd.read_file(f"{GEOJSON_FOLDER}/{points}")
    print("importing target points into database...")
    gdf = gdf.to_crs(f"EPSG:{srid}")
    gdf.to_postgis("target_points", engine, schema=None, if_exists="replace")


def import_taz():
    """imports taz population data from g drive"""
    zipfile = "/Volumes/GoogleDrive/Shared drives/Community & Economic Development /Ferry Service Feasibility_FY22/Shapefiles/CTPP2012_2016_total_pop_taz.zip"
    gdf = gpd.read_file(zipfile)
    gdf.rename(
        columns={"F0": "population", "F1": "moe", "name": "taz_name"}, inplace=True
    )
    print("importing taz polygons with population into database...")
    gdf = gdf.to_crs(f"EPSG:{srid}")
    gdf.to_postgis("taz_pop", engine, schema=None, if_exists="replace")


def import_dvrpc_munis():
    """imports dvrpc municipalities"""
    url = "https://arcgis.dvrpc.org/portal/rest/services/Boundaries/MunicipalBoundaries/FeatureServer/0/query?where=dvrpc_reg%20%3D%20'Yes'&outFields=*&outSR=4326&f=json"
    gdf = gpd.read_file(url)
    print("importing dvrpc municipal boundaries into database...")
    gdf = gdf.to_crs(f"EPSG:{srid}")
    gdf.to_postgis("dvrpc_munis", engine, schema=None, if_exists="replace")


def import_attractions():
    """imports attraction data"""
    print("importing attractions points into database...")
    gdf = gpd.read_file(f"{GEOJSON_FOLDER}/attractions.geojson")
    gdf = gdf.to_crs(f"EPSG:{srid}")
    gdf.to_postgis("attractions", engine, schema=None, if_exists="replace")


def import_osmnx(target_network):
    """imports toplogically sound osmnx network for target_network, defined above"""
    G = ox.graph_from_place(target_network, network_type="drive")
    ox.io.save_graph_geopackage(
        G, filepath=f"{GEOJSON_FOLDER} /graph.gpkg", encoding="utf-8", directed=False
    )
    gdf = gpd.read_file(f"{GEOJSON_FOLDER}/graph.gpkg", layer="edges")
    gdf.to_postgis("edges", engine, schema=None, if_exists="replace")

    gdf = gpd.read_file(f"{GEOJSON_FOLDER}/graph.gpkg", layer="nodes")
    gdf.to_postgis("nodes", engine, schema=None, if_exists="replace")


def osmnx_to_pg_routing():
    """creates a new table based on the OSMNX exports that is compatable with the pgRouting extension of PostGIS"""
    query = f"""
        drop table if exists osmnx;
        create table osmnx as(
        select fid as id,
            "from"::bigint as "source",
            "to"::bigint as target,
            "name",
            st_length(st_transform(geom,{srid})) * 3.28084 as len_feet,
            st_length(st_transform(geom,{srid})) as real_length,
            1000000000 as reverse_cost,
            st_transform(geom, {srid}) as geom
        from edges
        where geom is not null
    ); 
    SELECT UpdateGeometrySRID('osmnx','geom',{srid});
     """
    engine.execute(query)


# todo: cleanup nearest neighbor so it's more modular
def nearest_neighbor():
    query = f"""
       drop table if exists points;
        create table points as (
            select target_points.id as id, 
            (select osmnx."source" from osmnx order by st_distance(st_transform(target_points.geometry, {srid}),osmnx.geom) 
            limit 1) as osmnx_id, st_transform(target_points.geometry, {srid}) as geom from target_points);
            select UpdateGeometrySRID('points','geom',{srid});
            """

    engine.execute(query)
    df = pd.read_sql('select osmnx_id from "points"', dbConnection)
    neighbors = df.values.tolist()
    df2 = pd.read_sql('select id from "points"', dbConnection)
    list_of_ids = df2.values.tolist()
    return neighbors, list_of_ids


def make_isochrones(neighbors, list_of_ids):
    drop_query = f"""drop table if exists isochrones{round_miles};"""
    engine.execute(drop_query)
    count = 0
    for value, id in zip(neighbors, list_of_ids):
        isochrone_query = f"""        
        SELECT * FROM pgr_drivingDistance(
                'SELECT id, source, target, real_length as cost, reverse_cost FROM osmnx',
                {value[0]}, {distance_threshold}, false
            ) as a
        JOIN osmnx AS b ON (a.edge = b.id) ORDER BY seq;
        """

        tempgdf = gpd.GeoDataFrame.from_postgis(isochrone_query, engine)
        tempgdf["iso_id"] = id[0]
        tempgdf = tempgdf.set_crs(f"EPSG:{srid}")
        print(f"Creating isochrone # {count}...")
        count += 1
        tempgdf.to_postgis(f"isochrones{round_miles}", engine, if_exists="append")


def make_hulls():
    hull_query = f"""
    drop table if exists isochrone_hull{round_miles};
    create table isochrone_hull{round_miles} as(
        select iso_id, ST_ConcaveHull(ST_Union(geom), 0.80) as geom from isochrones{round_miles}
        group by iso_id
        );
    select UpdateGeometrySRID('isochrone_hull{round_miles}','geom',{srid});
    """
    print("Creating convex hulls around isochrones, just a moment...")
    engine.execute(hull_query)
    print("Isochrones and hulls created, see database for results.")


def calculate_taz_demand():
    # ripe for refactoring, variablize isohulls
    """uses drive time isochrones to calculate taz demand for both 15 and 30 minutes using HHTS data"""
    query = """drop table if exists to_from_15;
    create table to_from_15 as
    with nj_philly as(
        SELECT
        isochrone_hull9.iso_id
        , coalesce(sum(nj_philly_rec_trips.trips),0) AS nj_philly_trips_to_driveshed
        FROM isochrone_hull9 
            LEFT JOIN nj_philly_rec_trips 
            ON ST_Intersects(isochrone_hull9.geom, st_transform(nj_philly_rec_trips.geom, 2272)) 
        GROUP BY isochrone_hull9.iso_id
    ),
    philly_nj as (
    SELECT
        isochrone_hull9.iso_id
        , coalesce(sum(philly_nj_rec_trips.trips),0) AS philly_nj_trips_in_driveshed
        FROM isochrone_hull9 
            LEFT JOIN philly_nj_rec_trips 
            ON ST_Intersects(isochrone_hull9.geom, st_transform(philly_nj_rec_trips.geom, 2272)) 
        GROUP BY isochrone_hull9.iso_id
    ), to_from_total as(
        select philly_nj.iso_id, philly_nj_trips_in_driveshed, nj_philly.nj_philly_trips_to_driveshed from philly_nj
        inner join nj_philly
        on philly_nj.iso_id = nj_philly.iso_id)
        select * from to_from_total;
    drop table if exists to_from_joined15;
    create table to_from_joined15 as (
    SELECT tp.geometry, iso_id, philly_nj_trips_in_driveshed, nj_philly_trips_to_driveshed, COALESCE(philly_nj_trips_in_driveshed ,0) + COALESCE(nj_philly_trips_to_driveshed ,0) as sum
    FROM to_from_15
    inner join target_points tp
    on to_from_15.iso_id = tp.id);
    drop table if exists to_from_30;
    create table to_from_30 as
    with nj_philly as(
        SELECT
        isochrone_hull18.iso_id
        , coalesce(sum(nj_philly_rec_trips.trips),0) AS nj_philly_trips_to_driveshed
        FROM isochrone_hull18 
            LEFT JOIN nj_philly_rec_trips 
            ON ST_Intersects(isochrone_hull18.geom, st_transform(nj_philly_rec_trips.geom, 2272)) 
        GROUP BY isochrone_hull18.iso_id
    ),
    philly_nj as (
        SELECT
        isochrone_hull18.iso_id
        , coalesce(sum(philly_nj_rec_trips.trips),0) AS philly_nj_trips_in_driveshed
        FROM isochrone_hull18 
            LEFT JOIN philly_nj_rec_trips 
            ON ST_Intersects(isochrone_hull18.geom, st_transform(philly_nj_rec_trips.geom, 2272)) 
        GROUP BY isochrone_hull18.iso_id
    ), to_from_total as(
        select philly_nj.iso_id, philly_nj_trips_in_driveshed, nj_philly.nj_philly_trips_to_driveshed from philly_nj
        inner join nj_philly
        on philly_nj.iso_id = nj_philly.iso_id)
        select * from to_from_total;
    drop table if exists to_from_joined30;
    create table to_from_joined30 as (
        SELECT tp.geometry, iso_id, philly_nj_trips_in_driveshed, nj_philly_trips_to_driveshed, COALESCE(philly_nj_trips_in_driveshed ,0) + COALESCE(nj_philly_trips_to_driveshed ,0) as sum
        FROM to_from_30
        inner join target_points tp
        on to_from_30.iso_id = tp.id);
    drop table if exists to_from_15_30;
    create table to_from_15_30 as(
        select 
            tf15.iso_id, 
            tf15.philly_nj_trips_in_driveshed as philly_nj_15, 
            tf30.philly_nj_trips_in_driveshed as philly_nj_30,
            tf15.nj_philly_trips_to_driveshed as nj_philly_15,
            tf30.nj_philly_trips_to_driveshed as nj_philly_30,
            tf15.geometry as geom
        from to_from_joined15 tf15
        inner join to_from_joined30 tf30
        on tf15.iso_id = tf30.iso_id);
    drop table if exists to_from_15;
    drop table if exists to_from_30;
    drop table if exists to_from_joined;
    drop table if exists to_from_joined15;
    drop table if exists to_from_joined30;
    """
    engine.execute(query)
    print("HHTS demand created, check database for final table")


def calculate_attractions_and_demand_in_isos():
    """calculates the HHTS demand and the number of attractions for each isochrone_id"""
    # todo: refactor with variables rather than reqpeating slightly different query
    calculate_taz_demand()
    """calculates number of attractions within isochrones. has to be run after previous function."""
    query = """drop table if exists attractions30;
    create table attractions30 as(
    select iso_id, count(attractions) as attractions_30_min
        FROM isochrone_hull18 
        LEFT JOIN attractions 
        ON st_within(attractions.geometry, isochrone_hull18.geom) 
        GROUP BY isochrone_hull18.iso_id
        order by iso_id);
    drop table if exists attractions15;
    create table attractions15 as(
        select iso_id, count(attractions) as attractions_15_min
        FROM isochrone_hull9
        LEFT JOIN attractions 
        ON st_within(attractions.geometry, isochrone_hull9.geom) 
        GROUP BY isochrone_hull9.iso_id
        order by iso_id);
    select * from attractions15
    inner join attractions30 
    on attractions15.iso_id = attractions30.iso_id;
    drop table if exists attractions_in_isochrones;
    create table attractions_in_isochrones as(
        select attractions15.iso_id, attractions15.attractions_15_min, attractions30.attractions_30_min
        from attractions15
        inner join attractions30
        on attractions15.iso_id = attractions30.iso_id);
    drop table if exists attractions15;
    drop table if exists attractions30;
    alter table to_from_15_30
        add column attractions15 varchar(50);
    alter table to_from_15_30
        add column attractions30 varchar(50);
    update to_from_15_30 
    set attractions15 = attractions_in_isochrones.attractions_15_min 
    from attractions_in_isochrones
    where attractions_in_isochrones.iso_id=to_from_15_30.iso_id;
    update to_from_15_30 
    set attractions30 = attractions_in_isochrones.attractions_30_min 
    from attractions_in_isochrones
    where attractions_in_isochrones.iso_id=to_from_15_30.iso_id;"""
    print("calculating number of attractions in each isochrone...")
    engine.execute(query)


def calculate_population_in_isos(iso_distance):
    """calculates population for isochrone distance (in minutes) and adds new column to master table"""

    if iso_distance == 15:
        iso_hull = 9
    if iso_distance == 30:
        iso_hull = 18
    else:
        print(
            "iso distance not in current script, must edit it for different size isochrones"
        )
    query = f"""alter table to_from_15_30 
    drop column if exists pop{iso_distance};
    alter table to_from_15_30
    add column pop{iso_distance} varchar(50);
    UPDATE to_from_15_30 
    SET pop{iso_distance}=subquery.population
    FROM (select ih.iso_id, sum(population) as population from taz_pop tp 
        inner join isochrone_hull{iso_hull} ih 
        on st_intersects(ih.geom,tp.geometry)
        group by ih.iso_id
        order by ih.iso_id) AS subquery
    WHERE to_from_15_30.iso_id=subquery.iso_id;"""
    print(f"calculating total population in the {iso_distance}-minute shed...")
    engine.execute(query)


def pickup_munis():
    """joins the master table with dvprc municipalities for better identification of points"""
    query = """
    alter table to_from_15_30 
    drop column if exists muni;
    alter table to_from_15_30
    add column muni varchar(50);
    UPDATE to_from_15_30 
    SET muni=subquery.mun_name
    FROM (select 
        id, dvrpc_munis.mun_name
        from target_points tp
        inner join dvrpc_munis
        on st_intersects(tp.geometry, dvrpc_munis.geometry)
        group by id, dvrpc_munis.mun_name
        order by id) AS subquery
    WHERE to_from_15_30.iso_id=subquery.id;"""
    print(f"joining main table to DVRPC municipalities...")
    engine.execute(query)


if __name__ == "__main__":
    # import_points("dock_no_freight.geojson")
    # import_osmnx(target_network)
    # import_taz()
    # import_attractions()
    # import_dvrpc_munis()

    # osmnx_to_pg_routing()
    # neighbor_obj = nearest_neighbor()
    # make_isochrones(neighbor_obj[0], neighbor_obj[1])
    # make_hulls()
    # calculate_attractions_and_demand_in_isos()
    calculate_population_in_isos(15)
    calculate_population_in_isos(30)
    pickup_munis()

    # engine.dispose()

    # todo: do we need "len_feet" column? is it useful/used anywhere, if not, should be deleted as it's confusing since units are dynamic now
    # todo: calculate population function (need data at taz level)
    # add taz automation, insertion of philly_nj and nj_philly tables
