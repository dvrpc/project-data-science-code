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
    gdf.to_postgis(
        "edges", engine, schema=None, if_exists="replace", index=True, index_label="fid"
    )

    gdf = gpd.read_file(f"{GEOJSON_FOLDER}/graph.gpkg", layer="nodes")
    gdf.to_postgis(
        "nodes", engine, schema=None, if_exists="replace", index=True, index_label="fid"
    )


def osmnx_to_pg_routing():
    """creates a new table based on the OSMNX exports that is compatable with the pgRouting extension of PostGIS"""
    query = f"""
        drop table if exists osmnx;
        create table osmnx as(
        select
            fid as id,
            "from"::bigint as "source",
            "to"::bigint as target,
            "name",
            st_length(st_transform(geometry,{srid})) * 3.28084 as len_feet,
            st_length(st_transform(geometry,{srid})) as real_length,
            1000000000 as reverse_cost,
            st_transform(geometry, {srid}) as geom
        from edges
        where geometry is not null
    ); 
    SELECT UpdateGeometrySRID('osmnx','geom',{srid});
     """
    engine.execute(query)


# todo: cleanup nearest neighbor so it's more modular
def nearest_node():
    """finds the nearest node on the network to target point"""
    query = f"""
       drop table if exists nearest_node;
        create table nearest_node as (
            select target_points.id as id, 
            (select osmnx."source" from osmnx order by st_distance(st_transform(target_points.geometry, {srid}),osmnx.geom) 
            limit 1) as osmnx_id, st_transform(target_points.geometry, {srid}) as geom from target_points);
            select UpdateGeometrySRID('nearest_node','geom',{srid});
            """

    engine.execute(query)
    df = pd.read_sql('select osmnx_id from "nearest_node"', dbConnection)
    neighbors = df.values.tolist()
    df2 = pd.read_sql('select id from "nearest_node"', dbConnection)
    list_of_ids = df2.values.tolist()
    return neighbors, list_of_ids


def make_isochrones(neighbors, list_of_ids, minutes, speed_mph):
    miles = (minutes / 60) * speed_mph  # represents distance of isochrone
    distance_threshold = (
        miles * 5280
    )  # the distance, in the units of above SRID, of the isochrone using the network
    """generates isochrones using pgrouting query"""
    drop_query = f"""drop table if exists isochrones{minutes};"""
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
        print(f"Creating {minutes}-minute isochrone # {count}...")
        count += 1
        tempgdf.to_postgis(f"isochrones{minutes}_minutes", engine, if_exists="append")


def make_hulls(minutes):
    hull_query = f"""
    drop table if exists isochrone_hull{minutes}_minutes_minutes;
    create table isochrone_hull{minutes}_minutes_minutes as(
        select iso_id, ST_ConcaveHull(ST_Union(geom), 0.80) as geom from isochrones{minutes}_minutes
        group by iso_id
        );
    select UpdateGeometrySRID('isochrone_hull{minutes}_minutes_minutes','geom',{srid});
    """
    print(f"Creating convex hulls around {minutes}-minute isochrones, just a moment...")
    engine.execute(hull_query)
    print("Isochrones and hulls created, see database for results.")


def calculate_taz_demand(minutes):
    # ripe for refactoring, variablize isohulls
    """uses drive time isochrones to calculate taz demand for an isochrone of a given timeframe"""
    query = f"""drop table if exists to_from{minutes};
    create table to_from{minutes} as
    with nj_philly as(
        SELECT
        isochrone_hull{minutes}_minutes.iso_id
        , coalesce(sum(nj_philly_rec_trips.trips),0) AS nj_philly_trips_in_driveshed
        FROM isochrone_hull{minutes}_minutes 
            LEFT JOIN nj_philly_rec_trips 
            ON ST_Intersects(isochrone_hull{minutes}_minutes.geom, st_transform(nj_philly_rec_trips.geom, 2272)) 
        GROUP BY isochrone_hull{minutes}_minutes.iso_id
    ),
    philly_nj as(
    SELECT
        isochrone_hull{minutes}_minutes.iso_id
        , coalesce(sum(philly_nj_rec_trips.trips),0) AS philly_nj_trips_in_driveshed
        FROM isochrone_hull{minutes}_minutes 
            LEFT JOIN philly_nj_rec_trips 
            ON ST_Intersects(isochrone_hull{minutes}_minutes.geom, st_transform(philly_nj_rec_trips.geom, 2272)) 
        GROUP BY isochrone_hull{minutes}_minutes.iso_id)
    select philly_nj.iso_id, philly_nj_trips_in_driveshed, nj_philly.nj_philly_trips_in_driveshed from philly_nj
    inner join nj_philly 
    on philly_nj.iso_id=nj_philly.iso_id
    """
    engine.execute(query)
    print(f"{minutes}-minute demand table created, check database for table")


def aggregate_demand(iso_minutes_A, iso_minutes_B):
    """Builds a table that aggregates to and from demand for two sets of isochrones, e.g. 15 and 30 minute isochrones"""
    query = f"""
    drop table if exists aggregated_results;
    create table aggregated_results as(
        select 
            tf{iso_minutes_A}.iso_id,
            tf{iso_minutes_A}.philly_nj_trips_in_driveshed + tf{iso_minutes_A}.nj_philly_trips_in_driveshed as total_trips{iso_minutes_A},
            tf{iso_minutes_B}.philly_nj_trips_in_driveshed + tf{iso_minutes_B}.nj_philly_trips_in_driveshed as total_trips{iso_minutes_B}
        from to_from{iso_minutes_A} tf{iso_minutes_A}
        inner join to_from{iso_minutes_B} tf{iso_minutes_B}
        on tf{iso_minutes_A}.iso_id = tf{iso_minutes_B}.iso_id)"""
    engine.execute(query)
    print(f"To and from demand columns combined...")


def calculate_attractions_and_demand_in_isos(minutes):
    """calculates the HHTS demand and the number of attractions for each isochrone_id"""
    query = f"""
    ALTER table aggregated_results
        DROP column if exists attractions{minutes};
    ALTER table aggregated_results
        ADD column attractions{minutes} varchar(50);
    UPDATE aggregated_results 
    SET attractions{minutes}= subquery.attractions_{minutes}_min
    FROM (SELECT iso_id, count(attractions) as attractions_{minutes}_min
        FROM isochrone_hull{minutes}_minutes 
            LEFT JOIN attractions 
            ON st_within(attractions.geometry, isochrone_hull{minutes}_minutes.geom) 
            GROUP BY isochrone_hull{minutes}_minutes.iso_id
            ORDER by iso_id) AS subquery
    WHERE aggregated_results.iso_id=subquery.iso_id;
    """
    print(
        f"calculating number of attractions in each isochrone for the {minutes}-minute shed..."
    )
    engine.execute(query)


def calculate_population_in_isos(minutes):

    """calculates population for isochrone distance (in minutes) and adds new column to master table"""

    query = f"""
    alter table aggregated_results 
        DROP column if exists pop{minutes};
    ALTER table aggregated_results
        ADD column pop{minutes} varchar(50);
    UPDATE aggregated_results 
    SET pop{minutes}=subquery.population
    FROM (SELECT ih.iso_id, sum(population) as population 
        FROM taz_pop tp 
            INNER join isochrone_hull{minutes}_minutes ih 
            ON st_intersects(ih.geom,tp.geometry)
            GROUP by ih.iso_id
            ORDER by ih.iso_id) AS subquery
    WHERE aggregated_results.iso_id=subquery.iso_id;"""
    print(f"calculating total population in the {minutes}-minute shed...")
    engine.execute(query)


def pickup_munis():
    """joins the master table with dvprc municipalities for better identification of points"""
    query = """
    alter table aggregated_results 
    drop column if exists muni;
    alter table aggregated_results
    add column muni varchar(50);
    UPDATE aggregated_results 
    SET muni=subquery.mun_name
    FROM (select 
        id, dvrpc_munis.mun_name
        from target_points tp
        inner join dvrpc_munis
        on st_intersects(tp.geometry, dvrpc_munis.geometry)
        group by id, dvrpc_munis.mun_name
        order by id) AS subquery
    WHERE aggregated_results.iso_id=subquery.id;"""
    print(f"joining main table to DVRPC municipalities...")
    engine.execute(query)


if __name__ == "__main__":
    import_points("dock_no_freight.geojson")
    import_osmnx(target_network)
    import_taz()
    import_attractions()
    import_dvrpc_munis()
    osmnx_to_pg_routing()
    neighbor_obj = nearest_node()
    make_isochrones(neighbor_obj[0], neighbor_obj[1], 15, 35)
    make_isochrones(neighbor_obj[0], neighbor_obj[1], 30, 35)
    make_hulls(15)
    make_hulls(30)
    calculate_taz_demand(15)
    calculate_taz_demand(30)
    aggregate_demand(15, 30)
    calculate_attractions_and_demand_in_isos(15)
    calculate_attractions_and_demand_in_isos(30)
    calculate_population_in_isos(15)
    calculate_population_in_isos(30)
    pickup_munis()

    # todo: do we need "len_feet" column? is it useful/used anywhere, if not, should be deleted as it's confusing since units are dynamic now
    # todo: add taz automation, insertion of philly_nj and nj_philly tables
