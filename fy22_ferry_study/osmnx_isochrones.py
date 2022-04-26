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
miles = 8.75  # 8.75 is half
round_miles = round(miles)
distance_threshold = (
    miles * 5280
)  # the distance, in the units of above SRID, of the isochrone using the network


def import_points(points):
    """imports target points into your postgres db, i.e. what you want isochrones around"""
    print(GDRIVE_FOLDER)
    gdf = gpd.read_file(f"{GEOJSON_FOLDER}/{points}")
    print("importing target points into database...")
    gdf = gdf.to_crs(f"EPSG:{srid}")
    gdf.to_postgis("target_points", engine, schema=None, if_exists="replace")


def import_osmnx(target_network):
    """imports toplogically sound osmnx network for target_network, defined above"""
    G = ox.graph_from_place(target_network, network_type="drive")
    ox.io.save_graph_geopackage(
        G, filepath=f"{GEOJSON_FOLDER} /graph.gpkg", encoding="utf-8", directed=False
    )
    os.system(f"cd '{GEOJSON_FOLDER}'")
    os.system(
        f"ogr2ogr -f PostgreSQL 'PG:user={USER} password={PW} dbname={DB_NAME}' '{GEOJSON_FOLDER}/graph.gpkg' -progress -overwrite -t_srs EPSG:{srid}"
    )


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
        select iso_id, ST_ConcaveHull(ST_Union(geom), 0.80) as geom from isochrones
        group by iso_id
        );
    select UpdateGeometrySRID('isochrone_hull','geom',{srid});
    """
    print("Creating convex hulls around isochrones, just a moment...")
    engine.execute(hull_query)
    print("Isochrones and hulls created, see database for results.")


if __name__ == "__main__":
    # import_points("dock_no_freight.geojson")
    # import_osmnx(target_network)

    # osmnx_to_pg_routing()
    neighbor_obj = nearest_neighbor()
    make_isochrones(neighbor_obj[0], neighbor_obj[1])
    make_hulls()

    # engine.dispose()

    # todo: do we need "len_feet" column? is it useful/used anywhere, if not, should be deleted as it's confusing since units are dynamic now
