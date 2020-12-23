import osmnx as ox

from postgis_helpers import PostgreSQL

from fy21_downingtown import DB_NAME


def db_connection(db_name: str = DB_NAME) -> PostgreSQL:
    return PostgreSQL(db_name, verbosity="minimal")


def setup(db_name: str = DB_NAME):

    db = db_connection(db_name)

    # Get the boundary of Downingtown PA
    gdf_bounds = ox.geocode_to_gdf({"city": "Downingtown", "state": "PA"})
    gdf_bounds = gdf_bounds.to_crs("EPSG:26918")

    one_mile_in_meters = 1609.34
    five_miles_in_meters = one_mile_in_meters * 5

    gdf_bounds_buffer = gdf_bounds.copy()

    gdf_bounds_buffer["geometry"] = gdf_bounds.geometry.buffer(five_miles_in_meters)

    # Get all OSM road features
    polygon = gdf_bounds_buffer.to_crs("EPSG:4326").geometry[0]
    g = ox.graph_from_polygon(polygon)
    g = g.to_undirected()
    nodes, edges = ox.graph_to_gdfs(g)

    db.import_geodataframe(gdf_bounds, "boundary")
    db.import_geodataframe(gdf_bounds_buffer, "boundary_5mi_buffer")

    db.import_geodataframe(edges, f"osm_edges")
    db.import_geodataframe(nodes, f"osm_nodes")

    # Reproject from 4326 to 26918 to facilitate analysis queries
    db.table_reproject_spatial_data(f"osm_edges", 4326, 26918, "LINESTRING")
    db.table_reproject_spatial_data(f"osm_nodes", 4326, 26918, "POINT")

    # Make a uuid column
    make_id_query = f"""
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        alter table osm_edges add column osmuuid uuid;
        update osm_edges set osmuuid = uuid_generate_v4();
    """
    db.execute(make_id_query)


if __name__ == "__main__":
    pass
