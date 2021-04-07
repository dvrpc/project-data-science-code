from tqdm import tqdm
from postgis_helpers import PostgreSQL

from fy21_university_city import GDRIVE_FOLDER
from fy21_university_city.db_io import db_connection


GIS_FOLDER = GDRIVE_FOLDER / "Data/GIS"


def prep_data(db: PostgreSQL):
    """
    1) Import necessary shapefiles to PostGIS
    2) Extract nodes from street segments within study area
    3) Assign closest street node ID to all SEPTA bus stops near study area
    """

    # 1) Import necessary shapefiles to PostGIS
    # -----------------------------------------

    all_tables = db.all_tables_as_list()

    for sql_tablename, shp_path_suffix in [
        ("philly_streets", "philly complete streets/philly_complete_streets.shp"),
        ("septa_bus_stops_fall_2019", "Fall_2019_Stops_By_Route/Fall_2019_Stops_By_Route.shp"),
        ("study_bounds", "Draft_Study_Area_Extent/U_CIty_Study_Area_Dissolve_2.shp"),
    ]:
        if sql_tablename not in all_tables:
            full_path = GIS_FOLDER / shp_path_suffix
            db.import_geodata(sql_tablename, full_path)

    # 2) Extract nodes from street segments within study area
    # -------------------------------------------------------
    point_query = """
        with raw as (
            select fnode_, tnode_, st_startpoint(geom) as startpoint, st_endpoint(geom) as endpoint
            from philly_streets
            where st_intersects(geom, (select st_collect(geom) from study_bounds))
        ),
        merged_data as (
            select fnode_ as nodeid, startpoint as geom from raw
            union
            select tnode_ as nodeid, endpoint as geom from raw
        )
        select nodeid, geom
        from merged_data
        group by nodeid, geom
    """
    db.make_geotable_from_query(point_query, "street_nodes", "POINT", 26918)

    # 3) Assign closest street node ID to all SEPTA bus stops near study area
    # -----------------------------------------------------------------------
    df = db.query_as_df(
        """
        select gid
        from septa_bus_stops_fall_2019
        where
            st_dwithin(
                st_transform(geom, 26918),
                (select st_collect(geom) from study_bounds sb),
                30
            )
    """
    )

    db.table_add_or_nullify_column("septa_bus_stops_fall_2019", "nearest_node", "int")

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        gid = row[0]
        query = f"""
            select
                n.nodeid
            from
                septa_bus_stops_fall_2019 s,
                street_nodes n
            where
                s.gid = {gid}
            order by
                st_distance(st_transform(s.geom, 26918), n.geom) asc
            limit 1
        """
        node_id = db.query_as_single_item(query)
        update = f"""
            update septa_bus_stops_fall_2019
            set nearest_node = {node_id}
            where gid = {gid}
        """
        db.execute(update)


def export_data(db: PostgreSQL):
    """
    Using the prepared data, extract a point shapefile to disk.
    Geometry is the intersection nodes that are near bus stops.
    Attributes include the total number of weekday boardings, alightings,
    and a list of the bus routes and stop IDs that contributed to the sums.
    """

    # Aggregate boardings/alightings by street node id and join to geometry
    join_query = """
        with ridership_data as (
            select
                nearest_node,
                sum(weekday_bo) as boardings,
                sum(weekday_le) as alightings,
                array_agg(route) as routes,
                array_agg(gid) as stopgid
            from
                septa_bus_stops_fall_2019
            where
                nearest_node is not null
            group by
                nearest_node
        )
        select
            row_number() over() as uid,
            d.*,
            n.geom
        from
            ridership_data d
        left join
            street_nodes n
        on
            n.nodeid = d.nearest_node
    """
    gdf = db.query_as_geo_df(join_query)

    # The SQL array_agg() gets translated into a list by geopandas
    # and shapefiles don't support this data type
    # so instead, convert list to comma-delimited-list first
    for col in ["routes", "stopgid"]:
        gdf[col] = gdf[col].apply(lambda x: ",".join(map(str, x)))

    # Write to disk
    gdf.to_file(GIS_FOLDER / "bus_stops_with_ridership_agg.shp")


def main():
    db = db_connection()

    prep_data(db)
    export_data(db)


if __name__ == "__main__":
    main()
