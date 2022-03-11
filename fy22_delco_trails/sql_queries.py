"""
sql_queries.py
--------------

This script defines a series of SQL queries that
identify attributes of interest for a given trailhead,
using a placeholder for the 'gid' value. To use these templates,
replace the OBJECTID_PLACEHOLDER value with a specific number.
"""

trailhead_subquery = """
        select st_transform(geom, 26918) as geom
        from votes_from_webapp
        where original_gid = OBJECTID_PLACEHOLDER
"""

isochrone_subquery = """
        select st_transform(geom, 26918) as geom
        from isochrones_2mi
        where poi_uid = 'OBJECTID_PLACEHOLDER'
"""

parks = f"""
    with trailhead as (
        {trailhead_subquery}
    ),
    parks as (
        select st_transform(geom, 26918) as geom
        from openspace
    )
    select
        st_distance(p.geom, t.geom) as distance
    from
        parks p, trailhead t
    order by
        st_distance(p.geom, t.geom) asc 
    limit 1
"""

ipd = f"""
    with trailhead as (
        {trailhead_subquery}
    ),
    equity as (
        select
            st_transform(geom, 26918) as geom,
            ipd_class
        from ipd2019
        where cnty_name = 'Delaware'
    )	
    select ipd_class
    from
        equity q, trailhead t
    where
        st_dwithin(t.geom, q.geom, 804.672)	
    order by st_distance (t.geom, q.geom) asc 
    limit 1
"""

employment = f"""
    with trailhead as (
        {isochrone_subquery}
    ),
    employment as (
        select
            st_transform(geom, 26918) as geom,
            company, emp15
        from nets2015
    )
    select
        count(*) as number_of_employers,
        sum(emp15) as total_employees

    from
        employment e, trailhead t
    where
        st_within(e.geom, t.geom)
"""

destinations = f"""
    with trailhead as (
        {isochrone_subquery}
    ),

    destinations as (
        select
            st_transform(geom, 26918) as geom,
            loc_type
        from eta
    )
    select
        count(*) as number_of_destinations,	
       loc_type as destination_type
    from
        destinations d, trailhead t
    where
        st_within(d.geom, t.geom)
    group by loc_type

"""

septa = f"""
    with trailhead as (
        {isochrone_subquery}
    ),
    stops as (
        select
            st_transform(geom, 26918) as geom,
            lineabbr
        from septastops
    )
    select
        count(s.lineabbr) as number_of_stops,
        s.lineabbr
    from
        stops s, trailhead t
    where
        st_within(s.geom, t.geom)
    group by
        s.lineabbr
"""

population = f"""
    with trailhead_buffer as (
        {isochrone_subquery}
    ),
    blocks as (
        select
            totpop2020,
            st_area(geom) as square_meters,
            geoid,
            st_transform(geom, 26918) as geom
        from census_blocks
        left join block_table
        on geocode = geoid
    )
    select
        sum(b.totpop2020 * st_area(st_intersection(b.geom, t.geom)) / b.square_meters) as scaled_pop

    from blocks as b, trailhead_buffer as t
    where st_intersects(b.geom, t.geom)
"""
