"""
copy_votes_to_analysis_db.py
----------------------------

Copy the stakeholder votes from the webapp database
to the analysis database.
"""
import os
from pg_data_etl import Database
from dotenv import load_dotenv

load_dotenv()
ANALYSIS_DATABASE_URL = os.getenv("ANALYSIS_DATABASE_URL")
WEBAPP_DATABASE_URL = os.getenv("WEBAPP_DATABASE_URL")

db_webapp = Database.from_uri(WEBAPP_DATABASE_URL)
db_analysis = Database.from_uri(ANALYSIS_DATABASE_URL)

query_webapp_results = """
    with votes as (
        select
            email_address, submitted_on, unnest(trailheads) as gid
        from user_votes uv 
    )
    select
        t.gid as original_gid,
        count(v.gid) as votes,
        ST_Force2D(st_transform(st_setsrid(t.geom, 4326), 26918)) as geom 
    from trailheads t
    left join votes v on v.gid = t.gid
    group by t.gid, t.geom
    order by t.gid
"""

if __name__ == "__main__":
    print("Querying vote results")
    gdf = db_webapp.gdf(query_webapp_results)
    print("Copying vote results to analysis database")
    db_analysis.import_geodataframe(gdf, "votes_from_webapp", gpd_kwargs={"if_exists": "replace"})
