import click

from fy21_downingtown import db_io
from fy21_downingtown.db_io import db_connection


@click.group()
def main():
    "'downingtown' is used for the FY21 Downingtown Area Transportation Study"
    pass


@click.command()
def setup_db():
    """ Create the SQL database and import data """
    db_io.setup()


@click.command()
def prep_local_osm_layer():
    """Clip Downingtown roads for MANUAL edits """
    db = db_connection()

    db.make_geotable_from_query(
        """
            SELECT *
            FROM osm_edges
            WHERE st_intersects(geom, (SELECT geom FROM boundary))
        """,
        "osm_downingtown",
        "LINESTRING",
        26918,
    )

    db.table_add_or_nullify_column("osm_downingtown", "travel_lanes", "FLOAT")
    db.table_add_or_nullify_column("osm_downingtown", "parking_lanes", "TEXT")
    db.table_add_or_nullify_column("osm_downingtown", "design_plan", "TEXT")


all_commands = [
    setup_db,
    prep_local_osm_layer,
]

for cmd in all_commands:
    main.add_command(cmd)
