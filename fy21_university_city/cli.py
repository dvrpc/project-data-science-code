import click

from fy21_university_city import db_io
from fy21_university_city import process_apc_data as apc
from fy21_university_city import qaqc as qa


@click.group()
def main():
    "'ucity' is used for the FY21 University City Multimodal Master Plan"
    pass


@click.command()
def setup_db():
    """ Create the SQL database and import data """

    db_io.setup()


@click.command()
@click.argument("table_name")
def export_shp(table_name):
    """ Export a spatial table to shapefile """

    db_io.export_shp(table_name)


@click.command()
@click.argument("qaqc_process_name")
def qaqc(qaqc_process_name):
    """Run a pre-defined QAQC check"""

    qa.main(qaqc_process_name)


@click.command()
def process_apc_data():
    """ Work with APC data """

    apc.main()


all_commands = [
    setup_db,
    export_shp,
    qaqc,
    process_apc_data,
]

for cmd in all_commands:
    main.add_command(cmd)
