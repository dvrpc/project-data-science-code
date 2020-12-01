import click

from fy21_university_city import db_io
from fy21_university_city import process_apc_data as apc


@click.group()
def main():
    "'ucity' is used for the FY21 University City Multimodal Master Plan"
    pass


@click.command()
def setup_db():
    """ Create the SQL database and import data """

    db_io.setup()


@click.command()
def process_apc_data():
    """ Work with APC data """

    apc.main()


all_commands = [
    setup_db,
    process_apc_data,
]

for cmd in all_commands:
    main.add_command(cmd)