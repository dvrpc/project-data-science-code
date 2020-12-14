import click

from fy21_downingtown import db_io


@click.group()
def main():
    "'downingtown' is used for the FY21 Downingtown Area Transportation Study"
    pass


@click.command()
def setup_db():
    """ Create the SQL database and import data """
    db_io.setup()


all_commands = [
    setup_db,
]

for cmd in all_commands:
    main.add_command(cmd)
