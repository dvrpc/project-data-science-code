import click
from fy21_sidewalk_data.prep_ridescore_data import main as _prep_ridescore


@click.group()
def main():
    "'sidewalk' is used for the FY21 sidewalk data science work"
    pass


@click.command()
def prep_ridescore():
    """Merge original and delta ridescore inputs """
    _prep_ridescore()


all_commands = [prep_ridescore]

for cmd in all_commands:
    main.add_command(cmd)
