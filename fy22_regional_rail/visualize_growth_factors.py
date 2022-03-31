import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from pg_data_etl import Database

load_dotenv(find_dotenv())

FY22_RR = os.getenv("FY22_RR")
db = Database.from_uri(FY22_RR)

folder = Path(
    "/Volumes/GoogleDrive-106022133977257806176/Shared drives/FY22 Regional Rail Fare Equity/TOD"
)
input_folder = folder / "data-inputs"
output_folder = folder / "data-outputs"
excel_file = (
    input_folder / "TIM2_2020_2050_Demographic_Employment_Data - growth from 2020 to 2045.xlsx"
)
shapefile = input_folder / "data-inputs" / "2010_TAZ.shp"


def import_data():
    """
    Import excel sheets and shapefile to postgres
    """

    for sheet in ["totalchange2020_2045", "pctchange2020_2045"]:
        db.import_file_with_pandas(
            excel_file,
            f"taz_{sheet}",
            pd_read_kwargs={"sheet_name": sheet, "skiprows": 5},
            df_import_kwargs={"if_exists": "replace"},
        )

    db.import_gis(
        filepath=shapefile,
        sql_tablename="tazs",
        explode=True,
        gpd_kwargs={"if_exists": "replace"},
    )


def join_and_export_geojsons():
    """
    Use SQL to extract necessary datasets from postgres as geojson
    """
    taz_shapes = """
        select
            co_name, mun_name, subcogeog, tazt,
            st_transform(st_collect(geom), 4326) as geom
        from tazs
        group by co_name, mun_name, subcogeog, tazt
    """

    join_query = f"""
        with taz_centroids as (
            select
                co_name, mun_name, subcogeog, tazt,
                st_centroid(st_transform(st_collect(geom), 4326)) as geom
            from tazs
            group by co_name, mun_name, subcogeog, tazt
        )
        select c.*, d.*
        from taz_centroids c
        left join JOIN_TABLE_PLACEHOLDER d
        on c.tazt = d.zoneno::text
    """

    geojsons_to_export = {
        "taz_polygons": taz_shapes,
        "taz_centroids_w_total_change": join_query.replace(
            "JOIN_TABLE_PLACEHOLDER", "taz_totalchange2020_2045"
        ),
        "taz_centroids_w_pct_change": join_query.replace(
            "JOIN_TABLE_PLACEHOLDER", "taz_pctchange2020_2045"
        ),
    }

    for filename, query in geojsons_to_export.items():
        print(filename)
        print(query)

        gdf = db.gdf(query)

        output_filepath = folder / "data-outputs" / f"{filename}.geojson"
        gdf.to_file(output_filepath, driver="GeoJSON")


def make_vector_tiles():
    """
    Convert geojsons to individual tile files, and merge them all together into one set
    """
    # Make individual tile files
    for filepath in output_folder.rglob("*.geojson"):
        cmd = f'tippecanoe -o "{output_folder / filepath.stem}.mbtiles" -l {filepath.stem} -f -r1 -pk -pf "{filepath}"'
        os.system(cmd)

    # Merge all tiles together into a singular set
    output_mbtile = output_folder / f"taz_growth_2020_to_2045.mbtiles"
    cmd = f'tile-join -n taz_growth_2020_to_2045 -pk -f -o "{output_mbtile}"'
    for f in folder.rglob("*.mbtiles"):
        if "taz_growth_2020_to_2045" not in str(f):
            cmd += f' "{f}"'
    os.system(cmd)


if __name__ == "__main__":
    import_data()
    join_and_export_geojsons()
    make_vector_tiles()
