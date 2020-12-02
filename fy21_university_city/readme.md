# University City

## Database setup
```
ucity setup-db
```

## APC Data

Analyze the raw APC data and export a shapefile with the summarized results:

```
ucity process-apc-data
ucity export-shp apc_processed
```

## HTS data

...


## QAQC

Generate an Excel spreadsheet for all pre-defined QAQC processes:

##### APC records with null lat/long values:

```
ucity qaqc apc_with_null_latlngs
```
