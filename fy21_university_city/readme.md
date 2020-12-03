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

Analyze the raw Household Travel Survey (2012-2013) data. For each trip, find the next linked trip, and use the departure/arrival times to figure out if it takes place in the AM or PM peak periods.

```
ucity process-hts-data
ucity export-table hts_2013_aggregated_by_mode
ucity export-table hts_2013_aggregated_all_modes_combined
```

## QAQC

Generate an Excel spreadsheet for all pre-defined QAQC processes:

##### APC records with null lat/long values:

```
ucity qaqc apc_with_null_latlngs
```
