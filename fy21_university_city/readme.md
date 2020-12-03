# University City Multimodal Capacity Study

> Project # 21-52-100
___

## Database setup
Spin up a new PostgreSQL database and import the necessary source datasets.

This will only work if your GoogleDrive is mapped to include `U_City_FY_21`
under your `Shared Drives` folder.

```
ucity setup-db
```

## APC Data

Analyze the raw APC (automated person counter) data by stop and route, and export a shapefile with the summarized results:

```
ucity process-apc-data
ucity export-shp apc_processed
```

## HTS data

Analyze the raw [Household Travel Survey (2012-2013)](https://www2.dvrpc.org/Reports/14033.pdf) data. For each trip, find the next linked trip, and use the departure/arrival times to figure out if it takes place in the AM or PM peak periods.

```
ucity process-hts-data
ucity export-table hts_2013_aggregated_by_mode
ucity export-table hts_2013_aggregated_all_modes_combined
```

## QAQC

Generate an Excel spreadsheet for all pre-defined QAQC processes:

-  APC records with null lat/long values:

```
ucity qaqc apc_with_null_latlngs
```

-  HTS trips with null values for both CPAs:

```
ucity qaqc hts_with_two_null_cpas
```
