The repository contains OpenHEXA ETL pipelines to ingest climate data from the ERA5-Land dataset in
the [Climate Data
Store](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=overview). They heavily
rely on the `openhexa.toolbox.era5` package (see [openhexa-toolbox
repo](https://github.com/BLSQ/openhexa-toolbox/tree/main/openhexa/toolbox/era5) for more info).

Three DAGs are available:

* [`era5_extract`](era5_extract/README.md): download/sync raw ERA5 hourly data from the CDS for a given area of interest
* [`era5_aggregate`](era5_aggregate/README.md): aggregate raw hourly data in space and time according to an input geographic
  file (ex: administrative boundaries)
* [`era5_import_dhis2`](era5_import_dhis2/README.md): import ERA5 aggregated climate statistics into DHIS2 datasets

Pipelines documentation is available in the respective subdirectories.
