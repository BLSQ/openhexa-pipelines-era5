from io import BytesIO
from pathlib import Path

import geopandas as gpd
import polars as pl
import xarray as xr
from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from openhexa.sdk.datasets import DatasetFile
from openhexa.toolbox.era5.aggregate import (
    aggregate,
    aggregate_per_month,
    aggregate_per_week,
    build_masks,
    get_transform,
    merge,
)
from openhexa.toolbox.era5.cds import VARIABLES


@pipeline("__pipeline_id__", name="ERA5 Aggregate")
@parameter(
    "input_dir",
    type=str,
    name="Input directory",
    help="Input directory with raw ERA5 extracts",
    default="data/era5/raw",
)
@parameter(
    "output_dir",
    type=str,
    name="Output directory",
    help="Output directory for the aggregated data",
    default="data/era5/aggregate",
)
@parameter(
    "boundaries_dataset",
    name="Boundaries dataset",
    type=Dataset,
    help="Input dataset containing boundaries geometries",
    required=True,
)
@parameter(
    "boundaries_file",
    name="Boundaries filename in dataset",
    type=str,
    help="Filename of the boundaries file to use in the boundaries dataset",
    required=False,
    default="district.parquet",
)
@parameter(
    "boundaries_column_uid",
    name="Boundaries column UID",
    type=str,
    help="Column name containing unique identifier for boundaries geometries",
    required=True,
    default="id",
)
def era5_aggregate(
    input_dir: str,
    output_dir: str,
    boundaries_dataset: Dataset,
    boundaries_column_uid: str,
    boundaries_file: str | None = None,
):
    input_dir = Path(workspace.files_path, input_dir)
    output_dir = Path(workspace.files_path, output_dir)

    boundaries = read_boundaries(boundaries_dataset, filename=boundaries_file)

    # subdirs containing raw data are named after variable names
    subdirs = [d for d in input_dir.iterdir() if d.is_dir()]
    variables = [d.name for d in subdirs if d.name in VARIABLES.keys()]

    if not variables:
        msg = "No variables found in input directory"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    for variable in variables:
        daily = get_daily(
            input_dir=input_dir / variable,
            boundaries=boundaries,
            variable=variable,
            column_uid=boundaries_column_uid,
        )

        current_run.log_info(
            f"Applied spatial aggregation to {variable} data for {len(boundaries)} boundaries"
        )

        # only apply sum aggregation for accumulated variables such as total precipitation
        sum_aggregation = variable == "total_precipitation"

        weekly = aggregate_per_week(
            daily=daily,
            column_uid=boundaries_column_uid,
            use_epidemiological_weeks=False,
            sum_aggregation=sum_aggregation,
        )

        current_run.log_info(
            f"Applied weekly aggregation to {variable} data ({len(weekly)} rows)"
        )

        epi_weekly = aggregate_per_week(
            daily=daily,
            column_uid=boundaries_column_uid,
            use_epidemiological_weeks=True,
            sum_aggregation=sum_aggregation,
        )

        current_run.log_info(
            f"Applied epi. weekly aggregation to {variable} data ({len(epi_weekly)} rows)"
        )

        monthly = aggregate_per_month(
            daily=daily,
            column_uid=boundaries_column_uid,
            sum_aggregation=sum_aggregation,
        )

        current_run.log_info(
            f"Applied monthly aggregation to {variable} data ({len(monthly)} rows)"
        )

        dst_dir = output_dir / variable
        dst_dir.mkdir(parents=True, exist_ok=True)

        daily.to_parquet(dst_dir / f"{variable}_daily.parquet")
        current_run.add_file_output(
            Path(dst_dir, f"{variable}_daily.parquet").as_posix()
        )

        weekly.to_parquet(dst_dir / f"{variable}_weekly.parquet")
        current_run.add_file_output(
            Path(dst_dir, f"{variable}_weekly.parquet").as_posix()
        )

        epi_weekly.to_parquet(dst_dir / f"{variable}_epi_weekly.parquet")
        current_run.add_file_output(
            Path(dst_dir, f"{variable}_epi_weekly.parquet").as_posix()
        )

        monthly.to_parquet(dst_dir / f"{variable}_monthly.parquet")
        current_run.add_file_output(
            Path(dst_dir, f"{variable}_monthly.parquet").as_posix()
        )


def read_boundaries(
    boundaries_dataset: Dataset, filename: str | None = None
) -> gpd.GeoDataFrame:
    """Read boundaries geographic file from input dataset.

    Parameters
    ----------
    boundaries_dataset : Dataset
        Input dataset containing a "*district*.parquet" geoparquet file
    filename : str
        Filename of the boundaries file to read if there are several.
        If set to None, the 1st parquet file found will be loaded.

    Return
    ------
    gpd.GeoDataFrame
        Geopandas GeoDataFrame containing boundaries geometries

    Raises
    ------
    FileNotFoundError
        If the boundaries file is not found
    """
    ds = boundaries_dataset.latest_version

    ds_file: DatasetFile | None = None
    for f in ds.files:
        if f.filename == filename:
            if f.filename.endswith(".parquet"):
                ds_file = f
            if f.filename.endswith(".geojson") or f.filename.endswith(".gpkg"):
                ds_file = f

    if ds_file is None:
        msg = f"File {filename} not found in dataset {ds.name}"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    if ds_file.filename.endswith(".parquet"):
        return gpd.read_parquet(BytesIO(ds_file.read()))

    return gpd.read_file(BytesIO(ds_file.read()))


def get_daily(
    input_dir: Path, boundaries: gpd.GeoDataFrame, variable: str, column_uid: str
) -> pl.DataFrame:
    fp = None
    for f in input_dir.glob("*.grib"):
        fp = f
        break

    if fp is None:
        msg = f"No GRIB files found in {input_dir}"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    # get raster metadata from 1st grib file available
    ds = xr.open_dataset(fp, engine="cfgrib")
    ncols = len(ds.longitude)
    nrows = len(ds.latitude)
    transform = get_transform(ds)

    # build xarray dataset by merging all available grib files across the time dimension
    ds = merge(input_dir)

    # build binary raster masks for each boundary geometry for spatial aggregation
    masks = build_masks(boundaries, nrows, ncols, transform)

    var = VARIABLES[variable]["shortname"]

    daily = aggregate(ds=ds, var=var, masks=masks, boundaries_id=boundaries[column_uid])

    # kelvin to celsius
    if variable == "2m_temperature":
        daily = daily - 273.15

    # m to mm
    if variable == "total_precipitation":
        daily = daily * 1000

    return daily


def temporal_aggregation(
    daily: xr.Dataset, column_uid: str, variable: str, frequency: str = "weekly"
) -> pl.DataFrame:
    if variable == "total_precipitation":
        sum_aggregation = True
    else:
        sum_aggregation = False

    if frequency == "weekly":
        df = aggregate_per_week(
            daily=daily,
            column_uid=column_uid,
            use_epidemiological_weeks=False,
            sum_aggregation=sum_aggregation,
        )
    elif frequency == "epi_weekly":
        df = aggregate_per_week(
            daily=daily,
            column_uid=column_uid,
            use_epidemiological_weeks=True,
            sum_aggregation=sum_aggregation,
        )
    elif frequency == "monthly":
        df = aggregate_per_month(
            daily=daily, column_uid=column_uid, sum_aggregation=sum_aggregation
        )
    else:
        msg = f"Unsupported frequency: {frequency}"
        current_run.log_error(msg)
        raise ValueError(msg)

    return df
