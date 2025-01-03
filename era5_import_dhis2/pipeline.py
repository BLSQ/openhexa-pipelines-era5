import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("__pipeline_id__", name="ERA5 Import DHIS2")
@parameter(
    "input_dir",
    type=str,
    name="Input directory",
    help="Input directory with ERA5 aggregate statistics",
    default="data/era5/aggregate",
    required=True,
)
@parameter(
    "output_dir",
    type=str,
    name="Output directory",
    help="Output directory for the DHIS2 import reports",
    default="data/era5/import",
    required=True,
)
@parameter(
    "dhis2_connection",
    type=DHIS2Connection,
    name="Target DHIS2 instance",
    help="Target DHIS2 instance",
    required=True,
)
@parameter(
    "frequency",
    type=str,
    name="Frequency",
    choices=["weekly", "monthly"],
    help="Temporal aggregation frequency",
    required=True,
)
@parameter(
    "dhis2_dataset",
    type=str,
    name="DHIS2 dataset",
    help="Target DHIS2 dataset",
    required=True,
)
@parameter(
    "dhis2_dx_temperature",
    type=str,
    name="DHIS2 data element (temperature)",
    help="DHIS2 data element for temperature",
    required=False,
)
@parameter(
    "dhis2_dx_precipitation",
    type=str,
    name="DHIS2 data element (precipitation)",
    help="DHIS2 data element for total precipitation",
    required=False,
)
@parameter(
    "dhis2_dx_humidity",
    type=str,
    name="DHIS2 data element (humidity)",
    help="DHIS2 data element for soil humidity",
    required=False,
)
@parameter(
    "dhis2_coc",
    type=str,
    name="DHIS2 category option combo",
    help="DHIS2 category option combo UID",
    default="HllvX50cXC0",
    required=True,
)
@parameter(
    "import_mode",
    type=str,
    name="Import mode",
    help="Import mode",
    choices=["Append", "Overwrite"],
    default="Append",
    required=True,
)
@parameter(
    "dry_run", type=bool, default=False, name="Dry run", help="Simulate DHIS2 import"
)
def era5_import_dhis2(
    input_dir: str,
    output_dir: str,
    dhis2_connection: DHIS2Connection,
    frequency: str,
    dhis2_dataset: str,
    dhis2_coc: str,
    dhis2_dx_temperature: str | None = None,
    dhis2_dx_precipitation: str | None = None,
    dhis2_dx_humidity: str | None = None,
    import_mode: str = "Append",
    dry_run: bool = False,
):
    """Import ERA5 aggregate statistics into a DHIS2 dataset."""
    input_dir = Path(workspace.files_path, input_dir)
    output_dir = Path(workspace.files_path, output_dir)

    dhis2 = DHIS2(
        connection=dhis2_connection, cache_dir=Path(workspace.files_path, ".cache")
    )

    dx_uids = (dhis2_dx_temperature, dhis2_dx_precipitation, dhis2_dx_humidity)
    variables = (
        "2m_temperature",
        "total_precipitation",
        "volumetric_soil_water_layer_1",
    )

    for dx_uid, variable in zip(dx_uids, variables):
        if dx_uid is None:
            msg = f"Skipping import of variable {variable}: no DHIS2 data element provided"
            current_run.log_warning(msg)
            continue
        else:
            msg = f"Starting import of variable {variable} into DHIS2 data element {dx_uid}"
            current_run.log_info(msg)

        stats = read_aggregate(
            input_dir=Path(input_dir, variable), variable=variable, frequency=frequency
        )

        if import_mode != "Overwrite":
            existing_data = get_existing_data(
                dhis2=dhis2, dataset_uid=dhis2_dataset, stats=stats
            )
            stats = filter_periods(
                stats=stats, existing_data=existing_data, dx_uid=dx_uid
            )

        payload = to_json(stats=stats, dx_uid=dx_uid, coc_uid=dhis2_coc)
        summary = push_data_values(dhis2=dhis2, payload=payload, dry_run=dry_run)
        write_report(
            output_dir=Path(output_dir, variable), payload=payload, summary=summary
        )


@era5_import_dhis2.task
def read_aggregate(input_dir: Path, variable: str, frequency: str) -> pl.DataFrame:
    """Read ERA5 aggregate statistics."""
    fp = Path(input_dir / f"{variable}_{frequency}.parquet")
    if not fp.exists():
        msg = f"File not found: {fp.as_posix()}"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    period_column = {
        "daily": "date",
        "weekly": "week",
        "epi_weekly": "epi_week",
        "monthly": "month",
    }

    stats = pl.read_parquet(fp)

    msg = f"Loaded {len(stats)} data values for variable {variable}"
    current_run.log_info(msg)

    return stats.select(
        pl.col("boundary_id").alias("orgUnit"),
        pl.col(period_column[frequency]).alias("period"),
        pl.col("mean").alias("value"),
    )


@era5_import_dhis2.task
def get_existing_data(
    dhis2: DHIS2, dataset_uid: str, stats: pl.DataFrame
) -> pl.DataFrame:
    """Fetch existing data for a single org unit.

    Used to filter out periods for which data already exists before importing new data.
    """
    org_unit_uid = stats["orgUnit"].unique().to_list()[0]

    data_values = dhis2.data_value_sets.get(
        datasets=[dataset_uid],
        org_units=[org_unit_uid],
        start_date="2018-01-01",
        end_date=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
    )

    return pl.DataFrame(data_values)


@era5_import_dhis2.task
def filter_periods(
    stats: pl.DataFrame, existing_data: pl.DataFrame, dx_uid: str
) -> pl.DataFrame:
    """Filter out periods for which data already exists."""
    if existing_data.is_empty():
        msg = f"Did not found any existing data values for data element {dx_uid}"
        current_run.log_info(msg)
        return stats

    existing_data = existing_data.filter(pl.col("dataElement") == dx_uid)
    existing_periods = existing_data["period"].unique().to_list()

    msg = (
        f"Found {len(existing_periods)} existing periods for data element {dx_uid}. "
        "Filtering payload..."
    )
    current_run.log_info(msg)

    return stats.filter(pl.col("period").is_in(existing_periods).not_())


@era5_import_dhis2.task
def to_json(stats: pl.DataFrame, dx_uid: str, coc_uid: str) -> list[dict]:
    """Convert aggregate dataframe to JSON-like data values."""
    stats = stats.select(
        pl.lit(dx_uid).alias("dataElement"),
        pl.lit(coc_uid).alias("categoryOptionCombo"),
        pl.lit(coc_uid).alias("attributeOptionCombo"),
        pl.col("orgUnit"),
        pl.col("period"),
        pl.col("value").round(2).cast(str).alias("value"),
    )
    return stats.to_dicts()


@era5_import_dhis2.task
def push_data_values(dhis2: DHIS2, payload: list[dict], dry_run: bool) -> dict:
    """Push data values to DHIS2."""
    dhis2.data_value_sets.MAX_POST_DATA_VALUES = 1000
    summary = dhis2.data_value_sets.post(
        data_values=payload,
        import_strategy="CREATE_AND_UPDATE",
        dry_run=dry_run,
        skip_validation=True,
    )

    msg = f"Imported {len(payload)} data values to DHIS2"
    current_run.log_info(msg)

    return summary


@era5_import_dhis2.task
def write_report(output_dir: Path, payload: list[dict], summary: dict) -> None:
    """Write DHIS2 import report to output directory."""
    output_dir = Path(
        output_dir, datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / "payload.json", "w") as f:
        json.dump(payload, f, indent=2)

    with open(output_dir / "report.json", "w") as f:
        json.dump(summary, f, indent=2)

    msg = f"Import report written to {output_dir.as_posix()}"
    current_run.log_info(msg)

    current_run.add_file_output((output_dir / "payload.json").as_posix())
    current_run.add_file_output((output_dir / "report.json").as_posix())


if __name__ == "__main__":
    era5_import_dhis2()
