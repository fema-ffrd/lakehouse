"""
This module contains utility functions for loading and transforming
ras and hms model data into a format suitable for Iceberg tables.

This is a temporary module for example purposes and will be replaced by utilities in hecstac
that automatically format to the specification of the iceberg tables.
"""

import logging

import pandas as pd

SAMPLE_EVENTS = [4, 8, 11, 16032]
SAMPLE_RAS_MODELS = ["blw-bear", "blw-east-fork"]
SAMPLE_HMS_MODELS = ["trinity"]


def convert_wide_to_long(
    df_wide: pd.DataFrame,
    variable_name: str,
    model_id: str,
    event_id: int,
    realization_id: int = 1,
    run_version: str = "v1",
):
    """
    Convert a wide hydraulic dataset (time index + many columns)
    into the long schema designed for iceberg table.

    variable_name: one of ["flow", "stage", "base_flow"]
    """

    # Step 1 — Ensure timestamp index is a column named sim_time
    df = df_wide.copy()
    df.index.name = "sim_time"
    df = df.reset_index()

    # Step 2 — Melt to long format
    df_long = df.melt(
        id_vars=["sim_time"],
        var_name="site_name",
        value_name=variable_name,
    )

    # Step 3 — site_id is just the original column name
    df_long["site_id"] = df_long["site_name"]

    # Step 4 — Add static metadata columns
    df_long["realization_id"] = realization_id
    df_long["model_id"] = model_id
    df_long["event_id"] = event_id
    df_long["run_version"] = run_version

    # Step 5 — Ensure correct column order for Iceberg schema
    df_long = df_long[
        [
            "sim_time",
            "realization_id",
            "model_id",
            "site_id",
            "event_id",
            "run_version",
            variable_name,
        ]
    ]

    df_long["sim_time"] = df_long["sim_time"].astype("datetime64[us]")

    return df_long


def load_sample_hms_data(event_id: int, model_name: str, tseries_type: str = "flow") -> pd.DataFrame:
    s3_path = f"s3://trinity-pilot/stac/prod-support/conformance/event_id={event_id}/hms_model={model_name}/FLOW.pq"
    logging.info(f"Loading data from {s3_path}")
    df = pd.read_parquet(s3_path)
    df.rename(columns={"datetime": "sim_time"}, inplace=True)
    df.set_index("sim_time", inplace=True)
    df.drop(columns=["event_id", "hms_model"], inplace=True)
    logging.info(f"Data loaded with shape {df.shape}")
    return convert_wide_to_long(df, variable_name=tseries_type, model_id=model_name, event_id=event_id)


def load_sample_ras_data(event_id: int, model_name: str, tseries_type: str = "flow") -> pd.DataFrame:
    s3_path = f"s3://trinity-pilot/dev/conformance/simulations/event-data/{event_id}/hydraulics/{model_name}/{tseries_type}_timeseries.pq"
    logging.info(f"Loading data from {s3_path}")
    df = pd.read_parquet(s3_path)
    logging.info(f"Data loaded with shape {df.shape}")
    return convert_wide_to_long(df, variable_name=tseries_type, model_id=model_name, event_id=event_id)
