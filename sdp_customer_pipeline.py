# =============================================================================
# sdp_customer_pipeline.py
# Configuration-driven SDP pipeline: bronze (Auto Loader) + silver (transform)
# in a single file.  All tuneable parameters live in sdp_pipeline_config.json.
#
# Config path is read from the Spark conf key  `pipeline.config_path`.
# If the key is not set, the pipeline looks for the JSON file in the same
# directory as this script.
#
# To set the key, add this to the pipeline's Configuration section in the UI:
#   pipeline.config_path  =  /Workspace/Repos/training/dox_training/sdp_pipeline_config.json
# =============================================================================

import json
import os
import dlt
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp, current_date,
)

# ---------------------------------------------------------------------------
# 1.  Load configuration
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    """Read the JSON config file.  Path priority:
    1. Spark conf  `pipeline.config_path`  (set in the pipeline UI)
    2. Co-located  sdp_pipeline_config.json  (same folder as this script)
    """
    try:
        config_path = spark.conf.get("pipeline.config_path")
    except Exception:
        # Fall back to file next to this script
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "sdp_pipeline_config.json",
        )
    with open(config_path, "r") as f:
        return json.load(f)


CFG = _load_config()


# ---------------------------------------------------------------------------
# 2.  Helper: apply expectation decorators from config
# ---------------------------------------------------------------------------

def _apply_expectations(cfg_expectations: dict):
    """Return a list of DLT expectation decorators built from the config block.

    cfg_expectations looks like:
        { "warn": {name: expr, ...},
          "drop": {name: expr, ...},
          "fail": {name: expr, ...} }
    """
    decorators = []
    for name, expr in cfg_expectations.get("warn", {}).items():
        decorators.append(dlt.expect(name, expr))
    for name, expr in cfg_expectations.get("drop", {}).items():
        decorators.append(dlt.expect_or_drop(name, expr))
    for name, expr in cfg_expectations.get("fail", {}).items():
        decorators.append(dlt.expect_or_fail(name, expr))
    return decorators


def _decorate(fn, decorators):
    """Apply a list of decorators to a function (outermost-first order)."""
    for dec in reversed(decorators):
        fn = dec(fn)
    return fn


# ---------------------------------------------------------------------------
# 3.  Bronze  - Auto Loader ingestion
# ---------------------------------------------------------------------------

bronze_cfg  = CFG["bronze"]
source_cfg  = CFG["source"]


@dlt.table(
    name=bronze_cfg["table_name"],
    comment=bronze_cfg["comment"],
    table_properties=bronze_cfg.get("table_properties", {}),
)
def customer_bronze():
    """Streaming Table - bronze layer.
    Auto Loader reads only new files; SDP manages checkpoints automatically.
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source_cfg["format"])
    )
    # Apply every option from config
    for key, value in source_cfg.get("options", {}).items():
        reader = reader.option(key, value)

    # Apply explicit schema if provided
    if source_cfg.get("schema"):
        reader = reader.schema(source_cfg["schema"])

    return (
        reader
        .load(source_cfg["path"])
        .withColumn("_ingest_ts", current_timestamp())
    )


# Apply bronze expectations from config
customer_bronze = _decorate(
    customer_bronze,
    _apply_expectations(bronze_cfg.get("expectations", {})),
)


# ---------------------------------------------------------------------------
# 4.  Silver - clean & enrich
# ---------------------------------------------------------------------------

silver_cfg = CFG["silver"]
transforms = silver_cfg.get("transforms", {})


@dlt.table(
    name=silver_cfg["table_name"],
    comment=silver_cfg["comment"],
    table_properties=silver_cfg.get("table_properties", {}),
)
def customer_silver():
    """Streaming Table - silver layer.
    Reads incrementally from bronze; applies config-driven transformations.
    """
    df = dlt.read_stream(bronze_cfg["table_name"])

    # Trim whitespace on configured columns
    for c in transforms.get("trim_columns", []):
        df = df.withColumn(c, trim(col(c)))

    # Upper-case configured columns
    for c in transforms.get("upper_columns", []):
        df = df.withColumn(c, upper(trim(col(c))))

    # Replace negative values with a configured default
    for c, default_val in transforms.get("replace_negatives", {}).items():
        df = df.withColumn(
            c, when(col(c) < 0, default_val).otherwise(col(c))
        )

    # Add ingest_date
    df = df.withColumn("ingest_date", current_date())

    # Drop columns not needed in silver
    drop_cols = transforms.get("drop_columns", [])
    if drop_cols:
        df = df.drop(*drop_cols)

    return df


# Apply silver expectations from config
customer_silver = _decorate(
    customer_silver,
    _apply_expectations(silver_cfg.get("expectations", {})),
)
