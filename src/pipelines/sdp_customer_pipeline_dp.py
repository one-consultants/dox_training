# =============================================================================
# sdp_customer_pipeline_dp.py
#
# Same logic as sdp_customer_pipeline.py but uses the MODERN  @dp  API
# (`from pyspark import pipelines as dp`) instead of the legacy  @dlt  API.
#
# Key differences demonstrated here vs the dlt version:
#   1. Import:       `from pyspark import pipelines as dp`  (not `import dlt`)
#   2. Decorators:   `@dp.table()`  `@dp.expect()`  etc.
#   3. Reads:        `spark.readStream.table("name")`  (not `dlt.read_stream()`)
#   4. New decorator: `@dp.materialized_view()` available  (no dlt equivalent)
#
# Config path is read from the Spark conf key  `pipeline.config_path`.
# The bundle sets it in resources/sdp_customer_pipeline_dp.pipeline.yml:
#   pipeline.config_path = ${workspace.file_path}/src/pipelines/config/${var.dp_config_file}
# =============================================================================

import json
import os
from pyspark import pipelines as dp          # ← modern API
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp, current_date,
)

# ---------------------------------------------------------------------------
# 1.  Load configuration  (identical to the dlt version)
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    """Read the JSON config file.  Path priority:
    1. Spark conf  `pipeline.config_path`  (set by the bundle / pipeline settings)
    2. config/sdp_pipeline_config_dp.json  next to this script
    """
    try:
        config_path = spark.conf.get("pipeline.config_path")
    except Exception:
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "config",
            "sdp_pipeline_config_dp.json",
        )
    with open(config_path, "r") as f:
        return json.load(f)


CFG = _load_config()


# ---------------------------------------------------------------------------
# 2.  Helpers: expectations + transforms  (using dp.* instead of dlt.*)
# ---------------------------------------------------------------------------

def _apply_expectations(cfg_expectations: dict):
    """Build dp expectation decorators from config."""
    decorators = []
    for name, expr in cfg_expectations.get("warn", {}).items():
        decorators.append(dp.expect(name, expr))              # dp, not dlt
    for name, expr in cfg_expectations.get("drop", {}).items():
        decorators.append(dp.expect_or_drop(name, expr))      # dp, not dlt
    for name, expr in cfg_expectations.get("fail", {}).items():
        decorators.append(dp.expect_or_fail(name, expr))      # dp, not dlt
    return decorators


def _decorate(fn, decorators):
    """Apply a list of decorators to a function."""
    for dec in reversed(decorators):
        fn = dec(fn)
    return fn


def _apply_silver_transforms(df, transforms: dict):
    """Apply config-driven column transforms to a DataFrame."""
    for c in transforms.get("trim_columns", []):
        df = df.withColumn(c, trim(col(c)))
    for c in transforms.get("upper_columns", []):
        df = df.withColumn(c, upper(trim(col(c))))
    for c, default_val in transforms.get("replace_negatives", {}).items():
        df = df.withColumn(c, when(col(c) < 0, default_val).otherwise(col(c)))
    df = df.withColumn("ingest_date", current_date())
    drop_cols = transforms.get("drop_columns", [])
    if drop_cols:
        df = df.drop(*drop_cols)
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  CUSTOMER  tables  (top-level config keys: source, bronze, silver)
# ═══════════════════════════════════════════════════════════════════════════

cust_source = CFG["source"]
cust_bronze = CFG["bronze"]
cust_silver = CFG["silver"]


# ---- customer bronze (Auto Loader) ------------------------------------

@dp.table(                                                    # dp.table
    name=cust_bronze["table_name"],
    comment=cust_bronze["comment"],
    table_properties=cust_bronze.get("table_properties", {}),
)
def customer_bronze():
    """Streaming Table – Auto Loader ingestion (dp API)."""
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", cust_source["format"])
    )
    for k, v in cust_source.get("options", {}).items():
        reader = reader.option(k, v)
    if cust_source.get("schema"):
        reader = reader.schema(cust_source["schema"])
    return (
        reader.load(cust_source["path"])
        .withColumn("_ingest_ts", current_timestamp())
    )

customer_bronze = _decorate(
    customer_bronze,
    _apply_expectations(cust_bronze.get("expectations", {})),
)


# ---- customer silver (transform) -------------------------------------

@dp.table(                                                    # dp.table
    name=cust_silver["table_name"],
    comment=cust_silver["comment"],
    table_properties=cust_silver.get("table_properties", {}),
)
def customer_silver():
    """Streaming Table – silver layer (dp API).
    Uses spark.readStream.table() — the preferred read method.
    """
    df = spark.readStream.table(cust_bronze["table_name"])    # NOT dlt.read_stream
    return _apply_silver_transforms(df, cust_silver.get("transforms", {}))

customer_silver = _decorate(
    customer_silver,
    _apply_expectations(cust_silver.get("expectations", {})),
)


# ═══════════════════════════════════════════════════════════════════════════
#  ORDERS  tables  (nested under "orders" key in config)
# ═══════════════════════════════════════════════════════════════════════════

ord_cfg    = CFG["orders"]
ord_source = ord_cfg["source"]
ord_bronze = ord_cfg["bronze"]
ord_silver = ord_cfg["silver"]


# ---- orders bronze (Auto Loader) -------------------------------------

@dp.table(
    name=ord_bronze["table_name"],
    comment=ord_bronze["comment"],
    table_properties=ord_bronze.get("table_properties", {}),
)
def orders_bronze():
    """Streaming Table – orders Auto Loader ingestion (dp API)."""
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", ord_source["format"])
    )
    for k, v in ord_source.get("options", {}).items():
        reader = reader.option(k, v)
    if ord_source.get("schema"):
        reader = reader.schema(ord_source["schema"])
    return (
        reader.load(ord_source["path"])
        .withColumn("_ingest_ts", current_timestamp())
    )

orders_bronze = _decorate(
    orders_bronze,
    _apply_expectations(ord_bronze.get("expectations", {})),
)


# ---- orders silver (transform) ----------------------------------------

@dp.table(
    name=ord_silver["table_name"],
    comment=ord_silver["comment"],
    table_properties=ord_silver.get("table_properties", {}),
)
def orders_silver():
    """Streaming Table – orders silver layer (dp API)."""
    df = spark.readStream.table(ord_bronze["table_name"])     # preferred read
    return _apply_silver_transforms(df, ord_silver.get("transforms", {}))

orders_silver = _decorate(
    orders_silver,
    _apply_expectations(ord_silver.get("expectations", {})),
)
