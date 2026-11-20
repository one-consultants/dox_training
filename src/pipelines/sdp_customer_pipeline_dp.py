# =============================================================================
# sdp_customer_pipeline_dp.py
#
# Config-driven Lakeflow Spark Declarative Pipeline (dp API).
# Loops over every entry in config "sources" and creates:
#   bronze  - streaming table, Auto Loader, append only
#   silver  - streaming table, cleaned columns
#   gold    - materialized view, aggregation (only if "gold" is in the config)
#
# Config path comes from the pipeline setting  pipeline.config_path
# (set by the bundle in resources/sdp_customer_pipeline_dp.pipeline.yml).
# =============================================================================

import json
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp, current_date, date_trunc, expr,
)

with open(spark.conf.get("pipeline.config_path")) as f:
    CFG = json.load(f)


def expectations(rules):
    """Decorator that adds warn / drop / fail expectations from config."""
    def wrap(fn):
        if rules.get("warn"):
            fn = dp.expect_all(rules["warn"])(fn)
        if rules.get("drop"):
            fn = dp.expect_all_or_drop(rules["drop"])(fn)
        if rules.get("fail"):
            fn = dp.expect_all_or_fail(rules["fail"])(fn)
        return fn
    return wrap


# ---- bronze: raw files -> streaming table (append only) -------------------

def create_bronze(cfg):
    src, bronze = cfg["source"], cfg["bronze"]

    @expectations(bronze.get("expectations", {}))
    @dp.table(
        name=bronze["table_name"],
        comment=bronze["comment"],
        table_properties=bronze.get("table_properties", {}),
    )
    def bronze_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", src["format"])
            .options(**src.get("options", {}))
            .schema(src["schema"])
            .load(src["path"])
            .withColumn("_ingest_ts", current_timestamp())
        )


# ---- silver: cleaned streaming table ---------------------------------------

def create_silver(cfg):
    bronze, silver = cfg["bronze"], cfg["silver"]
    t = silver.get("transforms", {})

    @expectations(silver.get("expectations", {}))
    @dp.table(
        name=silver["table_name"],
        comment=silver["comment"],
        table_properties=silver.get("table_properties", {}),
    )
    def silver_table():
        df = spark.readStream.table(bronze["table_name"])
        for c in t.get("trim_columns", []):
            df = df.withColumn(c, trim(col(c)))
        for c in t.get("upper_columns", []):
            df = df.withColumn(c, upper(trim(col(c))))
        for c, default in t.get("replace_negatives", {}).items():
            df = df.withColumn(c, when(col(c) < 0, default).otherwise(col(c)))
        return (
            df.withColumn("ingest_date", current_date())
            .drop(*t.get("drop_columns", []))
        )


# ---- gold: aggregated materialized view -------------------------------------

def create_gold(cfg):
    silver, gold = cfg["silver"], cfg["gold"]
    period_col = gold["period_column_name"]

    @expectations(gold.get("expectations", {}))
    @dp.materialized_view(
        name=gold["table_name"],
        comment=gold["comment"],
        table_properties=gold.get("table_properties", {}),
    )
    def gold_table():
        return (
            spark.read.table(silver["table_name"])
            .withColumn(period_col, date_trunc(gold["period"], col(gold["date_column"])).cast("date"))
            .groupBy(*gold["key_columns"], period_col)
            .agg(*[expr(e).alias(name) for name, e in gold["aggregations"].items()])
        )


# ---- build every source from config ---------------------------------------

for cfg in CFG["sources"]:
    create_bronze(cfg)
    create_silver(cfg)
    if "gold" in cfg:
        create_gold(cfg)

