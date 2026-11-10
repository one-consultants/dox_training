# dox_training
Dbx training Repository 

This repo is a **Declarative Automation Bundle**. CI/CD runs through GitHub Actions.

## Layout

```
dox_training/
├── databricks.yml                         # bundle config: variables + dev/prod targets
├── resources/                             # one YAML per deployed resource
│   ├── sdp_customer_pipeline_dlt.pipeline.yml
│   ├── sdp_customer_pipeline_dp.pipeline.yml
│   └── sdp_customer_pipelines.job.yml     # runs dlt pipeline -> dp pipeline
├── src/
│   ├── pipelines/
│   │   ├── sdp_customer_pipeline.py       # @dlt API
│   │   ├── sdp_customer_pipeline_dp.py    # @dp API
│   │   └── config/
│   │       ├── sdp_pipeline_config_dlt.json   # tables dlt_customer_*
│   │       └── sdp_pipeline_config_dp.json    # tables dp_customer_*, dp_orders_*
│   └── notebooks/                         # training notebooks
└── .github/workflows/databricks-bundle.yml
```

## Pipelines and job

| Resource | Code | Config variable | Tables (in `${catalog}.${schema}`) |
|---|---|---|---|
| `sdp_customer_pipeline_dlt` | `src/pipelines/sdp_customer_pipeline.py` | `dlt_config_file` | `dlt_customer_bronze`, `dlt_customer_silver` |
| `sdp_customer_pipeline_dp` | `src/pipelines/sdp_customer_pipeline_dp.py` | `dp_config_file` | `dp_customer_bronze`, `dp_customer_silver`, `dp_orders_bronze`, `dp_orders_silver` |
| `sdp_customer_pipelines_job` | runs the two pipelines in sequence | - | - |

Defaults: catalog `training`, schema `default`, serverless compute.

The config file name is a **bundle variable**. Each pipeline receives it as `pipeline.config_path`. Pipeline tasks in a job cannot take job parameters, so pass a different file at deploy time instead:

```
databricks bundle deploy -t dev --var="dp_config_file=my_config.json"
```

## Commands

```
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev sdp_customer_pipelines_job
```

## CI/CD (GitHub Actions)

* Pull request to `main`: `bundle validate -t dev`
* Push to `main`: validate, `bundle deploy -t prod`, run the job
* Repository secrets needed: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (service principal, OAuth M2M)
* Prod deploys to `/Workspace/Users/<service principal>/.bundle/dox_training/prod`
