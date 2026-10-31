# budget_v1

A small personal-budget pipeline on Databricks. It ingests bank transaction CSV exports
(RBC format), cleans and deduplicates them, and builds monthly aggregates. Everything is
deployed with [Declarative Automation Bundles](https://docs.databricks.com/dev-tools/bundles/index.html).

## How it works

```
CSV files in a Volume ──► bronze ──► silver ──► gold
```

Raw files are uploaded to `/Volumes/workspace/budget/raw/transactions/` (named
`download-transactions*.csv`). A file-arrival job triggers the pipeline whenever a new file lands.

| Layer  | Table                    | Type              | What it does |
|--------|--------------------------|-------------------|--------------|
| Bronze | `bronze_transactions_v2` | Streaming table   | Incrementally reads new CSVs with `read_files`, renames columns, keeps the source file name and ingestion time, and captures malformed rows in `_rescued_data`. |
| Silver | `silver_transactions_v2` | Materialized view | Removes duplicates (see below), parses dates and amounts, assigns a stable `transaction_id` (SHA-256 of the row's fields), flips the sign so spending is positive, and drops credit-card payment rows (`AUTOMATIC PAYMENT`). |
| Gold   | `gold_transactions_v2`   | Materialized view | Total, count and average amount per account per month. |

**Deduplication.** Bank exports often overlap, so the same transaction can appear in several files.
The silver layer numbers identical rows *within* each file, so genuine repeats such as two identical
coffees on one day are kept. Across files, it keeps only the first file that produced each
(transaction, occurrence) pair.

Data quality is enforced with pipeline expectations: rows that fail to parse are flagged, and rows
without a valid date are dropped.

## Project layout

```
databricks.yml                         # Bundle definition, variables and targets
resources/
  etl.pipeline.yml                     # Lakeflow Declarative Pipeline (serverless)
  autorefresh_job.yml                  # Job that runs the pipeline when a new file arrives
src/transaction_transformation/
  my_transformation.sql                # Bronze, silver and gold definitions
```

## Configuration

| Variable  | Default         | Meaning |
|-----------|-----------------|---------|
| `catalog` | `workspace`     | Unity Catalog catalog for the output tables |
| `schema`  | `budget_bundle` | Schema for the output tables |

Before you deploy, make sure the source Volume `/Volumes/workspace/budget/raw/transactions/` exists.
If you use a different path, update it in `my_transformation.sql` and `autorefresh_job.yml`.

## Deploy and run

You need the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) and a Unity
Catalog-enabled workspace with serverless compute.

```bash
databricks configure                        # authenticate (once)
databricks bundle validate                  # check the config
databricks bundle deploy --target dev       # deploy a development copy
databricks bundle run rbc_transactions_etl  # run the pipeline manually
```

In `dev` mode, deployed resources get a `[dev <your-name>]` prefix and triggers are paused by
default. Upload a CSV to the Volume and run the pipeline manually, or unpause the job in the UI.

## Data and privacy

No transaction data is stored in this repository. Raw exports live only in the Databricks Volume.
Don't commit CSV files.
