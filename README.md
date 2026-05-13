# Snowflake Summit Spark Migration Workshop

Hands-on lab materials for migrating a PySpark financial analytics pipeline to Snowflake-native Snowpark Python using the Snowflake Migration Accelerator (SMA) and Cortex Code.

---

## Quick Start

1. Open `hands_on_lab_spark_to_snowflake.ipynb` in a Snowflake Notebook
2. Follow the sections in order — the notebook walks you through environment setup, data loading, running the SMA conversion via Cortex Code, and executing the converted Snowpark pipeline
3. Alternatively, follow the quickstart guide in `summit-spark-migration-quickstart/` for a self-paced web format

---

## Repository Structure

```
summit-spark-migration-workshop/
├── hands_on_lab_spark_to_snowflake.ipynb   # Main lab notebook (Snowflake Notebook)
├── scripts/                                # PySpark source files (migration input)
│   ├── transaction_pipeline.py             #   Primary migration target
│   ├── data_transformations.py
│   ├── dataframe_validators.py
│   ├── helper_methods.py
│   └── load_data.py
├── notebooks/                              # Additional PySpark topic notebooks
│   ├── 00_orchestrator.ipynb
│   ├── 01_data_ingestion_schema.ipynb
│   ├── 02_scd_patterns.ipynb
│   ├── 03_aggregations_pivot_windows.ipynb
│   ├── 03_aggregations_pivot_windows_scos.ipynb
│   ├── 04_functional_pipeline.ipynb
│   └── 05_date_utilities_sql_interop.ipynb
├── data/                                   # Sample CSV data files
│   ├── accounts.csv
│   └── transactions.csv
├── notebooks_converted/                    # SMA conversion output (post-migration)
│   ├── Output/                             #   Converted Snowpark notebooks
│   ├── Reports/                            #   SMA conversion reports (CSV)
│   ├── Logs/                               #   Conversion logs
│   ├── dvp/                                #   Data Validation Pipeline (tests + results)
│   ├── sma-dashboard/                      #   EWI dashboard (HTML + API server)
│   ├── analysis.json                       #   Codebase analysis metadata
│   ├── migration_readiness_report.html     #   Migration readiness assessment
│   └── migration_state.json                #   Migration state tracking
├── spark-migration-skill/                  # Bundled spark-migration skill (zip)
│   └── spark-migration.zip
├── summit-spark-migration-quickstart/      # Snowflake Quickstart version of the lab
│   ├── summit-spark-migration-quickstart.md
│   └── assets/
├── demo_data.py                            # Data generation helper script
├── sample_input_data.csv                   # Sample input for pipeline testing
└── README.md
```

---

## What's in This Repo

### Main Lab Notebook

**`hands_on_lab_spark_to_snowflake.ipynb`** — The primary lab experience. Runs as a Snowflake Notebook and covers:

| Section | What It Does |
|---------|-------------|
| 1. Environment Setup | Creates `LAB_WH`, `DEMO_DB`, `DEMO_DB.FINANCE` |
| 2. Load Source Data | Loads 50 accounts + 300 transactions from inline data |
| 3. PySpark Source Code | Explains the 5 key migration patterns |
| 4. Run Migration Skill | Step-by-step Cortex Code instructions for SMA conversion |
| 5. EWI Dashboard | How to read and resolve conversion warnings |
| 6. Converted Pipeline | Full Snowpark pipeline with API translation notes |
| 7. Validate Results | SQL + Python verification queries |
| 8. Cleanup | Drop all lab objects |

### PySpark Source (`scripts/`)

The original PySpark code that gets migrated:

- **`transaction_pipeline.py`** — Main pipeline: reads ACCOUNTS/TRANSACTIONS via Spark-Snowflake connector, applies UDFs, joins, aggregates by month/merchant, adds window functions, writes to Snowflake
- **`data_transformations.py`** — Transformation utilities
- **`dataframe_validators.py`** — Schema and data quality checks
- **`helper_methods.py`** — Shared helper functions
- **`load_data.py`** — Data loading logic

### Topic Notebooks (`notebooks/`)

Standalone PySpark notebooks covering specific migration patterns:

- **00** — Orchestrator (pipeline coordination)
- **01** — Data ingestion and schema handling
- **02** — Slowly Changing Dimensions (SCD) patterns
- **03** — Aggregations, pivot, and window functions
- **04** — Functional pipeline composition
- **05** — Date utilities and SQL interop

### Conversion Output (`notebooks_converted/`)

Pre-generated SMA conversion output for reference. Includes:

- Converted Snowpark notebooks in `Output/`
- Conversion reports and issue inventories in `Reports/`
- EWI dashboard with tracker UI in `sma-dashboard/`
- Data Validation Pipeline (DVP) with source/migrated test suites in `dvp/`
- Migration readiness report (`migration_readiness_report.html`)

### Spark Migration Skill (`spark-migration-skill/`)

A zipped copy of the Cortex Code `spark-migration` skill bundle for offline/portable use.

### Quickstart (`summit-spark-migration-quickstart/`)

The same lab content reformatted as a [Snowflake Quickstart](https://github.com/Snowflake-Labs/sfquickstarts) — a 10-section markdown guide suitable for publishing on the Snowflake Guides site.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Snowflake account | `SYSADMIN` or equivalent role |
| Cortex Code | Snowflake CLI with AI assistance |
| SMA CLI | Snowpark Migration Accelerator binary |
| Test workload | This repository |

> The SMA CLI is distributed separately from Cortex Code. If you do not have it, ask your Snowflake account team or SE contact.

---

## Lab Architecture

```
+-----------------------------------------------------------------+
|  PART A - Cortex Code (local machine)                          |
|                                                                 |
|  transaction_pipeline.py  -->  SMA CLI  -->  Snowpark output  |
|  (PySpark source)              (converts)   (converted .py)   |
+--------------------------------+--------------------------------+
                                 | EWI Dashboard (opens in browser)
+--------------------------------v--------------------------------+
|  PART B - Snowflake Notebook                                    |
|                                                                 |
|  ACCOUNTS ---+                                                  |
|              +--> Snowpark Pipeline --> MONTHLY_SPEND_SUMMARY  |
|  TRANSACTIONS+                                                  |
+-----------------------------------------------------------------+
```
