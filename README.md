# summit-spark-migration-workshop

Hands-on lab materials for migrating a PySpark financial analytics pipeline to Snowflake-native Snowpark Python using the Snowflake Migration Accelerator (SMA) and Cortex Code.

---

## hands_on_lab_spark_to_snowflake.ipynb

This is the primary lab notebook. It is a Snowflake Notebook that guides you through the full end-to-end migration of a PySpark pipeline.

### Overview

The lab migrates a financial analytics pipeline — originally written in PySpark using the legacy Spark-Snowflake connector — to native Snowpark Python running inside Snowflake. The conversion is performed by the `spark-migration` skill in Cortex Code (SMA CLI), and the resulting pipeline is validated in this notebook.

### Lab Architecture

```
+------------------------------------------------------------------+
|  PART A - Cortex Code (local machine)                            |
|                                                                  |
|  transaction_pipeline.py  -->  SMA CLI  -->  Snowpark output    |
|  (PySpark source)              (converts)   (converted .py)     |
+--------------------------------+---------------------------------+
                                 | EWI Dashboard (opens in browser)
+--------------------------------v---------------------------------+
|  PART B - This Snowflake Notebook                                |
|                                                                  |
|  ACCOUNTS ---+                                                   |
|              +--> Snowpark Pipeline --> MONTHLY_SPEND_SUMMARY   |
|  TRANSACTIONS+                                                   |
+------------------------------------------------------------------+
```

### Prerequisites

| Requirement | Notes |
|-------------|-------|
| Snowflake account | `SYSADMIN` or equivalent role |
| Cortex Code | Snowflake CLI with AI assistance — used in Part A |
| SMA CLI | Snowflake Migration Accelerator binary — used in Part A |
| Test workload | `test_workload_for_demo/` folder (this notebook lives inside it) |

> The SMA CLI is distributed separately from Cortex Code. If you do not have it, ask your Snowflake account team or SE contact.

---

### Notebook Sections

#### Section 1 — Snowflake Environment Setup
Creates the Snowflake objects used throughout the lab:
- **Warehouse:** `LAB_WH` (X-Small, auto-suspend 60s)
- **Database:** `DEMO_DB`
- **Schema:** `DEMO_DB.FINANCE`

#### Section 2 — Load Source Data
Loads two synthetic finance tables directly from inline data (no external files or stage uploads required):
- **ACCOUNTS** (50 rows): account holders with type (`CHECKING` / `SAVINGS` / `CREDIT`), status (`ACTIVE` / `INACTIVE` / `FROZEN`), and credit limit
- **TRANSACTIONS** (300 rows): `DEBIT`, `CREDIT`, and `TRANSFER` transactions spanning January 2023 to June 2024

These tables represent what the original PySpark pipeline read from Snowflake via the legacy Spark-Snowflake connector.

#### Section 3 — Run the SMA Migration (Cortex Code)
Instructions for running the `spark-migration` skill in Cortex Code locally against `scripts/transaction_pipeline.py`. The SMA CLI converts the PySpark source file to Snowpark Python and opens the EWI (Early Warning Indicator) dashboard in the browser.

#### Section 4 — Review the EWI Dashboard
Walkthrough of the EWI dashboard, which reports conversion issues found in the migrated code — categorised by severity and type.

#### Section 5 — Run the EWI Fixer
Instructions for running the automatic EWI Fixer from within the dashboard:
- **EWI comment handling:** Mark (prefixes resolved items with `[FIXED]` or `[NOT-FIXED]`)
- **Which EWIs to process:** Only pending (first run)

After the fixer completes, return to the notebook to run Section 6.

#### Section 6 — The Converted Snowpark Pipeline
Runs the full Snowpark equivalent of `transaction_pipeline.py` in order:

| Step | Description |
|------|-------------|
| 1 | Read `ACCOUNTS` and `TRANSACTIONS` tables via `session.table()` |
| 2 | Filter active accounts and valid DEBIT/TRANSFER transactions |
| 3 | Apply UDFs: `categorize_amount` (small/medium/large bands) and `fraud_flag` (REVIEW/OK heuristic for high-value CREDIT transactions) |
| 4 | Inner join transactions to active accounts |
| 5 | Aggregate by account, month, and merchant category (`TOTAL_SPEND`, `TRANSACTION_COUNT`, `AVG_TRANSACTION_AMOUNT`, `MAX_TRANSACTION_AMOUNT`, `FLAGGED_TRANSACTIONS`) |
| 6 | Add window function columns: `CUMULATIVE_SPEND`, `SPEND_RANK`, `PREV_MONTH_SPEND`, `MONTH_OVER_MONTH_CHANGE`, `CREDIT_UTILISATION_PCT` |
| 7 | Write results to `MONTHLY_SPEND_SUMMARY` table |

Also includes a PySpark → Snowpark API translation reference table covering key differences (snake_case methods, `get_active_session()`, import paths, UDF signatures, date format strings, and write/read patterns).

#### Section 7 — Validate the Results
SQL and Python cells to verify the pipeline output:
- Row count, distinct accounts/months/categories, total flagged transactions, average credit utilisation
- Top 10 accounts by lifetime spend
- Fraud-flagged rows (CREDIT accounts with high-value transactions)
- Full preview of `MONTHLY_SPEND_SUMMARY` with all window function columns

#### Section 8 — Cleanup (Optional)
Commented-out SQL to drop all lab objects:
```sql
DROP DATABASE IF EXISTS DEMO_DB;
DROP WAREHOUSE IF EXISTS LAB_WH;
```

---

## Repository Structure

```
summit-spark-migration-workshop/
├── hands_on_lab_spark_to_snowflake.ipynb  # Main lab notebook (Snowflake Notebook)
├── notebooks/                             # Additional topic notebooks
│   ├── 00_orchestrator.ipynb
│   ├── 01_data_ingestion_schema.ipynb
│   ├── 02_scd_patterns.ipynb
│   ├── 03_aggregations_pivot_windows.ipynb
│   ├── 03_aggregations_pivot_windows_scos.ipynb
│   ├── 04_functional_pipeline.ipynb
│   └── 05_date_utilities_sql_interop.ipynb
├── scripts/                               # PySpark source files for migration
│   ├── transaction_pipeline.py            # Primary migration target
│   ├── data_transformations.py
│   ├── dataframe_validators.py
│   ├── helper_methods.py
│   └── load_data.py
├── data/                                  # Sample CSV data files
│   ├── accounts.csv
│   └── transactions.csv
├── demo_data.py                           # Data generation helper
└── sample_input_data.csv                  # Sample input for pipeline testing
```
