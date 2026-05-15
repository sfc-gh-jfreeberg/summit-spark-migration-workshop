# Example 03 - Notebooks

Notebook conversion example demonstrating the notebook-to-script workflow for both Jupyter (`.ipynb`) and Databricks (`.py`) notebook formats, including cross-notebook `%run` dependencies.

## SMA Format

This example uses **SMA v2 format** (no `.snowma` file). The output structure is:

```
output/
├── sma-output/                              ← <output> = workload_path
│   ├── Output/
│   │   ├── 1_dataprep.ipynb                 ← migrated notebook (Snowpark, .ipynb)
│   │   ├── 2_scoring.ipynb                  ← migrated notebook (Snowpark, .ipynb)
│   │   ├── etl_daily_metrics.py             ← migrated notebook (Snowpark, Databricks .py)
│   │   ├── pipeline_config.py               ← migrated notebook (Snowpark, Databricks .py)
│   │   ├── store_config.ipynb               ← migrated config notebook (%run target)
│   │   └── store_kpi_update.ipynb           ← migrated KPI notebook (calls store_config)
│   ├── Reports/
│   ├── Logs/
│   └── dvp/                                 ← DVP workspace (inside sma-output)
│       ├── 01-source/                        ← converted scripts only (.dbx.py, .ipynb.py)
│       ├── 02-migrated/                      ← converted scripts only (.dbx.py, .ipynb.py)
│       ├── 03-tests/
│       ├── 04-results/
│       └── 05-assets/
├── results/
│   └── sma-code-processes/
│       └── sma-code-process-2026-02-13-18-21-10/   ← historical run
├── Notebook Example 03.snowct               ← project file (no paths)
```

> **Note:** In v2 format, DVP is created inside `sma-output/`, consistent with v1 where DVP lives inside `Conversion-*/`.
>
> **Important:** `01-source/` and `02-migrated/` contain both the original notebooks and the generated script files (`.ipynb.py`, `.dbx.py`). The unmodified originals also remain in `input/` and `sma-output/Output/`.

## Source Files

### Jupyter Notebooks (`.ipynb`)

| File | Description | Cells | Magic Types |
|------|-------------|-------|-------------|
| `input/1_dataprep.ipynb` | Data preparation - loads raw data, cleans, transforms | 45 code + 2 markdown | `!` (shell) |
| `input/2_scoring.ipynb` | Lead scoring - applies scoring model to prepared data | 23 code + 2 markdown | `%%time`, `%matplotlib`, `%%sql` |

### Databricks Notebook Source (`.py`)

| File | Description | Cells | Key Patterns |
|------|-------------|-------|--------------|
| `input/etl_daily_metrics.py` | ETL pipeline - daily metrics aggregation | 15 code + 14 markdown | `%run ./pipeline_config`, PySpark APIs |
| `input/pipeline_config.py` | Shared config - env, credentials, utilities | 6 code + 6 markdown | `dbutils.widgets`, `dbutils.secrets`, `%run` target |
| `input/store_config.py` | Store analytics config - parameters, credentials, utilities | 7 code + 6 markdown | `dbutils.widgets`, `dbutils.jobs.taskValues`, `%run` target |
| `input/store_kpi_update.py` | Store KPI pipeline - aggregates CSAT, SOS, accuracy | 13 code + 10 markdown | `%run ./store_config`, `spark.read.jdbc`, star ratings |

### `%run` Dependencies

```
etl_daily_metrics.py  ──%run──▶  pipeline_config.py (flat)
store_kpi_update.py   ──%run──▶  store_config.py    (flat)
```

Notebooks called via `%run` are converted as **flat scripts** (no `def run()` wrapper) so their variables and functions are injected into the caller's namespace.

## What This Example Tests

1. **Jupyter notebook conversion** — `.ipynb` → `.ipynb.py`
2. **Databricks notebook conversion** — `.py` (with `# Databricks notebook source`) → `.dbx.py`
3. **`%run` target detection** — Auto-detect which notebooks are called via `%run` and generate flat scripts
4. **Cross-notebook dependencies** — `nb.run()` executes the target in shared namespace
5. **SMA v2 format detection** — `.snowct` file (no paths), auto-detect `sma-output/` folder
6. **Markdown cell handling** — Markdown cells converted to `#` comments
7. **Cell tracking** — Sequential cell numbering across all cell types
8. **Magic command handling** — `%%time`, `%matplotlib`, `%%sql`, `!pip`, `%run`
9. **EWI preservation** — `#EWI:` comments retained in migrated scripts
10. **Scripts-only output** — `01-source/` and `02-migrated/` contain only `.dbx.py`/`.ipynb.py`, not raw notebooks

## DVP Output Structure

After conversion, the DVP folders contain only scripts:

```
dvp/
├── 01-source/                       ← pre-migration (original notebooks/source)
│   ├── 1_dataprep.ipynb.py          (wrapped)
│   ├── 2_scoring.ipynb.py           (wrapped)
│   ├── etl_daily_metrics.dbx.py     (wrapped)
│   ├── pipeline_config.dbx.py       (flat — %run target)
│   ├── store_config.dbx.py          (flat — %run target)
│   └── store_kpi_update.dbx.py      (wrapped)
├── 02-migrated/                     ← post-migration (SMA Output/)
│   ├── 1_dataprep.ipynb.py          (wrapped)
│   ├── 2_scoring.ipynb.py           (wrapped)
│   ├── etl_daily_metrics.dbx.py     (wrapped)
│   ├── pipeline_config.dbx.py       (flat — %run target)
│   ├── store_config.ipynb.py        (flat — %run target)
│   └── store_kpi_update.ipynb.py    (wrapped)
└── 03-tests/
    └── dvp_notebook_helper.py       ← runtime helper
```

## Path Resolution

DVP auto-detects the v2 format by finding `sma-output/`. The `.snowct` file exists but only contains a project ID (no paths), so `<input>` must be user-provided:

```
<input>  = examples/03 - Notebooks/input       (user-provided)
<output> = examples/03 - Notebooks/output/sma-output  (auto-detected)
```

## DVP Workflow

1. **dvp-orchestrator** detects v2 format, sets up workspace at `<output>/dvp/` (inside `sma-output/`)
2. **dvp-notebook-to-script** converts notebooks in `01-source/` and `02-migrated/` to executable Python scripts
3. Converted scripts (`.ipynb.py`, `.dbx.py`) are added alongside the originals in `01-source/` and `02-migrated/`

## Notes

- Both `.ipynb` and Databricks `.py` formats are auto-detected and converted
- Markdown cells are converted to `#` comments in the output script
- Each cell is wrapped with `nb.cell("NNN")` markers for runtime tracking
- `%run` targets generate flat scripts; all others generate wrapped scripts with `def run()`
- The `dvp_notebook_helper.py` module is placed in `03-tests/` and referenced via relative path
