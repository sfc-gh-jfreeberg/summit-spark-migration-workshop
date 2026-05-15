# DVP Examples

Sample workloads to exercise and test DVP skills. Each example has been migrated using the real SMA tool, producing authentic `<input>` and `<output>` structures.

## Available Examples

| Example | Description | SMA Format | Pipelines | Files | EWIs |
|---------|-------------|------------|-----------|-------|------|
| [01 - workload-simple-etl](01%20-%20workload-simple-etl/) | Multi-file ETL workload with 3 pipelines (customer ingestion, order processing, daily report), utility modules, and config | v1 | 3 | 6 .py files | - |
| [02 - ECommerceDataPipeline](02%20-%20ECommerceDataPipeline/) | Single-file e-commerce pipeline with 5 CSV inputs, 5 outputs, window functions, embedded SQL, and expected output data for validation | v1 | 1 (multi-output) | 1 .py + CSV data | 20 |
| [03 - Notebooks](03%20-%20Notebooks/) | Jupyter notebook conversion with 2 notebooks (data prep, scoring), testing notebook-to-script conversion | v2 | 2 | 2 .ipynb | - |

## SMA Output Formats

Examples may use different SMA output formats:

| Format | Structure | Project File | Path Resolution | Examples |
|--------|-----------|--------------|-----------------|----------|
| **v1 (legacy)** | `Conversion-<timestamp>/` | `.snowma` (JSON, has paths) | `<input>` and `<output>` from `.snowma` | 01, 02 |
| **v2** | `sma-output/` | `.snowct` (YAML, no paths) | `<output>` auto-detected (`sma-output/`), `<input>` user-provided | 03 |
| **v3** | `Conversion_SnowparkAPI` / `Conversion_SnowparkConnect` + `sma-code-process-*` | `.snowct` (optional, no paths) | Latest `sma-code-process-*` under chosen **`Conversion_SnowparkAPI`** or **`Conversion_SnowparkConnect`**; `<input>` user-provided | — |

## Structure

### v1 Format (Examples 01, 02)

```
<NN> - <name>/
├── <input-folder>/                          # SMA <input> (original PySpark code)
│   ├── *.py                                 # Source files
│   └── <name>.snowma                        # SMA project file (contains all paths)
└── <output-folder>/                         # SMA output root
    └── Conversion-<M>-<DD>-<YYYY>T<HH> <MM>/  # <output> (conversion folder)
        ├── Output/                          # Migrated Snowpark code
        │   ├── *.py
        │   └── <name>.snowma                # Copy of project file
        ├── Reports/                         # SMA inventories and reports
        │   ├── Issues.csv
        │   ├── SparkUsagesInventory.csv
        │   └── ...
        └── Logs/
            └── *.log
```

### v2 Format (Example 03)

```
<NN> - <name>/
├── input/                                   # SMA <input> (original notebooks/code)
│   └── *.ipynb                              # Source notebooks (no .snowma file)
└── output/                                  # SMA output root
    ├── <project-name>.snowct                # Project file (no paths, only project ID)
    ├── sma-output/                          # <output> = workload_path
    │   ├── Output/                          # Migrated Snowpark code
    │   │   └── *.ipynb                      # Notebooks with Snowpark code
    │   ├── Reports/                         # SMA inventories and reports
    │   │   └── ...
    │   ├── Logs/
    │   └── dvp/                             # DVP workspace (inside sma-output)
    │       ├── 01-source/
    │       ├── 02-migrated/
    │       ├── 03-tests/
    │       ├── 04-results/
    │       └── 05-assets/
    ├── results/
    │   └── sma-code-processes/
    │       └── sma-code-process-<timestamp>/  # Historical runs
```

> **Note:** In v2, DVP lives under `sma-output/`; in v1, under legacy `Conversion-*/`; in **v3**, under the latest `sma-code-process-*` inside `Conversion_SnowparkAPI` or `Conversion_SnowparkConnect`. Detection checks **`sma-output/` before v3** so v2 and v3 can coexist in the ecosystem. Examples may use different folder names for input/output (`input/` vs `in/`, `output/` vs `out/`).

### v3 Format (no bundled example)

```
<NN> - <name>/
└── output/                                  # SMA output root (any folder name)
    ├── <project>.snowct                     # Optional; no paths
    ├── Assessment/                          # Ignored for DVP path resolution
    ├── Conversion_SnowparkAPI/
    │   └── sma-code-process-<timestamp>/    # <output> when validating API (latest)
    │       ├── Output/
    │       ├── Reports/
    │       └── Logs/
    └── Conversion_SnowparkConnect/
        └── sma-code-process-<timestamp>/    # <output> when validating Connect (latest)
            ├── Output/
            ├── Reports/
            └── Logs/
```

## How to Use

### With dvp-orchestrator

The recommended way is to point the orchestrator at the example's input folder. DVP auto-detects the SMA format and resolves `<output>` automatically:

1. Provide the example's input folder path to the orchestrator
2. The orchestrator detects the SMA format (v1, v2, or v3) and resolves `<output>`
3. DVP workspace is created at `<output>/dvp/` (v1: legacy `Conversion-*/`; v2: `sma-output/`; v3: latest `sma-code-process-*` under the chosen **`Conversion_SnowparkAPI`** or **`Conversion_SnowparkConnect`**)

```
# Example 01 (v1 format - .snowma):
<input>  = examples/01 - workload-simple-etl/input
<output> = examples/01 - workload-simple-etl/output/Conversion-2-12-2026T13 8  (auto-resolved)

# Example 02 (v1 format - .snowma):
<input>  = examples/02 - ECommerceDataPipeline/in
<output> = examples/02 - ECommerceDataPipeline/out/Conversion-2-12-2026T12 23  (auto-resolved)

# Example 03 (v2 format - sma-output/):
<input>  = examples/03 - Notebooks/input
<output> = examples/03 - Notebooks/output/sma-output  (auto-detected)
```

### With individual skills

Run the orchestrator first to set up the DVP workspace, then invoke specific skills as needed.

## Adding New Examples

When creating a new example:

1. Create a folder with a numbered prefix: `<NN> - <descriptive-name>/`
2. Place the original PySpark source code in the input folder
3. Run the SMA tool against the input to generate the real output
4. Add a `README.md` inside the example folder describing the scenario
5. Update the table above

### Tips for Good Examples

- Include at least one file with `if __name__ == "__main__":` as a clear entry point
- Include at least one utility/helper module (not an entry point) to test classification
- Add various I/O patterns (read table, write table, read file) for the io-identifier
- Keep examples small but realistic (1-6 source files is ideal)
- Include a mix of entry point types (main block, main function, session init)
- When possible, include expected output data for the test-validation-generator
- Always use the real SMA tool to generate the output -- do not simulate SMA output manually
