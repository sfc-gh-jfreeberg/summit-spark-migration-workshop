# Analyzer Agent — Phase 1 Specialist

Run the SCOS compatibility analyzer on the Scala workload and produce `analysis.json`.

## Inputs

Read `migration_state.json` from the conversion root to get:
- `manifest` — list of `.scala` files to analyze
- `migrated_dir` — directory containing the copied source files
- `skill_directory` — path to `snowpark-connect/` for `uv run --project`

## Step 0: Setup RAG Resources (One-Time)

The analyzer defaults to the **remote WebAPI RAG backend** (`--rag-backend remote`).
It authenticates against the WebAPI with the current Snowflake session and, if the
WebAPI is unreachable, automatically falls back to Cortex Search.

You only need to set up Cortex Search when you want to **force** the Cortex path
(e.g., WebAPI is known to be unavailable in your environment, or you want fully
local/in-account search with no outbound HTTP).

Check if Cortex Search RAG is already initialized:
```bash
uv run --project <SKILL_DIRECTORY> \
  python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
try:
    rows = session.sql(\"SHOW CORTEX SEARCH SERVICES LIKE 'SCOS_COMPAT_ISSUES_SERVICE'\").collect()
    if rows:
        print(f'EXISTS {rows[0][\"database_name\"]}.{rows[0][\"schema_name\"]}')
    else:
        print('NOT_FOUND')
except Exception as e:
    print(f'ERROR {e}')
"
```

- **Default path (recommended)**: do nothing — run the analyzer without `--rag-backend`
  and let it use the WebAPI (with auto-fallback to Cortex if the WebAPI is down).
- **Force Cortex Search**: use `--rag-backend cortex`. Requires Cortex Search to
  exist (`EXISTS` above). If `NOT_FOUND` and role is ACCOUNTADMIN, ask the user
  for a warehouse and run `scos_rag.py` to initialize it first.

## Step 1: Run the Analyzer

```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/analyze_scala.py \
  --path <migrated_dir> --output-format json > analysis.json
```

To force a specific backend, add `--rag-backend remote` or `--rag-backend cortex`.

Wait for completion. Read `analysis.json` to verify it's valid JSON.

## Step 2: Supplement for Known Blind Spots

The analyzer may miss certain Scala-specific patterns. Scan ALL files in the manifest for:

1. **UDF patterns not in analysis**: `udf(`, `spark.udf.register(`, `UserDefinedFunction`, `UserDefinedAggregateFunction`
2. **`checkpoint()` / `localCheckpoint()`** calls
3. **Map column subscript**: `mapCol(col("key"))` pattern (apply-style indexing with Column key)
4. **Catalyst imports**: `org.apache.spark.sql.catalyst.*` — internal APIs not in Spark Connect client
5. **Hadoop/HDFS imports**: `org.apache.hadoop.*` — not available in SCOS
6. **HWC imports**: `com.hortonworks.spark.sql.hive.*` — HiveWarehouseSession not available
7. **Lineage imports**: `za.co.absa.spline.*` — Spline not available

For each found pattern NOT already in `analysis.json`, append a supplementary entry:
```json
{
  "file": "<path>",
  "lines": "<line_range>",
  "code": "<snippet>",
  "final_risk": 0.9,
  "root_cause": "<description>",
  "explanation": "<why this is a problem in SCOS>",
  "fix": "<suggested fix>",
  "confidence": "HIGH",
  "source": "supplementary_scan"
}
```

## Step 3: Update Gate File

Update `migration_state.json`:
```json
{
  "phase": 1,
  "phases_completed": {
    "1_analysis": {"status": "passed", "issues_found": N, "supplementary_added": M}
  }
}
```

## Output

- `analysis.json` in the conversion root
- Updated `migration_state.json`
- Report: "Analysis complete: N issues found (M supplementary)"
