# Fixer Agent — Phase 2 Specialist

Apply code fixes for SCOS compatibility issues identified in `analysis.json`.

## Inputs

Read `migration_state.json` to get:
- `manifest` — list of `.py` files
- `migrated_dir` — directory with copied source files
- `conversion_root` — for `analysis.json` and gate file

Read `analysis.json` from the conversion root.

## Chunk Mode (Coordinator-Dispatched)

<!-- SNOW-3383531: Chunk mode for large workloads -->
When the coordinator uses chunked dispatch (`coordinator_mode = true`), your prompt context includes:
- `CHUNK_MODE=true`
- `CHUNK_ID=<i>` — zero-based chunk index
- `CHUNK_FILES=<list>` — the specific files to process in this chunk
- `MIGRATION_STATE_PATH=<path>` — path to `migration_state.json`

**If `CHUNK_MODE=true`**: Process **only** the files in `CHUNK_FILES`, not the full manifest.

After processing all files in your chunk, update `migration_state.json` to record progress:
```python
import json
state = json.load(open(MIGRATION_STATE_PATH))
for f in CHUNK_FILES:
    if f in state.get('pending_files', []):
        state['pending_files'].remove(f)
    if f not in state.get('processed_files', []):
        state['processed_files'].append(f)
for chunk in state.get('chunks', []):
    if chunk['id'] == CHUNK_ID:
        chunk['status'] = 'done'
json.dump(state, open(MIGRATION_STATE_PATH, 'w'), indent=2)
```

Report: `"Chunk <CHUNK_ID> complete: X files processed"`

## Rules

Load `references/fix-rules.md` for the complete fix rule set. Key rules summary:

| Risk | Action |
|------|--------|
| `final_risk >= 0.7` | **Must fix** — apply fix or rewrite. If impossible, add `# SCOS: TODO` |
| `0.3 <= final_risk < 0.7` | **Should fix** — apply fix if suggested, else `# SCOS: TODO` |
| `final_risk < 0.3` | **Review** — fix if possible, else `# SCOS: <explanation>` |

**Critical exceptions (do NOT annotate):**
- No-op operations (`hint()`, `repartition()`, `coalesce()`) — leave as-is, no comment
- No-op configs (`spark.sql.shuffle.partitions`, `spark.executor.memory`, etc.) — leave as-is, no comment

**Comment prefixes:**
- `# SCOS: <explanation>` — fix applied or reviewed
- `# SCOS: TODO - <explanation>` — requires manual review
- `# SCOS: Performance tip - <explanation>` — optimization recommendation

## Important: PySpark Version Context

Snowpark Connect is based on Spark Connect protocol, NOT PySpark 4. Do NOT use PySpark 4 APIs or behaviors as a reference for fixes. Customers are typically on PySpark 3.x. All fixes must target Spark Connect compatibility as documented in the fix rules, not PySpark 4 features.

## Workflow

Process files **one at a time** from the manifest:

1. Read the file
2. Find all issues for this file in `analysis.json`
3. For each issue, consult `references/fix-rules.md` for the appropriate action:
   - **RDD operations**: Read `references/python/rdd-conversion.md` for DataFrame equivalents
   - **UDF serialization**: Read `references/python/udf-dependencies.md` for Tier 1/2/3 fixes
   - **Wildcard file reads**: Replace with explicit file lists or add TODO
   - **`checkpoint()`**: Replace with `cache()`
   - **Map column subscript**: Replace `map_col[col("key")]` with `element_at(map_col, col("key"))`
   - **Snowflake Connector pushdown**: Add comment recommending `SnowflakeSession.sql()` — keep original code
   - **Unsupported formats**: Flag with TODO (Avro/ORC/Delta → Parquet)
   - **SparkContext access**: Replace with `spark.conf.get()` or static fallbacks (Rule 14)
   - **Hadoop filesystem**: Flag with TODO for Snowflake stage operations (Rule 15)
   - **USE DATABASE/SCHEMA**: Replace with fully-qualified table references (Rule 16)
   - **JVM-only libraries (Deequ)**: Flag with TODO for native DataFrame alternatives (Rule 17)
   - **ML pipeline patterns**: Flag with TODO for Snowpark ML or scikit-learn (Rule 18)
   - **UDTF/UDAF patterns**: Convert to Snowpark handler classes (Rule 19)
   - **Delta Lake operations**: Replace with Snowflake table operations (Rule 20)
   - **Lazy view re-evaluation**: Insert `.cache()` before `createOrReplaceTempView()` (Rule 21)
   - **Memory anti-patterns**: Follow analyzer's `how_to_fix` guidance (Rule 22)
4. Apply fixes using the Edit tool
5. **Compilation guard**: After applying fixes to each file, verify syntax:
   ```bash
   python3 -m py_compile <file>
   ```
   If compilation fails:
   - Revert the file: `cd <CONVERSION> && git checkout -- <file>`
   - Add a comment at the top of the issue location: `# SCOS: SKIPPED - fix reverted (would break syntax)`
   - Log the file in migration_state.json under `"2_fixes_skipped": ["file.py"]`
   - Continue to the next file
6. Update `migration_state.json` with per-file progress:
   ```json
   "2_fixes": {"files_done": ["file1.py"], "files_remaining": ["file2.py", "file3.py"]}
   ```

## Notebook File Handling

When processing `.ipynb` files:
1. Read the notebook: `nb = json.load(open(path))`
2. For each code cell matching an issue in analysis.json:
   - Apply the same fix rules as for .py files
   - Modify the cell's `source` array in-place
3. Write the modified notebook: `json.dump(nb, open(path, 'w'), indent=1)`
4. Validate JSON structure: `python3 -c "import json; json.load(open('<path>'))"`
5. Do NOT modify markdown or raw cells

## Completeness Check

After processing all files:
- Every issue in `analysis.json` with `final_risk >= 0.7` has a fix or TODO
- Every issue with `final_risk >= 0.3` has a fix, comment, or TODO
- File count matches manifest

Report: "Fixes applied: X files processed, Y issues fixed, Z TODOs remaining"

## Output

- Modified files in `<MIGRATED>/`
- Updated `migration_state.json` with phase 2 status
