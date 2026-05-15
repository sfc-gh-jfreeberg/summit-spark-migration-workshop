---
name: migrate-pyspark-to-snowpark-connect
description: |
  Migrate PySpark and Databricks workloads to Snowflake SCOS (Snowpark Connect for Spark).
  Use when: converting Spark code to run on Snowflake, analyzing PySpark compatibility,
  updating imports to Spark Connect equivalents, or migrating from Databricks.
  Generates SCOS-compatible reports (Issues.csv, InputFilesInventory.csv, ArtifactDependencyInventory.csv)
  for the dvp-scos-dashboard-generator using official SCOS EWI codes (SPRKCNTPY*).
  Triggers: migrate pyspark, convert spark, scos migration,
  spark connect, pyspark compatibility, snowpark connect.
parent_skill: snowpark-connect
allowed-tools: Read, Write, Bash
---

# Migrate PySpark to SCOS — Coordinator

Orchestrate a multi-phase migration of PySpark workloads to Snowflake SCOS (Snowpark Connect for Spark). This coordinator delegates work to specialist sub-agents and validates each phase with critic agents before advancing.

## When to Load

[snowpark-connect] Intent Detection: After user indicates migration intent (convert, migrate, update imports, rewrite for SCOS).

## Arguments

- `$ARGUMENTS` — Path to the PySpark file or directory to migrate

### Optional Metadata (from orchestrator)

| Parameter | Variable | Description |
|-----------|----------|-------------|
| Output path | `$OUTPUT` | Target directory for migrated files and Reports/ |
| Customer Email | `$EMAIL` | Project metadata for reports |
| Customer Company | `$COMPANY` | Project metadata for reports |
| Project Name | `$PROJECT` | Project name for reports |

If not provided, use `${ARGUMENTS}_scos` as output and prompt for metadata in Phase 4.

## Prerequisites

### Skill Directory

`<SKILL_DIRECTORY>` is the **parent** `snowpark-connect/` directory containing `pyproject.toml` and `scripts/`. All tool invocations use `uv run --project <SKILL_DIRECTORY>`.

### uv Package Manager

```bash
uv --version || curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Workflow

You are a coordinator. You **NEVER** apply code fixes, update imports, or generate reports yourself. You delegate each phase to a specialist sub-agent via the `task()` tool, then validate with a critic sub-agent. State is tracked in `migration_state.json`.

### Phase 0: Collect Info and Create Conversion Folder

1. **Collect project info** from the user if not already provided: input path, output path, email, company, project name.

2. **Create timestamped conversion folder**:
```bash
OUTPUT_ROOT="<$OUTPUT or ${ARGUMENTS}_scos>"
TIMESTAMP=$(date +"%m-%d-%YT%H %M %S")
CONVERSION="${OUTPUT_ROOT}/Conversion-SCOS-${TIMESTAMP}"
mkdir -p "${CONVERSION}/Output" "${CONVERSION}/Reports" "${CONVERSION}/Logs"
```

3. **Copy source files** into `<CONVERSION>/Output/` (the `<MIGRATED>` directory):
```bash
cp -r $ARGUMENTS/* <CONVERSION>/Output/   # directory
cp $ARGUMENTS <CONVERSION>/Output/        # single file
```

4. **Build the file manifest**: <!-- SNOW-3383535: Sort by relative path for deterministic chunk boundaries -->
```bash
(cd <CONVERSION>/Output && find . \( -name "*.py" -o -name "*.ipynb" \) -type f | sort)
```

4a. **Unpack .dbc archives** (if present):
```bash
for dbc in $(find <CONVERSION>/Output -name "*.dbc" -type f); do
  UNPACK_DIR="${dbc%.dbc}_unpacked"
  python3 -c "
import zipfile, os
with zipfile.ZipFile('$dbc', 'r') as z:
    z.extractall('$UNPACK_DIR')
"
  # Add unpacked notebooks to manifest
  find "$UNPACK_DIR" -name "*.ipynb" -type f >> /tmp/additional_notebooks.txt
done
```
Append any unpacked .ipynb files to the manifest.

4b. **Notebook metadata**: For `.ipynb` files, extract code cell count for planning:
```bash
python3 -c "
import json, sys
nb = json.load(open(sys.argv[1]))
code_cells = [c for c in nb.get('cells', []) if c.get('cell_type') == 'code']
print(f'{sys.argv[1]}: {len(code_cells)} code cells')
" <notebook.ipynb>
```

5. **Determine dispatch mode**: Check manifest length against `DISPATCH_THRESHOLD` (default: 100).
   - If `len(manifest) <= 100`: set `coordinator_mode = false` — process inline without sub-agent overhead.
   - If `len(manifest) > 100`: set `coordinator_mode = true` — use chunked sub-agent dispatch for Phase 2.

6. **Initialize git**:
```bash
cd <CONVERSION> && git init && git add . && git commit -m "Initial commit: source copied for SCOS migration" && git branch -M main
```

7. **Write `migration_state.json`** to `<CONVERSION>/`:
```json
{
  "phase": 0,
  "manifest": ["<list of .py files, sorted by relative path>"],
  "file_order": ["<relative paths sorted alphabetically — mirrors manifest order for auditability>"],
  "notebook_files": ["<list of .ipynb files>"],
  "dbc_archives": ["<list of .dbc files>"],
  "conversion_root": "<CONVERSION>",
  "migrated_dir": "<CONVERSION>/Output/",
  "skill_directory": "<SKILL_DIRECTORY>",
  "coordinator_mode": true,
  "dispatch_threshold": 100,
  "context_budget_tokens": 160000,
  "chunk_size": 20,
  "chunks": [],
  "processed_files": [],
  "pending_files": [],
  "phases_completed": {},
  "metadata": {"email": "...", "company": "...", "project": "..."}
}
```

### Phase 1: Analysis

**Spawn specialist**: `task()` with the content of `agents/analyzer.md` as prompt context. Pass `migration_state.json` path. The analyzer runs `analyze_pyspark.py` and produces `analysis.json`.

**Spawn critic**: `task()` with `agents/analyzer-critic.md`. The critic validates `analysis.json` completeness and checks for known analyzer blind spots (UDFs, checkpoint, map subscript).

**Gate**: Critic must report `PASS`. If `FAIL`, re-run analyzer with critic feedback (max 2 retries). Update `migration_state.json` phase to 1.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 1: analysis complete"`

### Phase 2: Apply Fixes

<!-- SNOW-3385158: Orchestration moved to external script for deterministic dispatch -->
<!-- SNOW-3383531: Budget reduced to 80k tokens/chunk (aggressive mode) for guaranteed completion -->
**Pre-dispatch: Run External Orchestrator** — Compute budget-aware chunks and write the dispatch plan to `migration_state.json`:

```bash
python3 <SKILL_DIRECTORY>/scripts/orchestrate_phases.py \
  --state <CONVERSION>/migration_state.json \
  --phase 2 \
  --budget 80000 \
  --language python
```

The script prints a structured dispatch plan: for each chunk it outputs `CHUNK_MODE`, `CHUNK_ID`, and `CHUNK_FILES`. It also initialises `pending_files` and `processed_files` in `migration_state.json`, runs `fallback_transform.py` as a mandatory hard gate, and prints a final coverage report. Read the output and act on it.

Token formula: `file_tokens = file_chars // 4 + 2000` (characters ÷ 4 plus 2000 overhead per file). A single file that exceeds the budget on its own gets a dedicated chunk so it is never silently skipped.

**Spawn specialist**: `task()` with `agents/fixer.md` — dispatch **one fixer sub-agent per chunk** from the orchestrator output. Pass `CHUNK_MODE=chunked`, `CHUNK_ID=<n>`, and `CHUNK_FILES=<files>` as context parameters. The fixer processes **only** the files in `CHUNK_FILES`. On completion it updates `migration_state.json`: appends each file to `processed_files[]`, removes from `pending_files[]`, and sets `chunks[i].status = "done"`.

After each chunk agent completes, git checkpoint:
```bash
cd <CONVERSION> && git add -A && git commit -m "Phase 2: chunk <i>/<total> complete"
```

**Checkpoint detection**: After each specialist exits, read `migration_state.json`. If `pending_files` is non-empty, re-run `orchestrate_phases.py` — it recomputes chunks from the remaining files. Spawn a fresh `agents/fixer.md` specialist for each remaining chunk. Repeat until `pending_files` is empty.

**Spawn critic**: After all chunks complete, spawn `task()` with `agents/fixer-critic.md` on the full `Output/` directory.

**Gate**: Critic must report `PASS` with zero syntax errors. If `FAIL`, re-dispatch fixer on the failing files only. Update `migration_state.json` phase to 2.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 2: all chunks complete, fixes applied"`

### Phase 2a: Coverage Verification and Deterministic Fallback

<!-- SNOW-3375304: Ensure 100% file coverage after Phase 2 -->
<!-- SNOW-3383532: Enriched fallback — header + import annotations + session init replacement + EWI -->
<!-- SNOW-3385158: fallback_transform.py is now a mandatory hard gate inside orchestrate_phases.py -->
`orchestrate_phases.py` already ran `fallback_transform.py` as a mandatory hard gate and printed a coverage report. Read that output:

- If it reports `Coverage: 100%` — proceed to the compilation gate below.
- If it lists `MISSING` files — escalate to the user; files are absent even after fallback.

The fallback script reads `pending_files` from `migration_state.json` and applies deterministic transformations to any remaining files:
- Copies the original source to `Output/` if not already present
- Injects a SCOS migration header docstring
- Annotates `pyspark`, `databricks`, and `delta` imports with `# SCOS: [SPRKCNTPY0099]` comments
- Replaces `SparkSession.builder.getOrCreate()` (and variants) with `snowpark_connect.init_spark_session()` in entry-point files
- Appends a `SPRKCNTPY0099` EWI entry to `analysis.json` for each fallback file

**Gate**: All manifest files must exist in `<MIGRATED>`. `migration_state.json` field `orchestrator_coverage_verified` is set to `true` by the orchestrator when coverage is 100%.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 2a: deterministic fallback applied"`

### Phase 2b: Compilation Verification Gate

<!-- SNOW-3379886: Hard gate ensuring 100% compilation after code fixes -->
After Phase 2a coverage check, verify that every `.py` file in `<MIGRATED>` compiles:

```bash
FAIL_COUNT=0
for f in $(find <MIGRATED> -name "*.py" -type f); do
  if ! python3 -m py_compile "$f" 2>/dev/null; then
    echo "COMPILE_FAIL: $f"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    # Revert to pre-Phase-2 version
    git show HEAD~2:"$(git ls-files --full-name "$f")" > "$f" 2>/dev/null || true
  fi
done
```

If any files were reverted, re-run the fixer-critic on just those files. Log: `Compilation gate: N files reverted to pre-fix state`.

**Gate**: All `.py` files must pass `python3 -m py_compile`. Update `migration_state.json` with `"compilation_reverted_count": N`.

### Phase 2a: Coverage Verification and Deterministic Fallback

<!-- SNOW-3383532: Enriched fallback — header + import annotations + session init replacement + EWI -->
After fixes are applied, verify that every file in the manifest has a corresponding output in `<MIGRATED>`. Sub-agents may hit context limits and silently stop processing mid-batch.

1. **Run the deterministic fallback script**:
```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/fallback_transform.py \
  --state <CONVERSION>/migration_state.json
```

The script finds any files in `manifest` that the fixer agent did not record in `2_fixes.files_done` and applies a full deterministic transformation to each:
- Copies the original source to `Output/` if not already present
- Injects a SCOS migration header docstring
- Annotates `pyspark`, `databricks`, and `delta` imports with `# SCOS: [SPRKCNTPY0099]` comments
- Replaces `SparkSession.builder.getOrCreate()` (and variants) with `snowpark_connect.init_spark_session()` in entry-point files
- Appends a `SPRKCNTPY0099` EWI entry to `analysis.json` for each fallback file (captured by downstream report generation)

Log output ends with: `Fallback complete: N file(s) transformed`

2. **Verification gate**: After the script returns, re-check coverage:
```bash
python3 -c "
import json, os
state = json.load(open('<CONVERSION>/migration_state.json'))
migrated = state.get('migrated_dir', '<MIGRATED>')
missing = [f for f in state.get('manifest', []) if not os.path.exists(os.path.join(migrated, f))]
print('MISSING:', missing) if missing else print('Coverage: 100%')
"
```

If any files are still missing after fallback, escalate to the user.

**Gate**: All manifest files must exist in `<MIGRATED>`. The script updates `migration_state.json` with `"fallback_count": N` automatically.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 2a: deterministic fallback applied"`

### Phase 3: Imports and Headers

**Spawn specialist**: `task()` with `agents/import-updater.md`. Updates imports, replaces session initialization with `snowpark_connect.init_spark_session()`, and adds migration header docstrings to every file.

**Spawn critic**: `task()` with `agents/import-critic.md`. Verifies every file has a migration header, no `SparkSession.builder` in non-comment code, entry point has `snowpark_connect`, no unsupported imports remain.

**Gate**: Critic must report `PASS`. Update `migration_state.json` phase to 3.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 3: imports and headers updated"`

### Phase 4: Generate Reports

**Spawn specialist**: `task()` with `agents/reporter.md`. Runs `generate_scos_reports.py` to produce `Reports/Issues.csv`, `Reports/InputFilesInventory.csv`, `Reports/ArtifactDependencyInventory.csv`.

**Spawn critic**: `task()` with `agents/reporter-critic.md`. Validates CSV files exist and have correct structure.

**Gate**: Critic must report `PASS`. Update `migration_state.json` phase to 4.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 4: reports generated"`

### Phase 4a: Inject EWI Markers

After reports are generated, inject structured `#EWI:` markers into the migrated source files for downstream EWI fixer compatibility.

```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/scos_to_ewi_bridge.py \
  --workload-dir <CONVERSION> \
  --language python
```

The bridge reads `Reports/Issues.csv` and injects markers above the flagged lines in `Output/` files. It is idempotent.

### Phase 5: Offer Validation (Optional)

Ask the user:
```
Migration complete! Would you like to validate the migrated workload
by running it end-to-end with synthetic data?
```

If yes, load `validate-pyspark-to-snowpark-connect/SKILL.md` with `<MIGRATED>` as `$ARGUMENTS`.

### Resumption

If context is lost mid-migration, read `migration_state.json` to determine the last completed phase and resume from the next one. The gate file contains the manifest, paths, and per-file progress needed to continue.

## Stopping Points

- Phase 0: After collecting project info — confirm settings before starting
- Phase 2: If critic fails after 2 retries — escalate to user with specific errors
- Phase 5: After migration completes — ask user about validation

## Success Criteria

- `migration_state.json` shows all phases 1-4 completed with critic approval
- `Reports/Issues.csv` exists with data rows
- `Reports/InputFilesInventory.csv` row count matches manifest
- All `.py` files pass `py_compile` syntax check
- Every `.py` file has a migration header docstring
- File count matches between original and migrated directories

## Output

```
<output_root>/
  Conversion-SCOS-<timestamp>/    ← <CONVERSION>
    Output/                       ← <MIGRATED> — converted files
    Reports/                      ← SCOS-compatible dashboard reports
    Logs/                         ← Migration log
    migration_state.json          ← Phase gate tracking
    analysis.json                 ← Compatibility analysis
```
