---
name: migrate-spark-scala-to-snowpark-connect
description: |
  Migrate Spark Scala workloads to Snowflake SCOS (Snowpark Connect for Spark).
  Use when: converting Scala Spark code to run on Snowflake, analyzing Scala Spark compatibility,
  updating imports to Spark Connect equivalents, or migrating from standalone Spark Scala.
  Generates SMA-compatible reports (Issues.csv, InputFilesInventory.csv, ArtifactDependencyInventory.csv)
  for the dvp-sma-dashboard-generator using official SMA EWI codes (SPRKCNTSCL*).
  Triggers: migrate scala spark, convert scala, scos scala migration,
  spark connect scala, scala compatibility, snowpark connect scala.
parent_skill: snowpark-connect
allowed-tools: Read, Write, Bash
---

# Migrate Spark Scala to SCOS — Coordinator

Orchestrate a multi-phase migration of Spark Scala workloads to Snowflake SCOS (Snowpark Connect for Spark). This coordinator delegates work to specialist sub-agents and validates each phase with critic agents before advancing.

## When to Load

[snowpark-connect] Intent Detection: After user indicates migration intent for Scala code (convert, migrate, update imports, rewrite for SCOS).

## Arguments

- `$ARGUMENTS` — Path to the Spark Scala file or directory to migrate

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

4. **Build the file manifest** — enumerate `.scala` source files AND build files: <!-- SNOW-3383535: Sort by relative path for deterministic chunk boundaries -->
```bash
(cd <CONVERSION>/Output && find . -name "*.scala" -type f | sort)
(cd <CONVERSION>/Output && find . \( -name "*.sbt" -o -name "pom.xml" -o -name "build.gradle" -o -name "build.gradle.kts" \) -type f | sort)
```

5. <!-- SNOW-3383536: Dispatch mode threshold -->
   **Determine dispatch mode**: Check manifest length against `DISPATCH_THRESHOLD` (default: 100).
   - If `len(manifest) <= 100`: set `coordinator_mode = false` — process all phases in the current agent context without sub-agent dispatch. This avoids coordinator overhead for small workloads.
   - If `len(manifest) > 100`: set `coordinator_mode = true` — use chunked sub-agent dispatch for Phase 2 (code fixes). Each chunk is sized by context budget estimation.

6. **Initialize git**:
```bash
cd <CONVERSION> && git init && git add . && git commit -m "Initial commit: source copied for SCOS migration" && git branch -M main
```

7. **Write `migration_state.json`** to `<CONVERSION>/`:
```json
{
  "phase": 0,
  "manifest": ["<list of .scala files, sorted by relative path>"],
  "file_order": ["<relative paths sorted alphabetically — mirrors manifest order for auditability>"],
  "build_files": ["<list of build files>"],
  "conversion_root": "<CONVERSION>",
  "migrated_dir": "<CONVERSION>/Output/",
  "skill_directory": "<SKILL_DIRECTORY>",
  "coordinator_mode": true,
  "dispatch_threshold": 100,
  "context_budget_tokens": 160000,
  "phases_completed": {},
  "metadata": {"email": "...", "company": "...", "project": "..."}
}
```

> **Note**: As phases complete, each entry under `phases_completed` will contain `processed_files` (done), `pending_files` (remaining), and `checkpoint_timestamp`. Non-empty `pending_files` after a specialist exits means it was interrupted — the coordinator must spawn a resume agent.

### Phase 1: Analysis

**Spawn specialist**: `task()` with the content of `agents/analyzer.md` as prompt context. Pass `migration_state.json` path. The analyzer runs `analyze_scala.py` and produces `analysis.json`.

**Spawn critic**: `task()` with `agents/analyzer-critic.md`. The critic validates `analysis.json` completeness and checks for known analyzer blind spots (UDFs, checkpoint, Catalyst imports, Hadoop/Hive patterns).

**Gate**: Critic must report `PASS`. If `FAIL`, re-run analyzer with critic feedback (max 2 retries). Update `migration_state.json` phase to 1.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 1: analysis complete"`

### Phase 2: Apply Fixes

<!-- SNOW-3385158: Orchestration moved to external script for deterministic dispatch -->
<!-- SNOW-3383536: Budget reduced to 80k tokens/chunk (aggressive mode) for guaranteed completion -->
**Pre-dispatch: Run External Orchestrator** — Compute budget-aware chunks and write the dispatch plan to `migration_state.json`:

```bash
python3 <SKILL_DIRECTORY>/scripts/orchestrate_phases.py \
  --state <CONVERSION>/migration_state.json \
  --phase 2 \
  --budget 80000 \
  --language scala
```

The script prints a structured dispatch plan: for each chunk it outputs `CHUNK_MODE`, `CHUNK_ID`, and `CHUNK_FILES`. It also runs `fallback_transform.py` as a mandatory hard gate and prints a final coverage report. Read the output and act on it.

Token formula: `file_tokens = file_chars // 4 + 2000` (characters ÷ 4 plus 2000 overhead per file). A single file that exceeds the budget on its own gets a dedicated chunk so it is never silently skipped.

**Spawn specialist**: `task()` with `agents/fixer.md` — dispatch **one fixer sub-agent per chunk** from the orchestrator output. Pass `CHUNK_MODE=chunked`, `CHUNK_ID=<n>`, and `CHUNK_FILES=<files>` as context parameters. The fixer reads `analysis.json`, loads `references/fix-rules.md` for the detailed Scala-specific rules, and applies fixes to its assigned file list. It updates `migration_state.json` with per-file progress.

**Checkpoint detection**: After each specialist exits, read `migration_state.json`. If `phases_completed["2_fixes"]["pending_files"]` is non-empty, re-run `orchestrate_phases.py` — it recomputes chunks from the remaining files. Spawn a fresh `agents/fixer.md` specialist for each remaining chunk. Repeat until `pending_files` is empty.

**Spawn critic**: `task()` with `agents/fixer-critic.md`. The critic runs `scalac` or `sbt compile` for syntax validation, verifies all high-risk issues were addressed, and confirms no-ops were not over-annotated.

**Gate**: Critic must report `PASS` with zero compilation errors. If `FAIL`, re-run fixer on failing files with critic feedback. Update `migration_state.json` phase to 2.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 2: fixes applied"`

### Phase 2a: Coverage Verification and Deterministic Fallback

<!-- SNOW-3375304: Ensure 100% file coverage after Phase 2 -->
<!-- SNOW-3383533: Scala deterministic fallback — header + import annotations + session init + EWI -->
<!-- SNOW-3385158: fallback_transform.py is now a mandatory hard gate inside orchestrate_phases.py -->
`orchestrate_phases.py` already ran `fallback_transform.py --language scala` as a mandatory hard gate and printed a coverage report. Read that output:

- If it reports `Coverage: 100%` — proceed to the compilation gate below.
- If it lists `MISSING` files — escalate to the user; files are absent even after fallback.

The fallback script applies deterministic transformations to any files missed by sub-agents:
- Copies the original source to `Output/` if not already present
- Injects a SCOS migration header block comment (Scala `/* ... */` style)
- Annotates `org.apache.spark`, `com.databricks`, and `io.delta` imports with `// SCOS: [SPRKCNTSCL0099]` comments
- Replaces `SparkSession.builder().getOrCreate()` with `.builder().remote()` in entry-point files
- Appends a `SPRKCNTSCL0099` EWI entry to `analysis.json` for each fallback file

**Gate**: All manifest files must exist in `<MIGRATED>`. `migration_state.json` field `orchestrator_coverage_verified` is set to `true` by the orchestrator when coverage is 100%.

### Phase 2b: Compilation Verification Gate

After Phase 2a coverage check, verify that every `.scala` file in `<MIGRATED>` has balanced braces (basic syntax check):

```bash
python3 -c "
import os, sys
fail = 0
migrated = '<MIGRATED>'
for root, dirs, files in os.walk(migrated):
    for f in files:
        if f.endswith('.scala'):
            path = os.path.join(root, f)
            content = open(path).read()
            if content.count('{') != content.count('}'):
                print(f'BRACE_MISMATCH: {path}')
                fail += 1
print(f'Compilation gate: {fail} files with issues')
sys.exit(1 if fail else 0)
"
```

**Gate**: All `.scala` files must pass the syntax check. Update `migration_state.json` with `"compilation_check_failures": N`.

### Phase 3: Imports, Session, Build, and Headers

**Spawn specialist**: `task()` with `agents/import-updater.md`. Updates imports, replaces session initialization with Spark Connect `.remote()`, transforms build files (sbt/Maven), and adds migration header block comments to every `.scala` file.

**Spawn critic**: `task()` with `agents/import-critic.md`. Verifies every file has a migration header, no `enableHiveSupport()` in non-comment code, entry point uses `.remote()`, no unsupported imports remain, build files are actively transformed.

**Gate**: Critic must report `PASS`. Update `migration_state.json` phase to 3.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 3: imports, session, build, and headers updated"`

### Phase 4: Generate Reports

**Spawn specialist**: `task()` with `agents/reporter.md`. Runs `generate_scos_reports.py --language scala` to produce `Reports/Issues.csv`, `Reports/InputFilesInventory.csv`, `Reports/ArtifactDependencyInventory.csv`.

**Spawn critic**: `task()` with `agents/reporter-critic.md`. Validates CSV files exist and have correct structure.

**Gate**: Critic must report `PASS`. Update `migration_state.json` phase to 4.

**Git checkpoint**: `cd <CONVERSION> && git add -A && git commit -m "Phase 4: reports generated"`

### Phase 5: Offer Validation (Optional)

Ask the user:
```
Migration complete! Would you like to validate the migrated workload
by running it end-to-end with synthetic data?
```

If yes, load `validate-spark-scala-to-snowpark-connect/SKILL.md` with `<MIGRATED>` as `$ARGUMENTS`.

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
- All `.scala` files pass compilation check (scalac/sbt compile)
- Every `.scala` file has a migration header block comment
- Build files actively transformed (Scala 2.12+, Spark 3.5+, spark-connect-client-jvm added)
- File count matches between original and migrated directories

## Output

```
<output_root>/
  Conversion-SCOS-<timestamp>/    ← <CONVERSION>
    Output/                       ← <MIGRATED> — converted files
    Reports/                      ← SMA-compatible dashboard reports
    Logs/                         ← Migration log
    migration_state.json          ← Phase gate tracking
    analysis.json                 ← Compatibility analysis
```
