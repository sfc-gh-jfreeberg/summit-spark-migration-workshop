# Snowpark Migrator - Architecture Diagram

> **Last Updated**: 2026-04-13
> **Note**: Keep this diagram updated when making changes to the skill.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SNOWPARK MIGRATOR - FLOW                               │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│   TRIGGER       │  "run sma", "convert spark", "migrate pyspark", "sma conversion"
│   (Cortex)      │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: LOAD CONFIGURATION                                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  config_manager.py list <skill_directory>/configurations                        │
│  config_manager.py load-global <skill_directory>  → <global_config>            │
│  ┌──────────────┐                                                              │
│  │ Configs      │                                                              │
│  │ exist?       │                                                              │
│  ├── YES ───────┤──► Show full numbered list as text                            │
│  │              │    ask_user_question: "Use existing" / "Create new"           │
│  │              │    Use existing → ask name/number → load + merge defaults     │
│  └── NO ────────┘──► Ask 5 required params (no defaults) in numbered list      │
│                      #1 Project Name (required), #2-#5 optional for now        │
│                      Create config with defaults → persist user values         │
│                      Step 2 shows full summary (18 params) for confirm/edit    │
│  Store as <config> for reference in later steps                                │
│  Store <global_config> for sma_cli_path (shared across projects)               │
│                                                                                 │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 2: REVIEW CONFIGURATION AND COLLECT PROJECT INFORMATION                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────┐                                                              │
│  │ Has saved    │                                                              │
│  │ values?      │                                                              │
│  ├── YES ───────┤──► Show summary of ALL settings (with possible values)     │
│  │              │    ask_user_question: "Use these settings?" / "Edit"          │
│  │              │    If "Use" → set all variables, proceed to Step 3            │
│  │              │    If "Edit" → show numbered list of all 18 parameters        │
│  └── NO ────────┘──► Show numbered list of all 18 parameters                   │
│                      User responds with numbers to set/change                   │
│                      (e.g., "1. /path, 3. user@co.com, 4. Acme")              │
│                      Persist to <config_path> (sort_keys=True)                 │
│                      #8 sma_cli_path → saved to global config.json            │
│                                                                                 │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 3: DETERMINE MIGRATION STATUS                                            │
│  Read <config.migration_status> from configuration:                            │
│                                                                                 │
│  ┌─────────────────────┐                                                       │
│  │ migration_status?    │                                                       │
│  ├── already_migrated ──┤──► Go to STEP 4                                       │
│  └── migrate ───────────┘──► Go to STEP 5                                       │
│                                                                                 │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ├─── already_migrated ──────────────────────────────────────┐
         │                                                              ▼
         │   ┌──────────────────────────────────────────────────────────────────┐
         │   │  STEP 4: VALIDATE EXISTING OUTPUT (already_migrated)             │
         │   ├──────────────────────────────────────────────────────────────────┤
         │   │                                                                  │
         │   │  4.0 Ask for the output path:                                    │
         │   │    Pre-fill from <config.output_folder> if set                   │
         │   │    Store as <output>                                             │
         │   │                                                                  │
         │   │  4.1 Validate Output Structure:                                  │
         │   │    test -d "<output>/Output" && test -d "<output>/Reports"        │
         │   │                                                                  │
         │   │    If invalid → check for Conversion-* folder (SMA v1 format)    │
         │   │      ls -d "<output>"/Conversion-* | head -1                     │
         │   │      If found → <output> = <output>/Conversion-<timestamp>/      │
         │   │      Re-validate → still invalid? Ask user for correct path      │
         │   │                                                                  │
         │   │  4.2 Confirm and Proceed:                                        │
         │   │    Show validated paths → Go to Step 8                           │
         │   └──────────────────────────────────────────┬───────────────────────┘
         │                                              │
         ├─── migrate ──────────────────────────────────┼───┐
         │                                              │   ▼
         │   ┌──────────────────────────────────────────────────────────────────┐
         │   │  STEP 5: CHOOSE CONVERSION TOOL (migrate)                        │
         │   ├──────────────────────────────────────────────────────────────────┤
         │   │                                                                  │
         │   │  5.0 Ask for the output path:                                    │
         │   │    Pre-fill from <config.output_folder> if set                   │
         │   │    Store as <output>                                             │
         │   │                                                                  │
         │   │  5.1 ask_user_question (pre-select from <config.conversion_type>):   │
         │   │  ┌────────────────┐                                              │
         │   │  │ Conversion     │                                              │
         │   │  │ Tool?          │                                              │
         │   │  ├── (c) ────────┤──► Snowpark API (SMA CLI) → Go to STEP 6     │
         │   │  └── (d) ────────┘──► Snowpark Connect (SCOS) → Go to STEP 7    │
         │   │  Persist selection to <config_path>                                │
         │   │                                                                  │
         │   └────────┬─────────────────────────────────────────────────────────┘
         │            │
         │            ├─── (c) Snowpark API ────────────────────────────┐
         │            │                                                 ▼
         │            │   ┌─────────────────────────────────────────────────────────┐
         │            │   │  STEP 6: SNOWPARK API CONVERSION VIA SMA CLI (Option c) │
         │            │   ├─────────────────────────────────────────────────────────┤
         │            │   │                                                         │
         │            │   │  6.1 Load SMA CLI Path from <global_config.sma_cli_path>: │
         │            │   │    If set → validate (test -x) → use                    │
         │            │   │    If not set → ask user → persist to global config.json │
         │            │   │                                                         │
         │            │   │  6.2 CLI-Specific Fields (from config or ask):           │
         │            │   │    enable_jupyter_conversion, sql_flavor,                │
         │            │   │    generate_checkpoints                                  │
         │            │   │    Persist if asked (sort_keys=True)                     │
         │            │   │                                                         │
│            │   │  6.3 Run SMA CLI:                                       │
│            │   │  ┌─────────────────────────────────────────────────┐    │
│            │   │  │  "<sma_cli>" -i <input> -o <output>            │    │
│            │   │  │    -e <email> -c <company> -p <project>        │    │
│            │   │  │    [-x] [-f <dialect>] [-d]                    │    │
│            │   │  │  run_in_background: true → returns shell_id    │    │
│            │   │  └─────────────────────────────────────────────────┘    │
│            │   │                                                         │
│            │   │  6.4 Monitor Progress (poll every 5-10s)                │
│            │   │                                                         │
│            │   │  6.5 Resolve SMA v1 Output Path:                        │
         │            │   │    Check for Conversion-* folder inside <output>        │
         │            │   │    If found → <output> = Conversion-<timestamp>/        │
         │            │   │    If not → keep <output> unchanged                     │
         │            │   │                                                         │
         │            │   │  After success → proceed to Step 8                      │
         │            │   └─────────────────────────────────────┬───────────────────┘
         │            │                                         │
         │            ├─── (d) Snowpark Connect ────────────────┼───┐
         │            │                                         │   ▼
         │            │   ┌─────────────────────────────────────────────────────────┐
         │            │   │  STEP 7: SNOWPARK CONNECT CONVERSION (Option d)         │
         │            │   ├─────────────────────────────────────────────────────────┤
         │            │   │                                                         │
│            │   │  1. Search for snowpark-connect skill          │
         │            │   │     ┌──────────┐                                        │
         │            │   │     │ FOUND?   │                                        │
         │            │   │     ├── YES ───┤──► Invoke skill with:                  │
         │            │   │     │          │    <input>→$ARGUMENTS, <output>→$OUTPUT │
         │            │   │     │          │    <email>→$EMAIL, <company>→$COMPANY   │
         │            │   │     │          │    <project>→$PROJECT                   │
         │            │   │     └── NO ────┘──► STOP — install skill first          │
         │            │   │                                                         │
         │            │   │  After success → proceed to Step 8                      │
         │            │   └─────────────────────────────────────┬───────────────────┘
         │            │                                         │
         │            └─────────────────────────────────────────┘
         │                                              │
         └──────────────────────────────────────────────┘
         │ (all paths converge here)
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 8: INITIALIZE GIT AND VERIFY OUTPUT (convergence point)                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ⚠️ Git init happens HERE — after conversion or validation.                    │
│  SMA CLI v1 creates Conversion-*/ subfolder — git must init AFTER              │
│  path resolution so .git is at the correct level.                              │
│                                                                                 │
│  8.1 Initialize Git (at resolved <output>):                                    │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │  cd <output>  (already resolved to Conversion-*/ for v1)        │            │
│  │  git rev-parse --is-inside-work-tree                            │            │
│  │                                                                 │            │
│  │  NOT a repo → git init → git add . → git commit                │            │
│  │               → git branch -M main                             │            │
│  │  Already a repo → check clean → stash/commit/abort if dirty    │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│  git checkout -b sma/migration-process                                          │
│                                                                                 │
│  8.2 Verify output:                                                             │
│  Check: <output>/Output/           ← Converted Snowpark Python code             │
│         <output>/Reports/Issues.csv ← EWI data                                 │
│         <output>/Reports/ArtifactDependencyInventory.csv                        │
│         <output>/Reports/InputFilesInventory.csv                                │
│                                                                                 │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 9: INVOKE DASHBOARD GENERATOR                                             │
│  cd "<output>"                                                                  │
│  skill("sma-dashboard-generator")                                           │
│                                                                                 │
│  This parses Issues.csv, generates the interactive EWI dashboard,               │
│  and starts a local server at localhost:8080                                     │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 10: RUN NOTEBOOK MIGRATION                                                │
│                                                                                 │
│  Check <config.run_notebook_migration>:                                         │
│    "no"     → skip entirely → Go to Step 11                                     │
│    "yes"    → scan and auto-run (no user prompt)                                │
│    not set  → scan, then ask_user_question → "Run Notebook Migration?" (Y/N)    │
│                                                                                 │
│  Scan <output>/Output/ for notebook files:                                      │
│    .ipynb, Databricks .py (# Databricks notebook source),                       │
│    Databricks .scala (// Databricks notebook source)                            │
│                                                                                 │
│  No notebooks found → silently skip → Go to Step 11                             │
│  If Yes/auto: skill("snowflake-notebook-migration") with context:               │
│    <output>, conversion_type, enable_jupyter_conversion, git branch             │
│  IMPORTANT: NEVER spawn as background agent (run_in_background=true)            │
│  If No: Skip — user can run snowflake-notebook-migration later                  │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 11: ASK TO RUN EWI FIXER                                                  │
│                                                                                 │
│  Check <config.run_ewi_fixer>:                                                  │
│    "no"     → skip entirely → Go to Step 12                                    │
│    "yes"    → auto-run with config (ewi_comments, ewi_scope)                   │
│    not set  → ask_user_question → "Run EWI Fixer?" (Yes / No)                  │
│                                                                                 │
│  If Yes: Configure options (comment handling, scope) → persist                 │
│          skill("dvp-ewi-fixer") with pre-configured answers                     │
│          IMPORTANT: NEVER spawn as background agent                             │
│  If No: Skip — user can run dvp-ewi-fixer later                                │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 12: ASK TO RUN STAGE CONVERSION                                           │
│                                                                                 │
│  Check <config.run_stage_conversion>:                                           │
│    "no"     → skip entirely → Go to Step 13                                    │
│    "yes"    → auto-run with config (stage_name as prefix)                      │
│    not set  → ask_user_question → "Replace embedded file paths?" (Yes / No)    │
│                                                                                 │
│  If Yes: skill("stage-conversion") with stage prefix from config            │
│  IMPORTANT: NEVER spawn as background agent (run_in_background=true)            │
│  If No: Skip — user can run stage-conversion later → Go to Step 13         │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 13: DETERMINE DVP ORCHESTRATOR                                           │
│  Read <config.run_dvp_orchestrator> (default: "yes"). Do NOT ask the user.    │
│    "no"     → Skip DVP entirely → Go to Step 14                                │
│    "yes"    → Run skill("dvp-orchestrator")                                    │
│                                                                                 │
│  If yes: skill("dvp-orchestrator")                                             │
│    Passes pre-configured context: <input> and <output> paths                    │
│    DVP Orchestrator skips Step 1 (path detection)                               │
│  IMPORTANT: NEVER spawn as background agent (run_in_background=true)            │
│  If no: Skip — user can run dvp-orchestrator later → Go to Step 14             │
└────────┬────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  STEP 14: OPEN DASHBOARD                                                        │
│  Re-open the SMA Dashboard in the browser so user sees final state              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Database and Git Access Module

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  DATABASE AND GIT MODULE (sma_api.py) — UNIFIED DATABASE AND GIT ACCESS        │
│  Location: skills/spark-migration/scripts/sma_api.py                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Importable Python module                                                       │
│  59 public functions + 11 private helpers                                       │
│  (48 database functions + 11 git functions)                                     │
│                                                                                 │
│  All functions receive workload_path: str as FIRST parameter                   │
│  DB path: {workload_path}/sma_storage.sqlite3                                  │
│                                                                                 │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐  │
│  │ Initialization (3)   │  │ Read — EWI (8)       │  │ Read — Files (3)     │  │
│  ├──────────────────────┤  ├──────────────────────┤  ├──────────────────────┤  │
│  │ initialize_database  │  │ get_migration_summary│  │ list_files           │  │
│  │ create_artifact_     │  │ list_ewis            │  │ get_file_details     │  │
│  │   dependency_tables  │  │ get_blockers         │  │ get_ewi_descriptions │  │
│  │ create_input_files_  │  │ get_pending_ewi_codes│  └──────────────────────┘  │
│  │   table              │  │ get_ewis_by_code     │                            │
│  └──────────────────────┘  │ get_ewis_by_file     │  ┌──────────────────────┐  │
│                            │ get_summary_stats    │  │ Read — Deps (4)      │  │
│  ┌──────────────────────┐  │ get_ewi_code_stats   │  ├──────────────────────┤  │
│  │ Write — Status (9)   │  └──────────────────────┘  │ get_dependency_      │  │
│  ├──────────────────────┤                            │   summary            │  │
│  │ update_ewi_status    │  ┌──────────────────────┐  │ get_file_            │  │
│  │ update_file_status   │  │ EWI Fixer (8)        │  │   dependencies      │  │
│  │ update_line_status   │  ├──────────────────────┤  │ get_dependency_      │  │
│  │ bulk_update_ewi_     │  │ generate_fix_id      │  │   inventory          │  │
│  │   status             │  │ insert_fix_result    │  │ get_dependency_graph │  │
│  │ update_ewi_notes     │  │ batch_insert_fix_    │  └──────────────────────┘  │
│  │ update_ewi_status_   │  │   results            │                            │
│  │   single             │  │ get_fix_results      │  ┌──────────────────────┐  │
│  │ update_dependency_   │  │ get_fix_results_stats│  │ Reset (2)            │  │
│  │   status             │  │ insert_summary_start │  ├──────────────────────┤  │
│  │ update_file_         │  │ update_summary_end   │  │ reset_not_resolved_  │  │
│  │   validation         │  │ get_fix_summary      │  │   to_pending         │  │
│  │ update_recommended_  │  └──────────────────────┘  │ reset_all_to_pending │  │
│  │   actions            │                            └──────────────────────┘  │
│  └──────────────────────┘  ┌──────────────────────┐                            │
│                            │ Overview (1)         │                            │
│                            ├──────────────────────┤                            │
│                            │ save_overview_stats  │                            │
│                            └──────────────────────┘                            │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │ Git Tools (11 public + 1 private)                                      │    │
│  │ MIGRATION_BRANCH = "sma/migration-process"                             │    │
│  ├─────────────────────────────────────────────────────────────────────────┤    │
│  │ _git()               (private) Run git commands in workload dir        │    │
│  │ git_is_repo()        Check if directory is a git repository            │    │
│  │ git_current_branch() Get current branch name                           │    │
│  │ git_branch_exists()  Check if branch exists                            │    │
│  │ git_is_clean()       Check if working tree is clean                    │    │
│  │ git_status()         Get detailed git status                           │    │
│  │ git_init_if_needed() Idempotent: init + commit + rename to main        │    │
│  │ git_stash()          Stash changes (--include-untracked)               │    │
│  │ git_ensure_branch()  Create/checkout migration branch                  │    │
│  │ git_commit()         Commit all changes with message                   │    │
│  │ git_verify_branches()Verify main + migration branches exist            │    │
│  │ git_ensure_ready()   High-level: init → stash → ensure branch          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Inter-Skill Relationships

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  SKILL ORCHESTRATION                                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  spark-migration                                                      │  │
│  │  (SMA CLI runner — OWNER of sma_api.py)                                   │  │
│  │                                                                           │  │
│  │  Steps 1-8: Load config, collect info, choose path, convert               │  │
│  │  Step 9:  Invokes sma-dashboard-generator ─────────────────────────┐      │  │
│  │  Step 10: Invokes snowflake-notebook-migration (optional) ──────┐  │      │  │
│  │  Step 11: Invokes dvp-ewi-fixer (optional) ──────────────────┐  │  │      │  │
│  │  Step 12: Invokes stage-conversion (optional) ────────────┐  │  │  │      │  │
│  │  Step 13: Invokes dvp-orchestrator (optional) ─────────┐  │  │  │  │      │  │
│  └────────────────────────────────────────────────────────┼──┼──┼──┼──┼──────┘  │
│                                                           │  │  │  │  │         │
│                       ┌───────────────────────────────────┘  │  │  │  │         │
│                       │  ┌───────────────────────────────────┘  │  │  │         │
│                       │  │  ┌───────────────────────────────────┘  │  │         │
│                       │  │  │  ┌───────────────────────────────────┘  │         │
│                       │  │  │  │  ┌───────────────────────────────────┘         │
│                       ▼  │  │  │  │                                             │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  sma-dashboard-generator                                                  │  │
│  │  (Dashboard + HTML generation)                                            │  │
│  │                                                                           │  │
│  │  Uses sma_api.py: initialize_database,                                    │  │
│  │    create_artifact_dependency_tables,                                     │  │
│  │    create_input_files_table, get_migration_summary,                       │  │
│  │    save_overview_stats                                                    │  │
│  │                                                                           │  │
│  │  Does NOT invoke dvp-ewi-fixer                                            │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                          │  │  │  │                                             │
│                          └──┼──┼──┼──┐                                          │
│                             ▼  │  │  │                                          │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  snowflake-notebook-migration                                             │  │
│  │  (Databricks notebook → Snowflake Workspace .ipynb)                       │  │
│  │                                                                           │  │
│  │  Converts notebooks in-place in <output>/Output/                          │  │
│  │  Uses sma_api.git_commit() for git operations                             │  │
│  │  Uses sma/migration-process git branch                                    │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                │  │  │                                          │
│                                └──┼──┼──┐                                       │
│                                   ▼  │  │                                       │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  dvp-ewi-fixer                                                            │  │
│  │  (Automated EWI resolution)                                               │  │
│  │                                                                           │  │
│  │  Uses sma_api.py: initialize_database,                                    │  │
│  │    get_pending_ewi_codes, get_ewis_by_code,                               │  │
│  │    batch_insert_fix_results, update_ewi_status_single,                    │  │
│  │    update_summary_end, reset_all_to_pending,                              │  │
│  │    reset_not_resolved_to_pending, get_summary_stats                       │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                      │  │                                       │
│                                      └──┼──┐                                    │
│                                         ▼  │                                    │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  stage-conversion                                                         │  │
│  │  (Replace embedded file paths with Snowflake stages)                      │  │
│  │                                                                           │  │
│  │  Works on <output>/Output/ files                                          │  │
│  │  Uses sma_api.git_is_repo() for git check                                 │  │
│  │  Uses sma/migration-process git branch                                    │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                            │                                    │
│                                            ▼                                    │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │  dvp-orchestrator                                                         │  │
│  │  (Initialize DVP validation workspace)                                    │  │
│  │                                                                           │  │
│  │  Receives <input> and <output> from spark-migration                   │  │
│  │  Creates dvp/ workspace (01-source, 02-migrated, etc.)                    │  │
│  │  Kicks off DVP validation pipeline sub-skills                             │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
│  Shared resource: sma_storage.sqlite3 (in workload directory)                   │
│  Shared access:   sma_api.py (scripts/sma_api.py)                               │
│                   config_manager.py (scripts/config_manager.py)                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Database Schema

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  sma_storage.sqlite3                                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────┐    ┌────────────────────────────────────────┐  │
│  │ Issues                      │    │ ewi_fixer_results                      │  │
│  │ (from Reports/Issues.csv)   │    │ (per-EWI fix tracking)                 │  │
│  ├─────────────────────────────┤    ├────────────────────────────────────────┤  │
│  │ id (PK, autoincrement)      │    │ id (PK, autoincrement)                 │  │
│  │ Code          (CAPITALIZED) │    │ fix_id (FK → summary)                  │  │
│  │ Category      (CAPITALIZED) │    │ ewi_code                               │  │
│  │ Description   (CAPITALIZED) │    │ fix_description                        │  │
│  │ FileId        (CAPITALIZED) │    │ affected_file (full path)              │  │
│  │ Line          (CAPITALIZED) │    │ affected_lines                         │  │
│  │ Column        (CAPITALIZED) │    │ status ('success'/'failed')            │  │
│  │ Url           (CAPITALIZED) │    │ timestamp                              │  │
│  │ CellId        (CAPITALIZED) │    └────────────────────────────────────────┘  │
│  │ status (added, lowercase)   │                                                │
│  │ notes (added, lowercase)    │    ┌────────────────────────────────────────┐  │
│  │ ⚠️ CSV headers = col names   │    │ ewi_fixer_summary                      │  │
│  │ Indexes: idx_code,          │    │ (per-session tracking)                  │  │
│  │   idx_status, idx_code_     │    ├────────────────────────────────────────┤  │
│  │   status                    │    │ fix_id (PK, UUID)                       │  │
│  └─────────────────────────────┘    │ total_ewis                              │  │
│                                     │ auto_resolved_ewis                      │  │
│  ┌─────────────────────────────┐    │ not_auto_resolved_ewis                  │  │
│  │ input_files_inventory       │    │ total_files_fixed                       │  │
│  │ (from InputFiles.csv)       │    │ total_not_auto_resolved_files           │  │
│  ├─────────────────────────────┤    │ compilation_errors_fixed                │  │
│  │ element                     │    │ start_time                              │  │
│  │ project_id                  │    │ end_time                                │  │
│  │ file_id                     │    └────────────────────────────────────────┘  │
│  │ count                       │                                                │
│  │ session_id                  │    ┌────────────────────────────────────────┐  │
│  │ extension                   │    │ artifact_dependency_inventory           │  │
│  │ technology                  │    ├────────────────────────────────────────┤  │
│  │ bytes                       │    │ execution_id                            │  │
│  │ character_length            │    │ file_id                                 │  │
│  │ lines_of_code               │    │ dependency                              │  │
│  │ parse_result                │    │ type (UserCodeFile, IOSources,          │  │
│  │ ignored                     │    │   ThirdPartyLibraries, UnknownLibraries,│  │
│  │ origin_file_path            │    │   SqlObject)                            │  │
│  └─────────────────────────────┘    │ success, status_detail, arguments      │  │
│                                     │ location                                │  │
│  ┌─────────────────────────────┐    │ indirect_dependencies                   │  │
│  │ artifact_dependency_summary │    │ direct_parents, total_direct_parents    │  │
│  ├─────────────────────────────┤    │ indirect_parents, total_indirect_parents│  │
│  │ execution_id                │    │ status                                  │  │
│  │ file_id                     │    └────────────────────────────────────────┘  │
│  │ island                      │                                                │
│  │ total_user_code_file        │    ┌────────────────────────────────────────┐  │
│  │ total_io_sources            │    │ artifact_dependency_graph               │  │
│  │ total_third_party_libraries │    ├────────────────────────────────────────┤  │
│  │ total_unknown_libraries     │    │ source                                  │  │
│  │ total_sql_object            │    │ target                                  │  │
│  │ total_dependencies          │    │ island                                  │  │
│  │ total_issues                │    └────────────────────────────────────────┘  │
│  │ validated                   │                                                │
│  │ recommended_actions         │    ┌────────────────────────────────────────┐  │
│  └─────────────────────────────┘    │ overview_stats                          │  │
│                                     ├────────────────────────────────────────┤  │
│                                     │ id (PK)                                 │  │
│                                     │ stat_key                                │  │
│                                     │ stat_value                              │  │
│                                     │ updated_at                              │  │
│                                     └────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```
## Output Structure

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  SMA OUTPUT STRUCTURE                                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  <output>/                                                                      │
│  ├── Output/                              ← Converted Snowpark Python code      │
│  │   ├── *.py                             ← Converted Python files              │
│  │   └── *.ipynb                          ← Converted notebooks                 │
│  │                                                                              │
│  ├── Reports/                             ← SMA-generated reports               │
│  │   ├── Issues.csv                       ← EWIs from conversion                │
│  │   ├── IssuesConnect.csv                ← Alternative EWI source              │
│  │   ├── Inventory.csv                    ← Object inventory                    │
│  │   ├── IOInventory.csv                  ← Input/Output mappings               │
│  │   ├── ArtifactDependencyInventory.csv  ← Dependency data                     │
│  │   └── InputFilesInventory.csv          ← Input file metadata                 │
│  │                                                                              │
│  ├── Logs/                                ← SMA execution logs                  │
│  │                                                                              │
│  ├── sma_storage.sqlite3                  ← Unified database (created by sma_api) │
│  │                                                                              │
│  └── sma-dashboard/        ← Generated by dashboard-generator    │
│      ├── manifest.json                                                          │
│      ├── index.html                                                             │
│      ├── server/                                                                │
│      │   ├── sma_server.py                                                      │
│      │   └── start_server.py                                                    │
│      ├── assets/styles.css                                                      │
│      └── ewi-tracker/                                                           │
│          ├── overview.html                                                      │
│          ├── ewi_tracker.html                                                   │
│          ├── file_tracker.html                                                  │
│          ├── dependency_tracker.html                                            │
│          └── content/                                                           │
│              ├── ewi_tracker/files_*.html                                       │
│              ├── file_tracker/file_ewis_*.html                                  │
│              └── dependency_tracker/dep_*.html                                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Replaces (Legacy)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  LEGACY → UNIFIED MODULE                                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  BEFORE (multiple scripts):                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  scripts/sma_database.py      ← (REMOVED, merged into sma_api.py)      │    │
│  │  scripts/sma_mcp_server.py    ← Separate server (dashboard, 11 tools)  │    │
│  │  scripts/sma_server.py        ← HTTP server DB helpers                  │    │
│  │  scripts/sma_manager.py       ← DB creation/initialization              │    │
│  │  extractors/ewi_extractor.py  ← EWI aggregation logic                   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  AFTER (scripts/):                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  scripts/sma_api.py             ← Unified importable module              │    │
│  │                                  59 functions, workload_path first param │    │
│  │                                  11 private helpers                      │    │
│  │                                  Inline EWI aggregation                  │    │
│  │                                  No --db flag needed                     │    │
│  │  scripts/config_manager.py      ← Project + global config management        │    │
│  │                                  list/load/create/save configurations   │    │
│  │                                  load-global/save-global for config.json│    │
│  │                                  CLI: python3 config_manager.py <cmd>   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  Key changes:                                                                   │
│  - db = initialize_database(dir)  →  sma_api.initialize_database(workload)       │
│  - db.get_pending_ewi_codes()     →  sma_api.get_pending_ewi_codes(workload)     │
│  - db.close()                     →  (not needed, connections auto-close)        │
│  - --db <path> flag               →  workload_path first param on every func    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Important Constraints

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  CONSTRAINTS & WARNINGS                                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ⚠️  NEVER search the filesystem for the SMA CLI (no find, which, locate,       │
│      mdfind, or any directory scanning). ALWAYS ask the user explicitly.         │
│                                                                                 │
│  ⚠️  SMA CLI MUST run in background mode (run_in_background: true)              │
│      Large workloads can take several minutes                                   │
│                                                                                 │
│  ⚠️  Git is initialized in <output> AFTER conversion (Step 7.1)                 │
│      Initial commit on main, work on sma/migration-process branch              │
│                                                                                 │
│  ⚠️  All sma_api.py functions require workload_path as first parameter           │
│      DB is always at {workload_path}/sma_storage.sqlite3                        │
│                                                                                 │
│  ⚠️  After SMA completes, ALWAYS invoke sma-dashboard-generator             │
│      The dashboard depends on Reports/ CSV files generated by SMA               │
│                                                                                 │
│  ⚠️  After dashboard, run notebook migration (Step 10)                           │
│      Check config / scan for notebooks → invoke snowflake-notebook-migration    │
│      Invoke with skill("snowflake-notebook-migration") — NEVER as background    │
│                                                                                 │
│  ⚠️  After notebook migration, ASK user to run dvp-ewi-fixer                   │
│      Invoke with skill("dvp-ewi-fixer") — NEVER as background agent            │
│                                                                                 │
│  ⚠️  After ewi-fixer, ASK user to run stage-conversion                     │
│      Invoke with skill("stage-conversion") — NEVER as background agent     │
│                                                                                 │
│  ⚠️  After stage-conversion, ASK user to run dvp-orchestrator                   │
│      Invoke with skill("dvp-orchestrator") — NEVER as background agent         │
│      Pass <input> and <output> paths so it skips Step 1                         │
│                                                                                 │
│  ⚠️  Config files (configurations/<project>.json) store settings per project     │
│      Global config (config.json) stores sma_cli_path shared across projects   │
│      Always confirm/validate path on each run                                   │
│                                                                                 │
│  ⚠️  All git operations MUST use sma_api.py git functions                        │
│      No local git helper functions in individual skill scripts                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```
