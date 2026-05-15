---
name: spark-migration
description: "Run the SMA CLI to convert PySpark code to Snowpark, or generate dashboard and fix EWIs from existing SMA output. Supports both Snowpark API and Snowpark Connect (SCOS) conversion paths. Triggers: run sma, convert spark, migrate pyspark, sma conversion, migrate to snowpark, convert to snowpark, already ran sma, sma dashboard, fix ewis, stage conversion, snowpark connect, scos, scos migration, migrate to snowpark connect, migrate to scos."
---

# Spark Migration

Executes the Snowflake Migration Accelerator (SMA) CLI to convert PySpark code to Snowpark Python.

## Overview

This skill supports three flows:

1. **(a) Already migrated** — The user already has SMA/SCOS output. Provide the result path, verify structure, initialize git, and proceed to dashboard generation and post-conversion steps.
2. **(c) Snowpark API conversion** — Run the SMA CLI to convert PySpark code, then generate a dashboard and fix EWIs.
3. **(d) Snowpark Connect conversion** — Invoke the `snowpark-connect` skill to perform the conversion.

## Requirements

- **Already migrated (a)**: The SMA/SCOS output folder path (must contain `Output/` and `Reports/`)
- **Snowpark API conversion (c)**: SMA CLI installed, input path (PySpark source), output path, project metadata
- **Snowpark Connect conversion (d)**: `snowpark-connect` skill installed, input path, output path, project metadata

## Output Format

Every time you begin a step, sub-step, or significant action, prefix the message with a timestamp in the format `[YYYY-MM-DD HH:MM:SS]`. Obtain the current time by running `date '+%Y-%m-%d %H:%M:%S'` in bash.

Example:
```
[2026-03-24 14:05:32] Starting Step 8: Initialize Git and Verify SMA Output...
[2026-03-24 14:05:33] Git repository already initialized.
[2026-03-24 14:05:34] Verifying SMA output structure...
[2026-03-24 14:06:01] Step 8 complete.
```

⛔ **IMPORTANT — Final Summary**: When all steps are done (Step 14), you MUST display the Final Summary using the EXACT template defined in the **"Final Summary"** section at the end of this document. The template includes a 4-column table with `#`, `Step`, `Status`, `Details` — where `#` uses numbering like 1, 1.1, 1.2, 2, 3, 6.3, etc. and `Step` uses actual skill names. There are exactly **7 top-level steps**: 1 (conversion skill), 2 (sma-dashboard-generator), 3 (snowflake-notebook-migration), 4 (dvp-ewi-fixer), 5 (stage-conversion), 6 (dvp-orchestrator), 7 (Open Dashboard). DVP sub-skills (ASG, entrypoints, etc.) are numbered 6.1–6.8 under step 6. Do NOT improvise your own summary format.

## Usage

### Step 1: Load Configuration

**Record `<start_time>` = current time** (used for duration in the Final Summary).

Before collecting project information, check for saved configurations in `<skill_directory>/configurations/`.

**Configurations directory:** `<skill_directory>/configurations/`

Each configuration is a JSON file named after the project: `configurations/<project_name>.json`.

A configuration file may contain:

| Key | Description | Possible Values | Default |
|-----|-------------|-----------------|---------|
| `conversion_type` | Preferred conversion tool | `scos`, `snowpark_api` | `scos` |
| `migration_status` | Whether to run conversion or use existing output | `migrate`, `already_migrated` | `migrate` |
| `email` | Customer email | Any valid email | — |
| `company` | Customer company name | Any string | — |
| `project_name` | Project name for reports | Any string | (derived from filename) |
| `input_folder` | Source code path (PySpark) | Any valid path | — |
| `output_folder` | Output folder path | Any valid path | — |
| `enable_jupyter_conversion` | Enable Jupyter notebook conversion | `yes`, `no` | `yes` |
| `sql_flavor` | SQL dialect for SMA CLI | `SparkSql`, `HiveSql`, `Databricks` | `SparkSql` |
| `generate_checkpoints` | Generate checkpoint files | `yes`, `no` | `yes` |
| `run_notebook_migration` | Run Notebook Migration after dashboard | `yes`, `no` | `yes` |
| `run_ewi_fixer` | Run EWI Fixer after Notebook Migration | `yes`, `no` | `yes` |
| `run_ewi_fixer.ewi_comments` | EWI comment handling mode | `mark`, `remove` | `mark` |
| `run_ewi_fixer.ewi_scope` | Which EWIs to process | `only_pending`, `retry_not_resolved`, `all_reset` | `only_pending` |
| `run_stage_conversion` | Run Stage Conversion after EWI Fixer | `yes`, `no` | `yes` |
| `run_stage_conversion.stage_name` | Stage prefix for path replacement | Any string | `migration_stage` |
| `run_dvp_orchestrator` | Run DVP Orchestrator after Stage Conversion | `yes`, `no` | `yes` |

**Global configuration** (`<skill_directory>/config.json`) stores settings shared across all projects:

| Key | Description | Possible Values |
|-----|-------------|-----------------|
| `sma_cli_path` | Path to SMA CLI binary | Any valid path |

Load the global config at startup:
```bash
python3 '<skill_directory>/scripts/config_manager.py' load-global '<skill_directory>'
```
Store the result as `<global_config>`. The `sma_cli_path` is read from here (not from the project config).

#### 1.1 List Available Configurations

```bash
python3 '<skill_directory>/scripts/config_manager.py' list '<skill_directory>/configurations'
```

**If configurations exist**, display the full numbered list as plain text:

```
Available configurations:

  1. my_project
  2. client_migration
  3. spark_etl
  4. legacy_etl_v2
  5. analytics_pipeline
  ...
```

Then use `ask_user_question` with exactly two options:
- **Use existing configuration** — "Select one of the configurations listed above"
- **Create new configuration** — "Start a new project configuration"

If the user selects **"Use existing configuration"**: ask for the configuration name or number. The user can type the name (e.g., `my_project`) or the number (e.g., `3`). Resolve the selection and go to **1.2 Load Existing Configuration**.

If the user selects **"Create new configuration"**: go to **1.3 Create New Configuration**.

**If no configurations exist** (directory is empty or does not exist), go directly to **1.3 Create New Configuration**.

#### 1.2 Load Existing Configuration

Read the selected file and merge defaults for any missing keys:

```bash
python3 '<skill_directory>/scripts/config_manager.py' load '<config_path>'
```

Store `<config_path>` for all subsequent persist operations. Store the loaded values as `<config>`. Proceed to Step 2.

#### 1.3 Create New Configuration

Present only the required parameters (those without defaults) in a single numbered list:

```
New configuration — please provide the following:

  1. Project Name:            (used as configuration filename)
  2. Source Code Path:        (PySpark source directory)
  3. Output Folder:           (where converted code will be saved)
  4. Customer Email:
  5. Customer Company:

Example: "1. my_project, 2. /Users/me/spark-etl, 3. /Users/me/output, 4. user@co.com, 5. Acme Inc"
```

**Parameter #1 (Project Name) is required** — it is used as the configuration filename. If the user does not provide it, ask for it before proceeding. Parameters #2-#5 can be left blank and filled in later.

Parse the user's response. Extract `<project_name>` from parameter #1.

Set `<config_path>` = `<skill_directory>/configurations/<project_name>.json`

Create the configuration with defaults, then immediately save all user-provided values:

```bash
python3 '<skill_directory>/scripts/config_manager.py' create '<skill_directory>/configurations' '<project_name>'
```

If the user provided additional parameters (#2-#5), persist them:
```bash
python3 '<skill_directory>/scripts/config_manager.py' save '<config_path>' '{"input_folder": "<input>", "output_folder": "<output>", "email": "<email>", "company": "<company>"}'
```
Include only the keys the user explicitly provided.

Store `<config_path>` for all subsequent persist operations. Store the loaded values as `<config>`. Proceed to Step 2.

**In Step 2**, the full summary (all 18 parameters with defaults pre-filled) is shown for the user to confirm or edit.

### Step 2: Review Configuration and Collect Project Information

**If config has saved values** (from a previous run), present a single summary of ALL current settings:

```
Current configuration:

  ── Project ──────────────────────────────────
  Email:               <config.email>
  Company:             <config.company>
  Project Name:        <config.project_name>
  Input Folder:        <config.input_folder>
  Output Folder:       <config.output_folder>

  ── Conversion ───────────────────────────────
  Conversion Type:     <config.conversion_type>           (scos / snowpark_api)
  Migration Status:    <config.migration_status>           (migrate / already_migrated)
  SMA CLI Path:        <global_config.sma_cli_path or (not set)>  (only for snowpark_api)
  Jupyter Conversion:  <config.enable_jupyter_conversion> (yes / no, only for snowpark_api)
  SQL Flavor:          <config.sql_flavor>                (SparkSql / HiveSql / Databricks, only for snowpark_api)
  Checkpoints:         <config.generate_checkpoints>      (yes / no, only for snowpark_api)

  ── Post-Conversion ──────────────────────────
  Run Notebook Migration: <config.run_notebook_migration> (yes / no)
  Run EWI Fixer:          <config.run_ewi_fixer>             (yes / no)
    EWI Comments:         <config.run_ewi_fixer.ewi_comments> (mark / remove)
    EWI Scope:            <config.run_ewi_fixer.ewi_scope>   (only_pending / retry_not_resolved / all_reset)
  Run Stage Conversion:   <config.run_stage_conversion>       (yes / no)
    Stage Name:           <config.run_stage_conversion.stage_name>
  Run DVP Orchestrator:   <config.run_dvp_orchestrator>       (yes / no)

Would you like to use these settings or edit them?
```

Use `ask_user_question`:
- **Use these settings** — proceed with all configured values
- **Edit settings** — ask for each field that the user wants to change

**If user selects "Use these settings"**: Set all variables from config values (`<email>`, `<company>`, `<project>`, `<input>`, `<output>`, etc.). Proceed to Step 3.

**If user selects "Edit settings"** (or this is the first run with no saved values):

Present ALL parameters in a single numbered list. Show current/default values inline so the user only needs to provide the ones they want to set or change. The user can respond with just the numbers they want to update (e.g., "1. /path/to/source, 3. user@co.com"). Any parameter not mentioned keeps its current/default value.

```
Please review and provide the missing settings.
Only specify the numbers you want to set or change:

 ── Project ──────────────────────────────────
  1. Source Code Path:        <config.input_folder or (not set)>
  2. Output Folder:           <config.output_folder or (not set)>
  3. Customer Email:          <config.email or (not set)>
  4. Customer Company:        <config.company or (not set)>
  5. Project Name:            <config.project_name>

 ── Conversion ───────────────────────────────
  6. Conversion Type:         <config.conversion_type>          (scos / snowpark_api)
  7. Migration Status:        <config.migration_status>          (migrate / already_migrated)
   8. SMA CLI Path:            <global_config.sma_cli_path or (not set)>  (only for snowpark_api — saved globally)
  9. Jupyter Conversion:      <config.enable_jupyter_conversion> (yes / no)
 10. SQL Flavor:              <config.sql_flavor>                (SparkSql / HiveSql / Databricks)
 11. Generate Checkpoints:    <config.generate_checkpoints>      (yes / no)

 ── Post-Conversion ──────────────────────────
 12. Run Notebook Migration:  <config.run_notebook_migration>     (yes / no)
 13. Run EWI Fixer:           <config.run_ewi_fixer>             (yes / no)
 14. EWI Comments:            <config.run_ewi_fixer.ewi_comments> (mark / remove)
 15. EWI Scope:               <config.run_ewi_fixer.ewi_scope>   (only_pending / retry_not_resolved / all_reset)
 16. Run Stage Conversion:    <config.run_stage_conversion>       (yes / no)
 17. Stage Name:              <config.run_stage_conversion.stage_name>
 18. Run DVP Orchestrator:    <config.run_dvp_orchestrator>       (yes / no)

Example response: "1. /Users/me/spark-etl, 2. /Users/me/output, 3. user@company.com, 4. Acme Inc"
```

Parse the user's response. For each numbered item provided, update the corresponding parameter. Parameters not mentioned retain their current/default value.

Map the numbers to config keys:

| # | Config Key |
|---|---|
| 1 | `input_folder` |
| 2 | `output_folder` |
| 3 | `email` |
| 4 | `company` |
| 5 | `project_name` |
| 6 | `conversion_type` |
| 7 | `migration_status` |
| 8 | `sma_cli_path` (**global** — saved to `config.json`) |
| 9 | `enable_jupyter_conversion` |
| 10 | `sql_flavor` |
| 11 | `generate_checkpoints` |
| 12 | `run_notebook_migration` |
| 13 | `run_ewi_fixer` |
| 14 | `run_ewi_fixer.ewi_comments` |
| 15 | `run_ewi_fixer.ewi_scope` |
| 16 | `run_stage_conversion` |
| 17 | `run_stage_conversion.stage_name` |
| 18 | `run_dvp_orchestrator` |

Store the resolved values as `<input>`, `<output>`, `<email>`, `<company>`, `<project>`, etc.

**Persist ALL values** to the active configuration (deterministic merge):
```bash
python3 '<skill_directory>/scripts/config_manager.py' save '<config_path>' '{"email": "<email>", "company": "<company>", "project_name": "<project>", "input_folder": "<input>", "output_folder": "<output>", ...}'
```
Include only the keys that were set or changed in the JSON argument. **Do NOT include `sma_cli_path`** in the project save — it is saved globally.

If `sma_cli_path` (#8) was provided or changed, persist it separately to the global config:
```bash
python3 '<skill_directory>/scripts/config_manager.py' save-global '<skill_directory>' '{"sma_cli_path": "<sma_cli_path>"}'
```

### Step 3: Determine Migration Status

Read `<config.migration_status>` from the configuration set in Step 2.

- If `<config.migration_status>` is `already_migrated` → Go to Step 4
- If `<config.migration_status>` is `migrate` → Go to Step 5

### Step 4: Validate Existing Output (already_migrated)

The user already has a conversion result. Ask for the result path and validate it.

#### 4.0 Ask for the Output Path

If `<config.output_folder>` is set, pre-fill it as the default:
```
Please provide the path to your SMA/SCOS output folder.
[Default: <config.output_folder>]
```
If the user accepts the default (empty response or confirms), use `<config.output_folder>`.
Otherwise, use the user-provided path.

Store as `<output>`.

#### 4.1 Validate Output Structure

```bash
test -d "<output>/Output" && test -d "<output>/Reports" && echo "Valid" || echo "Invalid"
```

**If invalid**, check if `<output>` contains a `Conversion-*` folder (SMA v1 format):

```bash
ls -d "<output>"/Conversion-* 2>/dev/null | head -1
```

If a `Conversion-*` folder is found, resolve `<output>` to that folder:
```
<output> = <output>/Conversion-<timestamp>/
```
Then re-validate:
```bash
test -d "<output>/Output" && test -d "<output>/Reports" && echo "Valid" || echo "Invalid"
```

**If still invalid**, inform the user and ask for the correct path:
```
The provided path does not contain the expected Output/ and Reports/ subdirectories.
Please provide the correct SMA output folder path.
```

#### 4.2 Confirm and Proceed

```
SMA output folder validated:
  Output:  <output>/Output/
  Reports: <output>/Reports/

Proceeding to initialize git and verify output...
```

**Go to Step 8.**

### Step 5: Choose Conversion Tool (migrate)

The user wants to run a new conversion. First collect the output path, then ask which tool.

#### 5.0 Ask for the Output Path

If `<config.output_folder>` is set, pre-fill it as the default:
```
Where should the converted code be saved? (Output path)
[Default: <config.output_folder>]
```
If the user accepts the default (empty response or confirms), use `<config.output_folder>`.
Otherwise, use the user-provided path.

Store as `<output>`.

#### 5.1 Choose Tool

If `<config.conversion_type>` is set, pre-select the matching option. Ask using `ask_user_question`:

```
Which conversion tool would you like to use?
[Default: <config.conversion_type>]
```

Options:
- **(c) Snowpark API** — "Convert using the SMA CLI (Snowpark API)"
- **(d) Snowpark Connect** — "Convert using the snowpark-connect skill (SCOS)"

**Persist the selection** (deterministic merge):
```bash
python3 '<skill_directory>/scripts/config_manager.py' save '<config_path>' '{"conversion_type": "snowpark_api"}'
# Use "scos" instead of "snowpark_api" if (d) selected
```

#### If user selects **(c) Snowpark API** → Go to Step 6

#### If user selects **(d) Snowpark Connect** → Go to Step 7

### Step 6: Snowpark API Conversion via SMA CLI (Option c)

#### 6.1 Load or Configure SMA CLI Path

The SMA CLI path is loaded from the **global config** (`<global_config.sma_cli_path>`), not from the project config.

1. **If `<global_config.sma_cli_path>` is set**:
   Validate it still exists:
   ```bash
   test -x "<global_config.sma_cli_path>" && echo "Valid" || echo "Invalid"
   ```
   - If **valid**: use `<sma_cli>` = `<global_config.sma_cli_path>`, skip to Step 6.2.
   - If **invalid**: inform the user and ask for a new path (see step 2 below).

2. **If `<global_config.sma_cli_path>` is NOT set** (first run or missing):

   **IMPORTANT**: NEVER search the filesystem for the SMA CLI (no `find`, `which`, `locate`, `mdfind`, or any directory scanning). ALWAYS ask the user explicitly.

   Ask the user for the SMA CLI path using `ask_user_question`:
   ```
   I need to know where your SMA CLI is installed.
   Please provide the full path to the SMA CLI executable (the 'sma' binary inside the orchestrator folder).
   Example: /Users/Shared/AplicacionesSMA/GA/GA/SMA-CLI-arm64-mac/orchestrator/sma
   ```

   Validate:
   ```bash
   test -x "<user_provided_path>" && echo "Valid" || echo "Invalid"
   ```

   **If valid**, persist to the **global** config:
   ```bash
python3 '<skill_directory>/scripts/config_manager.py' save-global '<skill_directory>' '{"sma_cli_path": "<user_provided_path>"}'
```

**Store the validated path as `<sma_cli>`.**

#### 6.2 Collect CLI-Specific Fields

These fields have config defaults from Step 1. If all three are already set in `<config>`, skip asking and use the saved values directly. Otherwise, ask only for fields that are missing.

| # | Field | Config Key | Variable | Default |
|---|-------|------------|----------|---------|
| 5 | Enable Jupyter Conversion? | `enable_jupyter_conversion` | `Y` or `N` | `Y` (enabled) |
| 6 | SQL Flavor | `sql_flavor` | See options below | `SparkSql` |
| 7 | Generate Checkpoints? | `generate_checkpoints` | `Y` or `N` | `Y` (enabled) |

**SQL Flavor Options:** `SparkSql` | `HiveSql` | `Databricks`

Map config values to CLI variables:
- `<config.enable_jupyter_conversion>`: `yes` → `Y`, `no` → `N`
- `<config.sql_flavor>`: use directly (`SparkSql`, `HiveSql`, `Databricks`)
- `<config.generate_checkpoints>`: `yes` → `Y`, `no` → `N`

**If any fields were asked** (not from config), persist them (deterministic merge):
```bash
python3 '<skill_directory>/scripts/config_manager.py' save '<config_path>' '{"enable_jupyter_conversion": "yes", "sql_flavor": "SparkSql", "generate_checkpoints": "yes"}'
# Replace values with actual user selections (e.g., "no", "HiveSql", etc.)
```

#### 6.3 Run the SMA CLI

Execute the SMA CLI with the collected project information:

```bash
"<sma_cli>" -i "<input>" -o "<output>" \
    -e "<email>" -c "<company>" -p "<project>" -y [optional-flags]
```

**Complete parameter reference:**

| Short | Long | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `-i` | `--input` | Yes | Path to the input folder | — |
| `-o` | `--output` | Yes | Path to the output folder | — |
| `-e` | `--customerEmail` | Yes | Customer email | — |
| `-c` | `--customerCompany` | Yes | Customer company | — |
| `-p` | `--projectName` | Yes (first run) | Project name | — |
| `-x` | `--disableJupyterConversion` | No | Disable Jupyter notebook conversion | Enabled |
| `-f` | `--sql` | No | SQL flavor: `Databricks`, `SparkSql`, `HiveSql` | `SparkSql` |
| `-d` | `--disableCheckpoints` | No | Disable checkpoint file generation | Enabled |

**Optional flags logic:**

| Condition | Flag | Effect |
|-----------|------|--------|
| User answered **N** to Jupyter Conversion | `-x` | Disables conversion of Databricks/Scala/Python notebooks into `.ipynb` format |
| SQL Flavor is not `SparkSql` | `-f <dialect>` | Sets the SQL dialect for embedded SQL parsing |
| User answered **N** to Generate Checkpoints | `-d` | Disables checkpoint file generation |

**IMPORTANT**: Always run SMA in background mode to avoid timeouts on large workloads.

Use these Bash tool parameters:
- `run_in_background: true`
- `description: "Run SMA conversion in background"`

This returns a `shell_id` (e.g., `fc98`) that you'll use for monitoring.

#### 6.4 Monitor Progress

Poll the output every 5-10 seconds using `bash_output` with the shell_id:

```
bash_output(bash_id: "<shell_id>")
```

Key patterns to look for in the output:
- `[SMA] Step X/20 - <step_name>: STARTED` — Show current step
- `Found X Python files` — Report file counts
- `Info:` lines with useful status information
- `Conversion was successful` — Indicates completion

**Success indicators:**
- `Conversion was successful.`
- Exit code 0

**Failure indicators:**
- `Error:` messages
- `Execution aborted`
- Non-zero exit code

#### 6.5 Resolve SMA v1 Output Path

The SMA CLI (v1) creates a timestamped `Conversion-*` folder inside `<output>`. All subsequent steps must work **inside** this folder — do NOT copy its contents to the parent directory.

1. **Check if a `Conversion-*` folder was created inside `<output>`:**
   ```bash
   ls -d "<output>"/Conversion-* 2>/dev/null | head -1
   ```

2. **If found (v1 format):**
   - Select the most recent `Conversion-*` folder (by name, newest timestamp)
   - **Update `<output>`** to point to it:
     ```
     <output> = <output>/Conversion-<timestamp>/
     ```
   - Log: `Detected SMA v1 format. Resolved output path: <output>`

3. **If NOT found:**
   - Keep `<output>` unchanged for SMA layouts where migrated code already lives at `<output>/Output/` (e.g. **`sma-output/`** v2, or **v3** `.../Conversion_SnowparkAPI/sma-code-process-*` / `.../Conversion_SnowparkConnect/sma-code-process-*` when the migration flow already resolved the execution folder).
   - If the SMA output root contains **`Conversion_SnowparkAPI`** and/or **`Conversion_SnowparkConnect`** with nested **`sma-code-process-*`** folders and you have not yet narrowed `<output>`, **`dvp-orchestrator`** Step 1 resolves the concrete workload path (latest execution under the chosen **`Conversion_SnowparkAPI`** or **`Conversion_SnowparkConnect`** folder) — do not flatten or copy `Output/` upward.

**⚠️ CRITICAL: Do NOT copy `Output/`, `Reports/`, or `Logs/` from the `Conversion-*` folder to the parent directory. Always work inside the `Conversion-*` folder.**

**After successful Snowpark API conversion, proceed to Step 8.**

### Step 7: Snowpark Connect Conversion (Option d)

If the user selected **(d) Snowpark Connect conversion** in Step 5:

1. Search for the `snowpark-connect` skill in the available/installed skills.

2. **If skill is found**, invoke it with the collected inputs:

   | Parameter | Value | Maps to (skill variable) |
   |-----------|-------|--------------------------|
   | Source path | `<input>` | `$ARGUMENTS` — the PySpark file or directory to migrate |
   | Output path | `<output>` | `$OUTPUT` — migrated files and Reports/ are written here |
   | Customer Email | `<email>` | `$EMAIL` — project metadata for reports |
   | Customer Company | `<company>` | `$COMPANY` — project metadata for reports |
   | Project Name | `<project>` | `$PROJECT` — project name for reports |

3. **If skill is NOT found**, **STOP** the entire execution and inform the user:
   ```
   The `snowpark-connect` skill is not installed.
   Please install the skill before executing this conversion path.
   ```

**After successful Snowpark Connect conversion, proceed to Step 8.**

### Step 8: Initialize Git and Verify SMA Output

After conversion completes (or output is validated for path a), initialize the git repository and verify the output. At this point, `<output>` is already resolved to the correct directory (e.g., `Conversion-*/` for v1 format).

#### 8.1 Initialize Git Repository

**IMPORTANT**: Git is initialized HERE (after conversion) because the SMA CLI v1 creates a `Conversion-*` subfolder that becomes the working root. Initializing git before conversion would place `.git` at the wrong level.

1. **Navigate** to the resolved output directory:
   ```bash
   cd <output>
   ```

2. **Check** if Git repository already exists:
   ```bash
   git rev-parse --is-inside-work-tree
   ```

3. **If NOT a Git repository** (command fails):
   ```bash
   git init
   git add .
   git commit -m "Initial commit: SMA output before migration process"
   git branch -M main
   ```

   **⚠️ CRITICAL**: `git branch -M main` is **MANDATORY**. This ensures the default branch is named `main` so the original output is always preserved and accessible.

4. **If already a Git repository**:
   - Check if working directory is clean: `git status --porcelain`
   - **If clean**: proceed to Step 8.2
   - **If NOT clean** (has uncommitted changes): Ask user via `ask_user_question`:
     - **Stash changes**: `git stash push -m "Pre-migration stash"` (recover later with `git stash pop`)
     - **Commit changes**: Commit pending changes before proceeding
     - **Abort**: Stop and let user handle manually

5. **Create and checkout** the feature branch:
   ```bash
   git checkout -b sma/migration-process
   ```

   If branch already exists (from a previous run):
   ```bash
   git checkout sma/migration-process
   ```

**All subsequent file operations work on the `sma/migration-process` branch.**

#### 8.2 Verify SMA Output Structure

Verify the expected output structure exists:

| Path | Description |
|------|-------------|
| `<output>/Output/` | Converted Snowpark Python code |
| `<output>/Reports/` | Conversion reports and inventories |
| `<output>/Reports/Issues.csv` | EWI (Errors, Warnings, Issues) from conversion |

```bash
ls "<output>/Output/" && ls "<output>/Reports/Issues.csv"
```

### Step 9: Invoke Dashboard Generator

**IMPORTANT**: After SMA completes successfully, navigate to the output folder and invoke the dashboard generator:

1. **Change to the SMA output directory:**
   ```bash
   cd "<output>"
   ```

2. **Invoke the skill:** `sma-dashboard-generator`

This will:
- Parse `Reports/Issues.csv`
- Generate an interactive EWI tracking dashboard
- Start a local server and open the dashboard in the browser

### Step 10: Run Notebook Migration

After the dashboard is generated, check config defaults before proceeding:

**If `<config.run_notebook_migration>` is `no`**: Skip Notebook Migration entirely. Inform the user: "Skipping Notebook Migration (configured as disabled). You can run it later using the `snowflake-notebook-migration` skill." **Proceed to Step 11.**

**If `<config.run_notebook_migration>` is `yes`** (default): Proceed directly to invoking the skill (step 2 below). Do NOT scan for notebooks here — the child skill will scan using its own detection script. Do NOT ask the user.

**If `<config.run_notebook_migration>` is NOT set**: Scan for notebooks (step 1 below) and ask the user.

1. **Scan for notebooks** in `<output>/Output/` using the detection script from the `snowflake-notebook-migration` skill. Do NOT scan manually with `find`, `ls`, or other commands — always use this script:

   ```bash
   uv run --project "<snowflake-notebook-migration skill directory>" python "<snowflake-notebook-migration skill directory>/scripts/detect_and_parse_notebook.py" --scan "<output>/Output/"
   ```

   The script detects all Databricks notebook formats (`.ipynb`, `.python`, `.scala`, `.sql`, `.py`) and filters out plain scripts. It returns a JSON array.

   - **If no notebooks found** (empty array): Silently skip — do not prompt the user. Proceed directly to Step 11.
   - **If notebooks found**: Use `ask_user_question` tool:
     - Question: "Found N notebook(s) in the output. Would you like to run notebook migration to convert them to Snowflake Workspace format?"
     - Options: "Yes, run Notebook Migration" / "No, skip for now"

     If user selects **No**:
     - Inform the user: "Skipping Notebook Migration. You can run it later using the `snowflake-notebook-migration` skill."
     - **Persist the choice** (deterministic merge):
       ```bash
       python3 '<skill_directory>/scripts/config_manager.py' save '<config_path>' '{"run_notebook_migration": "no"}'
       ```
     - Proceed to Step 11.

2. **Invoke the Notebook Migration** (either from config `yes` or user selecting Yes):
   - Inform the user: "Running Notebook Migration..."
   - Invoke the `snowflake-notebook-migration` skill using the Skill tool:
     ```
     skill("snowflake-notebook-migration")
     ```
   - **IMPORTANT**: Use `skill("snowflake-notebook-migration")` in the foreground — **NEVER** spawn it as a background agent (`run_in_background=true`).
   - **IMPORTANT**: Immediately after the skill loads, send the context so it works in orchestrator mode:
     ```
     The following context was configured by the spark-migration orchestrator:
     - SMA output directory: <output>
     - Notebooks source: <output>/Output/ (notebooks are already here from SCOS copy or SMA conversion)
     - Git branch: sma/migration-process (already checked out)
     - Conversion type: <config.conversion_type> (scos or snowpark_api)
     - Jupyter conversion: <config.enable_jupyter_conversion> (yes or no; snowpark_api only)

     Scan <output>/Output/ for notebook files (.ipynb, .python, Databricks .py, .scala, .sql) and convert in-place.
     Use sma_api.git_commit() for git operations on the sma/migration-process branch.
     ```

Record the number of notebooks converted for the Final Summary.

### Step 11: Ask to Run EWI Fixer and Configure Options

After Notebook Migration completes (or is skipped), check config defaults before asking:

**If `<config.run_ewi_fixer>` is `no`**: Skip the EWI Fixer entirely. Inform the user: "Skipping EWI Fixer (configured as disabled). You can run it later using the `dvp-ewi-fixer` skill." **Go to Step 12.**

**If `<config.run_ewi_fixer>` is `yes`** (default): Use `<config.run_ewi_fixer.ewi_comments>` and `<config.run_ewi_fixer.ewi_scope>` as pre-configured options and proceed directly to invoking the fixer (step 4 below). Do NOT ask questions A and B.

**If `<config.run_ewi_fixer>` is NOT set**: Ask the user:

1. Use `ask_user_question` tool:
   - Question: "Would you like to run the EWI Fixer to automatically resolve issues in the converted files?"
   - Options: "Yes, run EWI Fixer" / "No, skip for now"

2. If user selects **No**:
   - Inform the user: "Skipping EWI Fixer. You can run it later using the `dvp-ewi-fixer` skill."
   - **Go to Step 12.**

3. If user selects **Yes**, ask the EWI Fixer configuration questions **before** invoking the skill:

   **Question A — EWI comment handling:**
   - Use `ask_user_question` tool
   - Question: "How should EWI comments be handled after fixing?"
   - Options:
     - **Mark**: Keep EWI comments but prefix with `[FIXED]` or `[NOT-FIXED]`
     - **Remove**: Delete EWI comments after applying fixes
   - Store the answer as `<ewi_comment_mode>` ("Mark" or "Remove")

   **Question B — Which EWIs to process:**
   - Use `ask_user_question` tool
   - Question: "Which EWIs should be processed?"
   - Options:
     - **Only pending**: Process only EWIs with status 'pending' (skips already processed)
     - **Retry not_auto_resolved**: Reset `not_auto_resolved` EWIs back to `pending` and retry them
     - **Specific EWI code**: Process only a specific EWI code (e.g., SPRKPY1002)
     - **All (reset)**: Reset all EWIs to 'pending' and reprocess everything
   - Store the answer as `<ewi_scope>`
   - If "Specific EWI code" selected: also ask for the code (e.g., "SPRKPY1002") and store as `<ewi_specific_code>`

   **Persist the EWI Fixer choices** (deterministic merge):
   ```bash
   python3 '<skill_directory>/scripts/config_manager.py' save '<config_path>' '{"run_ewi_fixer": "yes", "run_ewi_fixer.ewi_comments": "<ewi_comment_mode>", "run_ewi_fixer.ewi_scope": "<ewi_scope>"}'
   ```

4. **Invoke the EWI Fixer** with the pre-configured options:
   - Inform the user: "Running the EWI Fixer..."
   - Invoke the `dvp-ewi-fixer` skill using the Skill tool:
     ```
     skill("dvp-ewi-fixer")
     ```
   - **IMPORTANT**: Use `skill("dvp-ewi-fixer")` in the foreground — **NEVER** spawn it as a background agent (`run_in_background=true`).
   - **IMPORTANT**: Immediately after the skill loads, send the pre-configured answers so the EWI Fixer skips its Step 1 questions:
     ```
     The following options were already configured by the user:
     - SMA output directory: <output>
     - EWI comment handling: <ewi_comment_mode>
     - EWIs to process: <ewi_scope>
     [- Specific EWI code: <ewi_specific_code>]  (only if applicable)

     Skip Step 1 questions and proceed directly to Step 2 with these settings.
     ```
   - The EWI Fixer will:
     - Use the provided workload path (the SMA output directory)
     - Load the `sma_storage.sqlite3` database created by the dashboard generator
     - Scan converted files for EWI comments and resolve them
     - Update the database with resolution status

### Step 12: Ask to Run Stage Conversion

After the EWI Fixer completes (or is skipped), check config defaults before asking:

**If `<config.run_stage_conversion>` is `no`**: Skip Stage Conversion entirely. Inform the user: "Skipping Stage Conversion (configured as disabled). You can run it later using the `stage-conversion` skill." **Go to Step 13.**

**If `<config.run_stage_conversion>` is `yes`** (default): Proceed directly to invoking the skill (step 3 below) using `<config.run_stage_conversion.stage_name>` as the stage prefix. Do NOT ask the user.

**If `<config.run_stage_conversion>` is NOT set**: Ask the user:

1. Use `ask_user_question` tool:
   - Question: "Would you like to replace embedded file paths (s3://, hdfs://, etc.) with Snowflake stage references?"
   - Options: "Yes, run Stage Conversion" / "No, skip for now"

2. If user selects **No**:
   - Inform the user: "Skipping Stage Conversion. You can run it later using the `stage-conversion` skill."
   - **Go to Step 13.**

3. If user selects **Yes** (or auto-run from config):
   - Inform the user: "Running Stage Conversion..."
   - Invoke the `stage-conversion` skill using the Skill tool:
     ```
     skill("stage-conversion")
     ```
   - **IMPORTANT**: Use `skill("stage-conversion")` in the foreground — **NEVER** spawn it as a background agent (`run_in_background=true`).
   - **IMPORTANT**: Immediately after the skill loads, send the context so it works on the same branch:
      ```
       The following context was configured by the spark-migration orchestrator:
       - SMA output directory: <output>
       - Git branch: sma/migration-process (already checked out by ewi-fixer via sma_api.git_ensure_branch)
       - Target files directory: <output>/Output/
       - Stage prefix: <config.run_stage_conversion.stage_name> (use as the --prefix value)

       Skip Step 6 (git check) — the repository and branch are already set up via sma_api git functions.
       Work directly on the sma/migration-process branch.
       Use the provided stage prefix instead of asking the user in Step 3.
      ```

### Step 13: Determine DVP Orchestrator

After the Stage Conversion completes (or is skipped), read `<config.run_dvp_orchestrator>` from the saved configuration (default: `yes`). Do NOT ask the user — the value is already saved in the project config.

**If `<config.run_dvp_orchestrator>` is `no`**: Skip DVP initialization entirely. Inform the user: "Skipping DVP initialization (configured as disabled). You can run it later using the `dvp-orchestrator` skill." **Go to Step 14.**

**If `<config.run_dvp_orchestrator>` is `yes`** (default):
   - Inform the user: "Initializing DVP workspace..."
   - Invoke the `dvp-orchestrator` skill using the Skill tool:
     ```
     skill("dvp-orchestrator")
     ```
   - **IMPORTANT**: Use `skill("dvp-orchestrator")` in the foreground — **NEVER** spawn it as a background agent (`run_in_background=true`).
   - **IMPORTANT**: Immediately after the skill loads, send the pre-configured context so the DVP Orchestrator skips its Step 1 path detection and Step 4 flavor question:
      ```
       The following context was configured by the spark-migration orchestrator:
      - SMA input directory (PySpark source): <input>
      - SMA output directory: <output>
        (This is the resolved path from Step 6.5 or Step 4.1 — it points to the Conversion-* folder for v1 format)
      - Conversion type: <config.conversion_type> (scos or snowpark_api)

      Skip Step 1 (Detect SMA Paths) — the paths are already known.
      Use <input> as the source code path and <output> as the SMA output path.
      Skip the flavor question in Step 4 — use <config.conversion_type> to determine the migrated folder:
        - "scos" → dvp/02-migrated_scos/
        - "snowpark_api" → dvp/02-migrated/
      Proceed directly to Step 2 (Validate SMA Structure).
       Execute ALL steps through Step 13 (dvp-test-setup-generator) of dvp-orchestrator. Do NOT stop early.
      ```

### Step 14: Open Dashboard and Show Final Summary

After all previous steps complete (whether DVP was run or skipped), re-open the SMA Dashboard in the browser so the user sees the final state with all data (EWI statuses, test registrations, etc.).

```bash
cd "<output>/sma-dashboard" && python3 start_server.py
```

`start_server.py` already handles everything: checks if the server is running, finds an available port, starts the server, and opens the browser. Just run it.

⛔ **AFTER opening the dashboard, you MUST display the Final Summary using the EXACT template from the "Final Summary" section at the end of this document.** The summary MUST have:
- A `#` column with numbering: 1, 1.1, 1.2, 2, 2.1, 3, etc.
- Skill names in the Step column (e.g., `spark-migration (SMA CLI)`, `sma-dashboard-generator`, `dvp-ewi-fixer`, `stage-conversion`, `dvp-orchestrator`)
- **Exactly 7 top-level steps**: 1 (conversion), 2 (sma-dashboard-generator), 3 (snowflake-notebook-migration), 4 (dvp-ewi-fixer), 5 (stage-conversion), 6 (dvp-orchestrator), 7 (Open Dashboard)
- **DVP sub-skills (ASG, entrypoints, code adapter, etc.) are 6.1–6.8 under step 6** — NOT separate top-level steps
- Sections: Output location, Git branches, Duration, **Dashboard** (with `cd ... && python3 start_server.py`), Next steps
- The `═══` title bar with project name

**Do NOT create your own summary format. Scroll down to the "Final Summary" section and copy the template exactly.**

## Database Access Module

This skill includes a shared Python module that provides programmatic access to `sma_storage.sqlite3`. It is used by `sma-dashboard-generator` and `dvp-ewi-fixer` for all database operations.

**Module location:** `<skill_directory>/scripts/sma_api.py`

All 38 functions receive `workload_path: str` as their first parameter (the SMA output directory containing `sma_storage.sqlite3`).

### Available Functions

All functions receive `workload_path: str` as first parameter.

#### Initialization
| Function | Description |
|----------|-------------|
| `initialize_database` | Create/load `sma_storage.sqlite3`, import Issues.csv, create tables |
| `create_artifact_dependency_tables` | Import ArtifactDependencyInventory.csv and build dependency graph |
| `create_input_files_table` | Import InputFilesInventory.csv |

#### Read — EWI
| Function | Description |
|----------|-------------|
| `get_migration_summary` | High-level readiness summary (files, EWIs, blockers, readiness) |
| `list_ewis` | List EWIs with optional category/status filters |
| `get_blockers` | List critical blocker EWIs that prevent migration |
| `get_pending_ewi_codes` | Distinct pending EWI codes with descriptions |
| `get_ewis_by_code` | EWIs for a specific code, optionally filtered by status |
| `get_ewis_by_file` | EWIs for a specific file, optionally filtered by status |
| `get_summary_stats` | Status counts across all EWIs |
| `get_ewi_code_stats` | Per-code statistics |

#### Read — Files
| Function | Description |
|----------|-------------|
| `list_files` | List files with their EWI summary |
| `get_file_details` | Detailed EWI info for a specific file |
| `get_ewi_descriptions` | All unique EWI code → description mappings |

#### Read — Dependencies
| Function | Description |
|----------|-------------|
| `get_dependency_summary` | Dependency islands overview |
| `get_file_dependencies` | Dependencies for a specific file |
| `get_dependency_inventory` | Full artifact dependency inventory |
| `get_dependency_graph` | Dependency graph edges |

#### Write — Status
| Function | Description |
|----------|-------------|
| `update_ewi_status` | Update status/notes for an EWI code (cascading to all rows) |
| `update_file_status` | Update status for all EWIs in a file (cascading) |
| `update_line_status` | Update status for a specific line in a file |
| `bulk_update_ewi_status` | Update status for multiple EWI codes at once |
| `update_ewi_notes` | Update only notes for an EWI code |
| `update_ewi_status_single` | Update status for a single EWI row (code + file_id + line) |
| `update_dependency_status` | Update status for a dependency edge |
| `update_file_validation` | Mark a file as validated |
| `update_recommended_actions` | Set recommended actions for a file |

#### EWI Fixer
| Function | Description |
|----------|-------------|
| `generate_fix_id` | Generate a new fix session ID |
| `insert_fix_result` | Record a single fix attempt |
| `batch_insert_fix_results` | Batch insert multiple fix results |
| `get_fix_results` | Get fix results for a session |
| `get_fix_results_stats` | Get success/failed counts for a session |
| `insert_summary_start` | Create summary record with start time |
| `update_summary_end` | Complete summary record with final results |
| `get_fix_summary` | Get summary record for a session |

#### Reset
| Function | Description |
|----------|-------------|
| `reset_not_resolved_to_pending` | Reset `not_auto_resolved` EWIs back to `pending` |
| `reset_all_to_pending` | Reset ALL EWIs to `pending` |

#### Overview
| Function | Description |
|----------|-------------|
| `save_overview_stats` | Persist overview statistics to the database |

## Error Handling

### SMA Fails to Start

If SMA doesn't start or hangs:
- Verify the binary has execute permissions: `chmod +x "<sma_cli>"`
- Check if another SMA process is running
- Try running with elevated permissions if needed

### No Output Generated

If `<output>/Output/` is empty after SMA completes:
- Check SMA logs in `<output>/Logs/` (if exists)
- Verify input path contains valid PySpark code
- Look for errors in the SMA console output

### Missing Issues.csv

If `Reports/Issues.csv` doesn't exist:
- The conversion may have failed silently
- Check other files in `<output>/Reports/` for clues
- Re-run SMA and capture full output

## Outputs

| Output | Location | Description |
|--------|----------|-------------|
| Converted code | `<output>/Output/` | Snowpark Python files |
| Issues report | `<output>/Reports/Issues.csv` | EWI tracking data |
| Inventory | `<output>/Reports/Inventory.csv` | Object inventory |
| I/O Inventory | `<output>/Reports/IOInventory.csv` | Input/Output mappings |
| Dashboard | `<output>/sma-dashboard/` | Generated by sma-dashboard-generator |
| DVP Workspace | `<output>/dvp/` | Validation pipeline workspace (created by dvp-orchestrator) |

## Example Workflows

### Example 1: Snowpark API Conversion (migrate)

```
Agent: Welcome to Snowpark Migrator!
       Let me collect your project information first.

       1. Original Source Code Path?

User: /Users/me/projects/spark-etl

Agent: 2. Customer Email?

User: user@company.com

Agent: 3. Customer Company?

User: Company Inc

Agent: 4. Project Name?

User: MyProject

Agent: [Step 3: config.migration_status = "migrate" → Step 5]
       Where should the converted code be saved? (Output path)

User: /Users/me/projects/spark-etl-converted

Agent: Which conversion tool would you like to use?
       (c) Snowpark API — Convert using the SMA CLI
       (d) Snowpark Connect — Convert using snowpark-connect

User: [selects (c) Snowpark API]

Agent: Enable Jupyter Conversion? (Y/N)

User: Y

Agent: SQL Flavor? (SparkSql / HiveSql / Databricks)

User: HiveSql

Agent: Generate Checkpoints? (Y/N)

User: Y

Agent: Starting SMA conversion in background mode...
       SMA Conversion Complete!
       Resolved output path: /Users/me/projects/spark-etl-converted/Conversion-03-20-2026T10 30/
       Initializing git repository...
       [git init → commit → main branch → sma/migration-process branch]
       Verified SMA output structure.
       Now generating the EWI dashboard...
       [invokes sma-dashboard-generator]

       [Step 10: config.run_notebook_migration = "yes" → scanning for notebooks]
       Found N notebook(s). Running Notebook Migration...
       [invokes snowflake-notebook-migration with orchestrator context]

       Would you like to run the EWI Fixer?

User: [selects "Yes, run EWI Fixer"]

Agent: How should EWI comments be handled after fixing?

User: [selects "Mark"]

Agent: Which EWIs should be processed?

User: [selects "Only pending"]

Agent: Running the EWI Fixer with:
         Comment handling: Mark
         Scope: Only pending
       [invokes dvp-ewi-fixer with pre-configured options]

       Would you like to replace embedded file paths with Snowflake stage references?

User: [selects "Yes, run Stage Conversion"]

Agent: Running Stage Conversion...
       [invokes stage-conversion on sma/migration-process branch]

       [Step 13: config.run_dvp_orchestrator = "yes" → running DVP]
       Initializing DVP workspace...
       [invokes dvp-orchestrator with <input> and <output> paths]
```

### Example 2: Snowpark Connect Conversion (migrate)

```
Agent: Welcome to Snowpark Migrator!
       Let me collect your project information first.

       1. Original Source Code Path?

User: /Users/me/projects/spark-etl

Agent: 2. Customer Email?

User: user@company.com

Agent: 3. Customer Company?

User: Company Inc

Agent: 4. Project Name?

User: MyProject

Agent: [Step 3: config.migration_status = "migrate" → Step 5]
       Where should the converted code be saved? (Output path)

User: /Users/me/projects/spark-etl-scos

Agent: Which conversion tool would you like to use?
       (c) Snowpark API — Convert using the SMA CLI
       (d) Snowpark Connect — Convert using snowpark-connect

User: [selects (d) Snowpark Connect]

Agent: Searching for snowpark-connect skill... Found!
       Invoking Snowpark Connect conversion...
       [invokes snowpark-connect with collected inputs]
       Conversion complete!
       Initializing git repository...
       [git init → commit → main branch → sma/migration-process branch]
       Verified output structure.
       Proceeding to generate the dashboard...
       [invokes sma-dashboard-generator]

       [Step 10: config.run_notebook_migration = "yes" → scanning for notebooks]
       Found N notebook(s). Running Notebook Migration...
       [invokes snowflake-notebook-migration with orchestrator context]
```

### Example 3: Already Migrated (already_migrated)

```
Agent: Welcome to Snowpark Migrator!
       Let me collect your project information first.

       1. Original Source Code Path?

User: /Users/me/projects/spark-etl

Agent: 2. Customer Email?

User: user@company.com

Agent: 3. Customer Company?

User: Company Inc

Agent: 4. Project Name?

User: MyProject

Agent: [Step 3: config.migration_status = "already_migrated" → Step 4]
       Please provide the path to your SMA/SCOS output folder.

User: /Users/me/projects/spark-etl-converted

Agent: Validating output structure...
       Detected SMA v1 format. Resolved path:
         /Users/me/projects/spark-etl-converted/Conversion-03-19-2026T15 11/
       SMA output folder validated:
         Output:  .../Conversion-03-19-2026T15 11/Output/
         Reports: .../Conversion-03-19-2026T15 11/Reports/

       Initializing git repository...
       [git init → commit → main branch → sma/migration-process branch]
       Proceeding to generate the dashboard...
       [invokes sma-dashboard-generator]

       [Step 10: config.run_notebook_migration = "yes" → scanning for notebooks]
       Found N notebook(s). Running Notebook Migration...
       [invokes snowflake-notebook-migration with orchestrator context]

       Would you like to run the EWI Fixer?

User: [selects "Yes, run EWI Fixer"]

Agent: How should EWI comments be handled after fixing?

User: [selects "Remove"]

Agent: Which EWIs should be processed?

User: [selects "All (reset)"]

Agent: Running the EWI Fixer with:
         Comment handling: Remove
         Scope: All (reset)
       [invokes dvp-ewi-fixer with pre-configured options]

       Would you like to replace embedded file paths with Snowflake stage references?

User: [selects "No, skip for now"]

Agent: Skipping Stage Conversion. You can run it later using the stage-conversion skill.

       [Step 13: config.run_dvp_orchestrator = "no" → skipping]
       Skipping DVP initialization. You can run it later using the dvp-orchestrator skill.
```

## Final Summary

⛔ **CRITICAL — COPY THIS TEMPLATE EXACTLY**: After all steps complete, you MUST present the final summary using the EXACT format below. Do NOT improvise your own table format. Do NOT omit the `#` column. Do NOT rename sections. Do NOT skip the Dashboard section. Copy this template verbatim and replace the placeholders with real values.

**At the very start of Step 1**, record the current time as `<start_time>` (e.g., `datetime.now()`). At the end, compute `<duration>` = current time − `<start_time>`.

The template has two variants for Step 1 depending on conversion type. Use ONLY the one that matches the actual flow (SCOS or Snowpark API). Delete the other variant and the `— or —` separator.

```
═══════════════════════════════════════════════════════════════════════════
  Snowflake Migration Complete — <project_name>
═══════════════════════════════════════════════════════════════════════════

┌──────┬────────────────────────────────────────────────┬──────────┬──────────────────────────────────────────────┐
│  #   │ Step                                           │ Status   │ Details                                      │
├──────┼────────────────────────────────────────────────┼──────────┼──────────────────────────────────────────────┤
│  1   │ migrate-pyspark-to-snowpark-connect            │ Done     │ N files converted (Snowpark Connect)         │
│  1.1 │   Setup RAG Resources                          │ Done     │ RAG initialized (or fallback to remote)      │
│  1.2 │   Analyze Workload                             │ Done     │ N issues found                               │
│  1.3 │   Create Conversion Folder                     │ Done     │ Created at <output>                          │
│  1.4 │   Apply Fixes from Analysis                    │ Done     │ N fixes applied                              │
│  1.5 │   Update Imports and Session                   │ Done     │ N files updated                              │
│  1.6 │   Add Migration Header                         │ Done     │ N files with header                          │
│  1.7 │   Verify Migration                             │ Done     │ All checks passed                            │
│  1.8 │   Generate Dashboard Reports                   │ Done     │ Issues.csv, Inventory.csv generated          │
│      │  — or (if Snowpark API) —                      │          │                                              │
│  1   │ spark-migration (SMA CLI)                  │ Done     │ N files converted (Snowpark API)             │
│  1.1 │   Run SMA CLI                                  │ Done     │ sma convert completed                        │
│  1.2 │   Jupyter Conversion                           │ Done     │ N notebooks converted                        │
│  2   │ sma-dashboard-generator                        │ Done     │ Generated at sma-dashboard/                  │
│  2.1 │   Verify Requirements                          │ Done     │ CSV files validated                          │
│  2.2 │   Run SMA Dashboard Manager                    │ Done     │ SQLite DB created, dashboard built           │
│  2.3 │   Report Results                               │ Done     │ N EWIs, N files, N dependencies              │
│  3   │ snowflake-notebook-migration                   │ Done     │ N notebooks converted to Snowflake format    │
│  3.1 │   Scan for Notebooks                           │ Done     │ N notebooks found in Output/                 │
│  3.2 │   Convert Notebooks                            │ Done     │ N notebooks converted in-place               │
│  3.3 │   Git Commit                                   │ Done     │ Changes committed on sma/migration-process   │
│  4   │ dvp-ewi-fixer                                  │ Done     │ Fixed N/M EWIs                               │
│  4.1 │   Load EWI Context                             │ Done     │ N EWIs loaded from database                  │
│  4.2 │   Apply Fixes                                  │ Done     │ N files processed                            │
│  4.3 │   Update Database                              │ Done     │ Results saved to SQLite                      │
│  5   │ stage-conversion                               │ Skipped  │ User opted out                               │
│  5.1 │   Scan for Embedded Paths                      │ Done     │ N paths found in M files                     │
│  5.2 │   Preview Changes                              │ Done     │ Dry run completed                            │
│  5.3 │   Apply Replacements                           │ Done     │ N paths replaced with @stage_name            │
│  6   │ dvp-orchestrator                               │ Done     │ Validation pipeline completed                │
│  6.1 │   Create DVP Workspace                         │ Done     │ dvp/ structure created                       │
│  6.2 │   dvp-notebook-to-script                       │ Skipped  │ No notebooks found                           │
│  6.3 │   dvp-asg-generation                           │ Done     │ Generated ASG from N source files            │
│  6.4 │   dvp-entrypoint-identifier                    │ Done     │ N entrypoints detected                       │
│  6.5 │   dvp-code-adapter                             │ Done     │ N files adapted for testing                  │
│  6.6 │   dvp-io-schema-identifier                     │ Done     │ N inputs, M outputs mapped                   │
│  6.7 │   dvp-synthetic-data-generator                 │ Done     │ Test data generated for N inputs             │
│  6.8 │   dvp-test-setup-generator                     │ Done     │ N test suites registered                     │
│  7   │ Open Dashboard                                 │ Done     │ Dashboard opened in browser                  │
└──────┴────────────────────────────────────────────────┴──────────┴──────────────────────────────────────────────┘

Output location: <output>/

Git branches:
  • main — original conversion output (unmodified)
  • sma/migration-process — all fixes applied

Duration: Start: <start_time> | End: <end_time> | Duration: <duration>

Dashboard:
  To open the SMA Dashboard and review EWI issues and migration status:
    cd "<output>/sma-dashboard" && python3 start_server.py
  This starts a local server and opens the dashboard in your default browser.
  If the server is already running, open http://localhost:<port> in your browser.

Next steps:
  • Run tests with: cd "<output>/dvp/03-tests" && pytest source/ -v
  • Fix remaining EWIs by invoking the dvp-ewi-fixer skill
  • Review and validate converted code in <output>/Output/
```

⛔ **CHECKLIST — verify before displaying the summary:**
1. Does the table have a `#` column with numbers like 1, 1.1, 1.2, 2, 2.1, 3, etc.?
2. Do main rows (1, 2, 3, 4, 5, 6, 7) use the **actual skill name** (e.g., `dvp-ewi-fixer`, not "EWI Fixer")?
3. Is there a **Dashboard** section with the `cd ... && python3 start_server.py` command?
4. Are all `N`, `M`, `<output>`, `<project_name>`, `<duration>`, `<port>` replaced with real values?
5. Are skipped skills shown with only the parent row (no sub-steps)?
6. ⛔ **Are DVP sub-skills numbered 6.1–6.8 under `dvp-orchestrator` (step 6)?** They must NOT be separate top-level steps (7, 8, 9, 10...). The ONLY top-level steps are: 1 (conversion), 2 (sma-dashboard-generator), 3 (snowflake-notebook-migration), 4 (dvp-ewi-fixer), 5 (stage-conversion), 6 (dvp-orchestrator), 7 (Open Dashboard). Everything inside DVP is 6.x.

**Rules:**
- Replace `N`, `M` with actual counts from each sub-skill's output
- Replace `<project_name>`, `<output>`, `<start_time>`, `<end_time>`, `<duration>`, `<port>` with real values
- `<start_time>` is recorded at Step 1 start, `<end_time>` is current time, `<duration>` is the difference (e.g., "~13 minutes")
- Status is `Done`, `Skipped`, or `Failed`
- **Only include the rows for the path that was actually executed:**
  - SCOS flow: show `migrate-pyspark-to-snowpark-connect` with sub-steps 1.1–1.8. Omit the SMA CLI rows.
  - Snowpark API flow: show `spark-migration (SMA CLI)` with sub-steps 1.1–1.2. Omit the SCOS rows.
  - If a skill was skipped (e.g., stage-conversion): show only the parent row with `Skipped`, omit sub-steps.
  - If a sub-skill failed: show `Failed` with brief error message.
- Sub-step rows (N.1, N.2, ...) only appear if the parent step was executed and completed
- The `#` column uses user-facing numbering: main steps are skills (1, 2, 3, ...), sub-steps are internal skill steps (1.1, 1.2, 6.3, ...)
- The git branches section uses `sma_api.git_verify_branches()` to confirm both branches exist
- Duration is wall-clock time from Step 1 start to final summary display
