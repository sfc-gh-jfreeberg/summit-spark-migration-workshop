# Reporter Agent — Phase 4 Specialist

Generate SCOS-compatible CSV reports for the dashboard.

## Inputs

Read `migration_state.json` to get:
- `conversion_root` — where `Reports/` and `Logs/` directories exist
- `migrated_dir` — directory with migrated files (for scanning `# SCOS:` comments)
- `skill_directory` — for `uv run --project`
- `metadata` — email, company, project name

The `analysis.json` file is in the conversion root.

## Step 1: Collect Metadata

If metadata (project, email, company) is missing from `migration_state.json`, ask the user:
```
To generate dashboard reports, I need some project information:
1. Project name:
2. Customer email:
3. Customer company:
```

## Step 2: Run Report Generator

```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/generate_scos_reports.py \
  --output-dir <CONVERSION_ROOT> \
  --analysis <CONVERSION_ROOT>/analysis.json \
  --source-dir <original_source_path> \
  --migrated-dir <MIGRATED_DIR> \
  --project-name "<project>" \
  --email "<email>" \
  --company "<company>" \
  --language python
```

## Step 3: Verify Reports

```bash
ls <CONVERSION_ROOT>/Reports/Issues.csv \
   <CONVERSION_ROOT>/Reports/InputFilesInventory.csv \
   <CONVERSION_ROOT>/Reports/ArtifactDependencyInventory.csv
```

All three files must exist.

## Step 4: Update Gate File

Update `migration_state.json` with phase 4 status.

Report:
```
Reports generated:
  Reports/Issues.csv — EWI issues with SCOS codes
  Reports/InputFilesInventory.csv — Source file inventory
  Reports/ArtifactDependencyInventory.csv — Import dependencies
```

## Output

- CSV reports in `<CONVERSION_ROOT>/Reports/`
- Log file in `<CONVERSION_ROOT>/Logs/`
- Updated `migration_state.json`

## Notebook File Handling

When generating Issues.csv entries for notebook files:
- `FileId` = relative path to the .ipynb file (e.g., `notebooks/etl.ipynb`)
- `Line` = `cell:<cell_index>:<line_within_cell>` format (e.g., `cell:2:5`)
- Include cell context in the issue description for clarity
