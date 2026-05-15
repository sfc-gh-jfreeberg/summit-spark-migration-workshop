# Transformation Rules Registry

Single source of truth for incompatible-pattern rules applied by the `snowflake-notebook-migration` skill. Consumed by both standalone (`references/standalone-mode.md`) and orchestrator (`references/orchestrator-mode.md`) modes.

## Contract

To add or change a rule, append or edit a row in the table below. Do not touch `SKILL.md` or the mode reference files — they are pure orchestrators.

Columns:

- `pattern` — concrete token, call, or structural match (e.g. `%md`, `dbutils.widgets.text(...)`, `display(df)` where `df` is a PySpark DataFrame).
- `category` — one of `magic`, `dbutils`, `display`, `sql_ref`, `unsupported`, `ignored`.
- `action` — what the skill does when it matches.
- `preserves` — what is kept as-is (variable names, imports, comments, surrounding code).
- `notes` — caveats, anti-patterns, links to examples.

## Core principle (reminder)

This is a migration, not a rewrite. If a cell has no matching rule, copy it as-is. If a cell mixes compatible and incompatible lines, apply rules only to the incompatible lines and leave everything else untouched. If the entire cell is covered by an unsupported rule, convert the cell to a markdown migration note that preserves the original code in a fenced block (see [Migration Note Format](#migration-note-format)).

## Rules

| # | pattern | category | action | preserves | notes |
|---|---------|----------|--------|-----------|-------|
| 1 | `%md` (first line of cell) | magic | Convert cell to `cell_type: "markdown"`. Remove the `%md` line. | Remaining cell content unchanged. | Only the directive is stripped. |
| 2 | `%sql` (first line of cell) | magic | Convert cell to `cell_type: "sql"`, `language: "sql"`. Remove the `%sql` line. Add `resultVariableName` metadata with a meaningful name. | Remaining SQL body unchanged. | See rule 8 for `_sqldf` handling in downstream cells. |
| 3 | `%scala` (first line of cell) | magic | Convert cell to markdown migration note with the original code in a fenced block. | Original code preserved inside the fenced block. | Not supported in Snowflake Workspace notebooks. |
| 4 | `%r` (first line of cell) | magic | Convert cell to markdown migration note with the original code in a fenced block. | Original code preserved inside the fenced block. | Same treatment as `%scala`. |
| 5 | `%sh` (first line of cell) | magic | Convert cell to Python (`cell_type: "code"`). Remove `%sh`. Prefix each command line with `!`. | Command text, order, and comments preserved. | Shell availability in Snowflake Workspace is limited — owner may need to rewrite for Python equivalents. |
| 6 | `%fs` (first line of cell) | magic | Convert cell to markdown migration note. Suggest Snowflake stage equivalents in the note. | Original commands preserved inside the fenced block. | DBFS does not exist in Snowflake. |
| 7 | `%run ./path` | magic | Keep as-is; rewrite the path to target the post-conversion filename following the File Naming Convention in `SKILL.md`. `config.py` → `%run ./config.py.ipynb`. Existing `.ipynb` targets are unchanged. | Cell as a whole; only the path is edited. | Do NOT inline the referenced notebook's code. If the target file does not exist at the expected path, leave the path and add an inline migration note. |
| 8 | `_sqldf` reference in a Python cell | sql_ref | Replace `_sqldf` with the `resultVariableName` assigned to the preceding SQL cell (see rule 2). | Surrounding Python code. | The result is already a pandas DataFrame — remove any `.toPandas()` call immediately following the `_sqldf` reference. |
| 9 | `{{sql_var_name}}` (or absence of it) in a SQL cell that references another SQL cell's result | sql_ref | Use Jinja `{{variable_name}}` to reference the upstream SQL cell's `resultVariableName`. | Rest of the SQL unchanged. | Only applies to SQL-to-SQL cross-references. |
| 10 | `dbutils.widgets.text(...)`, `dbutils.widgets.dropdown(...)`, `dbutils.widgets.get(...)`, `dbutils.widgets.multiselect(...)`, `dbutils.widgets.combobox(...)`, `dbutils.widgets.getArgument(...)`, `dbutils.widgets.removeAll()`, `dbutils.widgets.*` | dbutils | Replace with `sys.argv` parameters with hardcoded fallback defaults. Use the notebook's own filename to detect whether arguments were passed: `_NOTEBOOK_NAME = "<notebook_filename>.ipynb"; _has_args = os.path.basename(sys.argv[0]) == _NOTEBOOK_NAME; param = sys.argv[1] if _has_args and len(sys.argv) > 1 else "default_value"`. Replace each `dbutils.widgets.get("name")` / `dbutils.widgets.getArgument("name")` with the corresponding variable. Remove all `dbutils.widgets.text/dropdown/multiselect/combobox/removeAll` setup calls. | Variable names referenced by downstream cells. | Do NOT convert the whole cell to markdown — downstream cells likely depend on the variable. `ipywidgets` support for interactive UI controls is planned for Workspace notebooks; once available, widgets can be converted to `ipywidgets` equivalents. |
| 11 | `dbutils.notebook.run(...)` | dbutils | Convert to markdown migration note suggesting `%run` (for variable sharing) or a Snowflake Task DAG with `EXECUTE NOTEBOOK` (for orchestration). | Original call inside the fenced block. | `%run` only covers the variable-sharing use case, not the return-value use case. Flag both options in the note. |
| 12 | `dbutils.notebook.exit(...)` | dbutils | Convert to `%notebook_exit`. Do NOT replace with `raise SystemExit()`. | Surrounding code. | `%notebook_exit` is the Snowflake Workspace equivalent. |
| 13 | `dbutils.secrets.get(scope=..., key=...)` | dbutils | Comment out the call in place. Assign a placeholder value (`None` or `"TODO"`) so downstream cells that reference the variable continue to resolve. Add an inline comment noting the owner must migrate to Snowflake Secrets. | All other lines in the cell, including the variable name bound to the secret. | Do NOT convert the entire cell to markdown — downstream cells depend on the variable. |
| 14 | `dbutils.fs.ls(...)`, `dbutils.fs.cp(...)`, `dbutils.fs.rm(...)`, `dbutils.fs.*` | dbutils | Convert the cell (or just the incompatible lines) to a markdown migration note. Suggest Snowflake stage operations (`LIST @stage`, `PUT`, `GET`, `REMOVE @stage`). | Original commands inside the fenced block. | DBFS does not exist in Snowflake. |
| 15 | `display(df)` where `df` is a PySpark or Snowpark DataFrame | display | Replace with `df.show()`. | Variable name and surrounding code. | Do NOT leave a bare `df` — lazy DataFrames print only the schema string, not the data. |
| 16 | `display(pdf)` where `pdf` is a pandas DataFrame | display | Replace with a bare `pdf` as the last expression of the cell. | Variable name and surrounding code. | Snowflake Workspace notebooks render pandas DataFrames when they are the final expression. |
| 17 | `display(plt.gcf())` and similar matplotlib figure calls | display | Replace with `plt.show()`. | Surrounding matplotlib code. | Applies to any pyplot figure passed to `display`. |
| 18 | `from pyspark.rdd import ...`, `rdd.*`, `pyspark.RDD` usage | unsupported | Convert to markdown migration note; preserve original code in a fenced block. | Original code inside the fenced block. | RDD API is not provided by Snowpark Connect. |
| 19 | `pyspark.ml.*` usage | unsupported | Convert to markdown migration note; preserve original code in a fenced block. | Original code inside the fenced block. | ML library is not available. |
| 20 | `pyspark.streaming.*` usage | unsupported | Convert to markdown migration note; preserve original code in a fenced block. | Original code inside the fenced block. | Structured streaming is not available in Snowpark Connect. |
| 21 | `DataFrameWriter.jdbc(...)`, `DataFrameReader.orc(...)`, `DataFrameWriter.orc(...)` | unsupported | Flag each call with an inline migration note; preserve the call. | All surrounding code. | Do not silently remove — the owner must decide on the destination. |
| 22 | Iterator type in UDFs (`Iterator[...] -> Iterator[...]`) | unsupported | Flag with an inline migration note; preserve the UDF. | UDF body. | Iterator-typed UDFs are not supported. |
| 23 | Reads/writes against external databases (Redshift, RDS/MySQL, Postgres, etc.) | unsupported | Flag with an inline migration note; preserve the call. | All surrounding code. | The owner must migrate these to Snowflake connectors or stages. |
| 24 | Cloud-storage writes through custom libraries (e.g. S3 utility wrappers) | unsupported | Flag with an inline migration note; preserve the call. | All surrounding code. | Suggest Snowflake stages (`PUT`, `COPY INTO`) in the note. |
| 25 | Imports from custom utility libraries that are not in the migration scope | unsupported | Flag with an inline migration note; preserve the import and its usages. | All surrounding code. | Do not attempt to inline or rewrite custom utilities. |
| 26 | Hardcoded credentials or secret-reference constants | unsupported | Flag with an inline migration note; preserve the constant. | All surrounding code. | Recommend migration to Snowflake Secrets in the note. Do not scrub the value silently — the owner must decide. |
| 27 | `DataFrame.hint(...)` | ignored | No action. Leave the call untouched. | Entire call. | Snowpark Connect silently ignores this call; no behavioral effect. |
| 28 | `DataFrame.repartition(...)` | ignored | No action. Leave the call untouched. | Entire call. | Snowpark Connect silently ignores this call; no behavioral effect. |
| 29 | `%md-sandbox` (first line of cell) | magic | Same as rule 1. Convert cell to `cell_type: "markdown"`. Remove the `%md-sandbox` line. | Remaining cell content unchanged. | Databricks sandbox variant of `%md`; treated identically. |
| 30 | `%python` / `%py` (first line of cell) | magic | Remove the magic line. Keep the cell as `cell_type: "code"`. | All code in the cell unchanged. | Cell is already Python — the magic is redundant in Snowflake Workspace notebooks. |
| 31 | `%pip` (first line of cell) | magic | Keep as-is. No transformation needed. | Entire cell unchanged. | `%pip` is supported in Snowflake Workspace notebooks. |
| 32 | `%time` (first line of cell) | magic | Keep as-is. No transformation needed. | Entire cell unchanged. | IPython magic; supported in Snowflake Workspace notebooks. |
| 33 | `%load_ext` (first line of cell) | magic | Keep as-is. No transformation needed. | Entire cell unchanged. | IPython magic; supported in Snowflake Workspace notebooks. |
| 34 | `%environment` (first line of cell) | magic | Convert to `%env`. | Remaining cell content unchanged. | `%environment` is Databricks-specific; `%env` is the IPython equivalent supported in Workspace notebooks. |
| 35 | `dbutils.library.installPyPI(...)` | dbutils | Convert to `!pip install <package>`. Extract the package name from the call arguments. | Surrounding code in the cell. | Databricks library utility; `!pip install` is the Workspace equivalent. |
| 36 | `dbutils.library.restartPython()` | dbutils | Remove the call. | All other lines in the cell. | Not needed in Snowflake Workspace notebooks — kernel restarts are handled differently. |
| 37 | `dbutils.jobs.taskValues`, `dbutils.jobs.TaskValuesUtils.get(...)`, `dbutils.jobs.TaskValuesUtils.set(...)` | dbutils | Convert to markdown migration note; preserve original code in a fenced block. | Original code inside the fenced block. | Databricks job orchestration APIs with no direct Snowflake equivalent. Suggest Snowflake Task DAG for orchestration in the note. |
| 38 | `dbutils.secrets.list(...)`, `dbutils.secrets.listScopes(...)`, `dbutils.secrets.getBytes(...)` | dbutils | Flag with an inline migration note; preserve the call. | All surrounding code. | Non-`get` secrets APIs. No Snowflake equivalent yet. See rule 13 for `dbutils.secrets.get`. |
| 39 | `dbutils.fs.mount(...)`, `dbutils.fs.unmount(...)`, `dbutils.fs.mounts()`, `dbutils.fs.refreshMounts()` | dbutils | Convert to markdown migration note; preserve original code in a fenced block. | Original code inside the fenced block. | DBFS mounts do not exist in Snowflake. Suggest external volumes or stages in the note. |

## Migration Note Format

When a rule's action is "convert to markdown migration note", use this exact template for the new markdown cell:

````markdown
> **Migration Note**: [Brief description of the issue]
> [Why it's incompatible with Snowflake]
>
> **Owner action required**: [What the owner needs to decide or do]

Original code:
```[language]
[original code preserved exactly]
```
````
