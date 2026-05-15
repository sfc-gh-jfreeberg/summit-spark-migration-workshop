# Fixer Agent — Phase 2 Specialist

Apply code fixes for SCOS compatibility issues identified in `analysis.json` for Scala workloads.

## Inputs

Read `migration_state.json` to get:
- `manifest` — list of `.scala` files
- `migrated_dir` — directory with copied source files
- `conversion_root` — for `analysis.json` and gate file

Read `analysis.json` from the conversion root.

## Rules

Load `references/fix-rules.md` for the complete Scala-specific fix rule set. Key rules summary:

| Risk | Action |
|------|--------|
| `final_risk >= 0.7` | **Must fix** — apply fix or rewrite. If impossible, add `// SCOS: TODO` |
| `0.3 <= final_risk < 0.7` | **Should fix** — apply fix if suggested, else `// SCOS: TODO` |
| `final_risk < 0.3` | **Review** — fix if possible, else `// SCOS: <explanation>` |

**Critical exceptions (do NOT annotate):**
- No-op operations (`hint()`, `repartition()`, `coalesce()`) — leave as-is, no comment
- No-op configs (`spark.sql.shuffle.partitions`, `spark.executor.memory`, etc.) — leave as-is, no comment

**Comment prefixes (Scala uses `//` not `#`):**
- `// SCOS: <explanation>` — fix applied or reviewed
- `// SCOS: TODO - <explanation>` — requires manual review
- `// SCOS: Performance tip - <explanation>` — optimization recommendation

## Workflow

Process files **one at a time** from the manifest:

1. Read the file
2. Find all issues for this file in `analysis.json`
3. For each issue, consult `references/fix-rules.md` for the appropriate action:
   - **RDD operations**: Read `references/scala/rdd-conversion.md` for DataFrame equivalents
   - **UDF serialization**: Read `references/scala/udf-dependencies.md` for Scala-specific fixes (REPLClassDirMonitor, addArtifact, staged JARs)
   - **Wildcard file reads**: Replace with explicit file lists or add TODO
   - **`checkpoint()`**: Replace with `cache()`
   - **Map column subscript**: Replace `mapCol(col("key"))` with `element_at(mapCol, col("key"))`
   - **Catalyst imports**: Create local case class replacements (Rule 15)
   - **Hadoop/HDFS**: Remove imports, replace file ops with Snowflake stage/table (Rule 16)
   - **HWC**: Replace `hive.sql()` → `spark.sql()`, remove HWC declarations (Rule 17)
   - **Hive DDL**: Comment out with TODO (Rule 18)
   - **Cross-file consistency**: After any signature change, grep entire codebase for callers (Rule 20)
   - **Import emission**: Only emit valid Scala import lines — no trailing text/em-dashes (Rule 21)
   - **Syntax artifact cleanup**: Clean up trailing text, bare em-dashes, orphaned comments (Rule 22)
4. Apply fixes using the Edit tool
5. Update `migration_state.json` with per-file progress:
   ```json
   "2_fixes": {"files_done": ["File1.scala"], "files_remaining": ["File2.scala"]}
   ```

## Completeness Check

After processing all files:
- Every issue in `analysis.json` with `final_risk >= 0.7` has a fix or TODO
- Every issue with `final_risk >= 0.3` has a fix, comment, or TODO
- Cross-file consistency verified (Rule 20) — no calls to removed methods/parameters remain
- File count matches manifest

Report: "Fixes applied: X files processed, Y issues fixed, Z TODOs remaining"

## Output

- Modified files in `<MIGRATED>/`
- Updated `migration_state.json` with phase 2 status
