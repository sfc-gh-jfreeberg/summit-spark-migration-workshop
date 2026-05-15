---
name: snowpark-connect
description: |
  Snowpark Connect (SCOS) skills for migrating and validating PySpark and Spark Scala workloads on Snowflake.
  Generates SMA-compatible reports (Issues.csv, InputFilesInventory.csv, ArtifactDependencyInventory.csv)
  using EWI codes (SPRKCNTPY* for Python, SPRKCNTSCL* for Scala) for use with dvp-sma-dashboard-generator.
  Use when: migrating PySpark or Spark Scala to Snowpark Connect, validating SCOS migrations,
  analyzing Spark compatibility, or working with Snowpark Connect for Spark.
  Triggers: snowpark connect, scos, pyspark migration, spark connect, scala spark migration,
  validate migration, pyspark compatibility, scala compatibility.
---

# Snowpark Connect

Skills for working with Snowpark Connect for Spark (SCOS) on Snowflake — supports Python and Scala workloads.

## When to Use

- User wants to migrate PySpark, Databricks, or Spark Scala code to Snowflake
- User asks about SCOS or Snowpark Connect compatibility
- User wants to validate a completed SCOS migration
- User mentions "spark connect", "scos", or "snowpark connect"

## Intent Detection

Determine the **language** and **action** from the user request, then route to the correct sub-skill:

```
Start
  ↓
Analyze User Request
  ↓
Detect Language
  ├─→ Python (.py, PySpark, Databricks, pyspark)
  │     ├─→ Migration intent → Load migrate-pyspark-to-snowpark-connect/SKILL.md
  │     └─→ Validation intent → Load validate-pyspark-to-snowpark-connect/SKILL.md
  │
  ├─→ Scala (.scala, Spark Scala, build.sbt)
  │     ├─→ Migration intent → Load migrate-spark-scala-to-snowpark-connect/SKILL.md
  │     └─→ Validation intent → Load validate-spark-scala-to-snowpark-connect/SKILL.md
  │
  └─→ Ambiguous → Ask the user which language the workload uses
```

### Step 1: Detect Language

Determine the source language from:
- **Explicit mention**: "PySpark", "Python Spark", "Scala Spark"
- **File extensions**: `.py` → Python; `.scala` → Scala
- **Import patterns**: `from pyspark` / `import pyspark` → Python; `import org.apache.spark` with `.scala` → Scala
- **Build files**: `requirements.txt` / `pyproject.toml` → Python; `build.sbt` → Scala

If the language cannot be determined, ask the user:
```
I detected Spark code in your workload. Which language is it written in?
- Python (PySpark / Databricks)
- Scala (Spark Scala)
```

### Step 2: Route by Intent

**Migration intent** — keywords: migrate, convert, rewrite, update imports, move to SCOS
**Validation intent** — keywords: validate, verify, check, test, review migration

### Route: Migrate PySpark to Snowpark Connect

**If user wants to migrate Python Spark code:**
- **Load** `migrate-pyspark-to-snowpark-connect/SKILL.md`
- Follow the migration workflow
- Uses EWI codes: `SPRKCNTPY*`
- References: `references/python/`

### Route: Migrate Spark Scala to Snowpark Connect

**If user wants to migrate Scala Spark code:**
- **Load** `migrate-spark-scala-to-snowpark-connect/SKILL.md`
- Follow the migration workflow
- Uses EWI codes: `SPRKCNTSCL*`
- References: `references/scala/`

### Route: Validate a PySpark Migration

**If user wants to validate a completed Python migration:**
- **Load** `validate-pyspark-to-snowpark-connect/SKILL.md`
- Follow the validation workflow

### Route: Validate a Spark Scala Migration

**If user wants to validate a completed Scala migration:**
- **Load** `validate-spark-scala-to-snowpark-connect/SKILL.md`
- Follow the validation workflow

## Stopping Points

None — this skill routes to sub-skills. Stopping points are defined within each sub-skill.

## Output

Output is determined by the loaded sub-skill:
- **Python Migration**: Migrated `_scos` files with compatibility fixes, migration headers, and SCOS-compatible dashboard reports (`Reports/Issues.csv`, `Reports/InputFilesInventory.csv`, `Reports/ArtifactDependencyInventory.csv`) using `SPRKCNTPY*` codes
- **Scala Migration**: Migrated `_scos` files with compatibility fixes, migration headers, and SCOS-compatible dashboard reports using `SPRKCNTSCL*` codes
- **Validation**: Validation report with pass/fail status for each check
