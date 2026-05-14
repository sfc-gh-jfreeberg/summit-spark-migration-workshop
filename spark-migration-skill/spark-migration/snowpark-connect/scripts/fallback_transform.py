#!/usr/bin/env python3
"""
SNOW-3383532: Deterministic fallback transformation for files not processed by LLM sub-agents.

After all sub-agent phases complete, the coordinator checks migration_state.json for any
files in the manifest that were never moved to files_done by the fixer agent.  For each
such file this script:

  1. Copies the original source to Output/ (if not already present)
  2. Injects a migration header comment
  3. Rewrites pyspark / spark imports with annotated SCOS comments
  4. Replaces SparkSession.builder with snowpark_connect.init_spark_session()
     for files detected as entry-points
  5. Appends a SPRKCNTPY0099 EWI entry to analysis.json so downstream report
     generation records the partial-migration coverage gap

Usage:
    python fallback_transform.py --state /path/to/migration_state.json

Returns exit code 0 always (failures are logged, not fatal) so the coordinator
can continue to Phase 3 regardless of fallback count.
"""

import argparse
import json
import os
import re
import shutil
import sys
from datetime import datetime

# SNOW-3383532: EWI code for "file not fully migrated by LLM agent"
FALLBACK_EWI_CODE = "SPRKCNTPY0099"
FALLBACK_EWI_DESCRIPTION = (
    "File was not fully processed by the LLM migration agent. "
    "A deterministic fallback transformation was applied: imports annotated, "
    "session init replaced where detectable. Manual review required."
)

# Ordered import rewrite rules: (pattern, annotation_comment)
IMPORT_REWRITE_RULES = [
    # pyspark top-level
    (r"^(from pyspark(?:\.\S+)?\s+import\s+.+)$",
     "# SCOS: [SPRKCNTPY0099] PySpark import — review for Spark Connect compatibility"),
    (r"^(import pyspark(?:\.\S+)?)(.*)$",
     "# SCOS: [SPRKCNTPY0099] PySpark import — review for Spark Connect compatibility"),
    # databricks
    (r"^(from databricks(?:\.\S+)?\s+import\s+.+)$",
     "# SCOS: [SPRKCNTPY0099] Databricks import — not available in Snowpark Connect"),
    (r"^(import databricks(?:\.\S+)?)(.*)$",
     "# SCOS: [SPRKCNTPY0099] Databricks import — not available in Snowpark Connect"),
    # delta
    (r"^(from delta(?:\.\S+)?\s+import\s+.+)$",
     "# SCOS: [SPRKCNTPY0099] Delta Lake import — replace with Snowflake table operations"),
    (r"^(import delta(?:\.\S+)?)(.*)$",
     "# SCOS: [SPRKCNTPY0099] Delta Lake import — replace with Snowflake table operations"),
]

# SparkSession.builder patterns → snowpark_connect replacement
SESSION_PATTERNS = [
    (
        "DatabricksSession.builder.getOrCreate()",
        "snowpark_connect.init_spark_session()",
    ),
    (
        "SparkSession.builder.master(master).appName(app_name).getOrCreate()",
        "snowpark_connect.init_spark_session()",
    ),
    (
        "SparkSession.builder.appName(app_name).getOrCreate()",
        "snowpark_connect.init_spark_session()",
    ),
    (
        "SparkSession.builder.getOrCreate()",
        "snowpark_connect.init_spark_session()",
    ),
]

MIGRATION_HEADER_MARKER = "SCOS Migration Output"


def load_state(state_path: str) -> dict:
    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state_path: str, state: dict) -> None:
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def find_unprocessed_files(state: dict) -> list[str]:
    """Return manifest entries not yet processed by the fixer agent.

    Supports two tracking styles:
    - ``pending_files`` key (private/chunked skill): list populated at Phase 2
      start; fixer removes entries as it completes each file.
    - ``2_fixes.files_done`` key (public/non-chunked skill): list of processed
      files appended by the fixer agent.

    Falls back to returning all manifest files if neither key is present
    (safe first-run default: all files need fallback).
    """
    manifest: list[str] = state.get("manifest", [])

    # Private-skill style: pending_files is authoritative when present.
    if "pending_files" in state:
        return list(state["pending_files"])

    # Public-skill style: compare manifest against 2_fixes.files_done.
    phase2 = state.get("2_fixes", {})
    files_done: list[str] = phase2.get("files_done", [])
    done_set = set(files_done)
    done_basenames = {os.path.basename(p) for p in done_set}

    return [
        entry for entry in manifest
        if entry not in done_set
        and os.path.basename(entry) not in done_basenames
    ]


def is_entry_point(content: str) -> bool:
    """Heuristic: file is an entry point if it contains SparkSession.builder."""
    return bool(re.search(r"SparkSession\s*\.\s*builder|DatabricksSession\s*\.\s*builder", content))


def add_migration_header(content: str, filename: str) -> str:
    """Prepend a SCOS migration header docstring if not already present."""
    if MIGRATION_HEADER_MARKER in content[:500]:
        return content

    today = datetime.now().strftime("%Y-%m-%d")
    header = (
        '"""\n'
        f"{MIGRATION_HEADER_MARKER}\n"
        "=====================\n"
        f"Source File: {filename}\n"
        f"Migrated on: {today}\n"
        "\n"
        "Changes Overview:\n"
        "- Deterministic fallback transformation applied (SNOW-3383532)\n"
        "- LLM agent did not fully process this file; imports annotated manually\n"
        "\n"
        "Known Limitations:\n"
        "- Manual review required — this file was not processed by the LLM fixer agent\n"
        '"""\n'
    )
    return header + content


def rewrite_imports(content: str) -> tuple[str, int]:
    """Annotate pyspark/databricks/delta imports with SCOS comments.

    Returns (modified_content, count_of_rewrites).
    """
    lines = content.splitlines(keepends=True)
    count = 0
    new_lines = []

    for line in lines:
        stripped = line.rstrip("\n")
        for pattern, annotation in IMPORT_REWRITE_RULES:
            if re.match(pattern, stripped.lstrip()):
                # Don't double-annotate
                if "SCOS:" not in stripped:
                    indent = len(stripped) - len(stripped.lstrip())
                    new_lines.append(" " * indent + annotation + "\n")
                    count += 1
                break
        new_lines.append(line)

    return "".join(new_lines), count


def replace_session_init(content: str) -> tuple[str, bool]:
    """Replace SparkSession.builder patterns with snowpark_connect.init_spark_session().

    Returns (modified_content, was_replaced).
    """
    replaced = False
    for search, replace_with in SESSION_PATTERNS:
        if search in content:
            content = content.replace(search, replace_with, 1)
            replaced = True
            break  # only replace first/most-specific match
    return content, replaced


def append_ewi_entry(analysis_path: str, rel_path: str) -> None:
    """Append a SPRKCNTPY0099 EWI entry to analysis.json for this file."""
    issues: list[dict] = []
    if os.path.exists(analysis_path):
        try:
            with open(analysis_path, "r", encoding="utf-8") as f:
                issues = json.load(f)
        except (json.JSONDecodeError, OSError):
            issues = []

    # Avoid duplicate entries for the same file
    for existing in issues:
        if (existing.get("file") == rel_path
                and existing.get("code") == FALLBACK_EWI_CODE):
            return

    issues.append({
        "file": rel_path,
        "code": FALLBACK_EWI_CODE,
        "lines": "1",
        "root_cause": FALLBACK_EWI_DESCRIPTION,
        "category": "Partial Migration",
        "fix": "Review file manually and apply full SCOS migration fixes",
        "final_risk": 0.9,
        "snowpark_connect_category": "Partial Migration",
    })

    with open(analysis_path, "w", encoding="utf-8") as f:
        json.dump(issues, f, indent=2, ensure_ascii=False)


def transform_file(
    source_path: str,
    output_path: str,
    filename: str,
) -> dict:
    """Apply all fallback transforms to one file. Returns a result dict."""
    result = {
        "file": filename,
        "copied": False,
        "header_added": False,
        "imports_rewritten": 0,
        "session_replaced": False,
        "error": None,
    }

    try:
        # Step 1: copy source if output not present
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if not os.path.exists(output_path):
            shutil.copy2(source_path, output_path)
            result["copied"] = True

        with open(output_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        original = content

        # Step 2: migration header
        content = add_migration_header(content, filename)
        if content != original:
            result["header_added"] = True

        # Step 3: rewrite imports
        content, import_count = rewrite_imports(content)
        result["imports_rewritten"] = import_count

        # Step 4: session init replacement (entry-point detection)
        if is_entry_point(content):
            content, replaced = replace_session_init(content)
            result["session_replaced"] = replaced

        if content != original or result["copied"]:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)

    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SNOW-3383532: Deterministic fallback for unprocessed migration files"
    )
    parser.add_argument(
        "--state", required=True,
        help="Path to migration_state.json",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would be done without modifying files",
    )
    args = parser.parse_args()

    state_path = os.path.abspath(args.state)
    if not os.path.exists(state_path):
        print(f"ERROR: migration_state.json not found: {state_path}", file=sys.stderr)
        return 1

    state = load_state(state_path)
    conversion_root = state.get("conversion_root", os.path.dirname(state_path))
    migrated_dir = state.get("migrated_dir", os.path.join(conversion_root, "Output"))
    analysis_path = os.path.join(conversion_root, "analysis.json")

    # Resolve original source directory (parent of Output/)
    source_dir = os.path.dirname(migrated_dir.rstrip("/"))

    unprocessed = find_unprocessed_files(state)

    print("SCOS Deterministic Fallback Transformation")
    print("==========================================")
    print(f"  State:         {state_path}")
    print(f"  Source dir:    {source_dir}")
    print(f"  Output dir:    {migrated_dir}")
    print(f"  Analysis:      {analysis_path}")
    print(f"  Unprocessed:   {len(unprocessed)} file(s)")
    print()

    if not unprocessed:
        print("All manifest files were processed by sub-agents. No fallback needed.")
        state["fallback_processed"] = []
        state["fallback_count"] = 0
        if not args.dry_run:
            save_state(state_path, state)
        return 0

    if args.dry_run:
        for f in unprocessed:
            print(f"  DRY-RUN: would transform {f}")
        print(f"\n{len(unprocessed)} file(s) would be fallback-transformed.")
        return 0

    fallback_processed = []
    total_imports = 0
    total_sessions = 0
    errors = []

    for rel_path in unprocessed:
        # rel_path may be absolute or relative to source
        if os.path.isabs(rel_path):
            source_file = rel_path
            try:
                display_rel = os.path.relpath(rel_path, source_dir)
            except ValueError:
                display_rel = os.path.basename(rel_path)
            out_file = os.path.join(migrated_dir, display_rel)
        else:
            source_file = os.path.join(source_dir, rel_path)
            out_file = os.path.join(migrated_dir, rel_path)
            display_rel = rel_path

        if not os.path.exists(source_file):
            # Try migrated dir — file may already have been copied but not fixed
            source_file = out_file

        if not os.path.exists(source_file):
            print(f"  SKIP  {display_rel} — source file not found")
            errors.append(display_rel)
            continue

        result = transform_file(source_file, out_file, os.path.basename(display_rel))

        if result["error"]:
            print(f"  ERROR {display_rel} — {result['error']}")
            errors.append(display_rel)
            continue

        # Step 5: record EWI entry
        append_ewi_entry(analysis_path, rel_path)

        fallback_processed.append(rel_path)
        total_imports += result["imports_rewritten"]
        if result["session_replaced"]:
            total_sessions += 1

        flags = []
        if result["copied"]:
            flags.append("copied")
        if result["header_added"]:
            flags.append("header")
        if result["imports_rewritten"]:
            flags.append(f"{result['imports_rewritten']} import(s) annotated")
        if result["session_replaced"]:
            flags.append("session replaced")
        print(f"  DONE  {display_rel} [{', '.join(flags) or 'no-op'}]")

    print()
    print(f"Fallback complete: {len(fallback_processed)} file(s) transformed, "
          f"{total_imports} import(s) annotated, {total_sessions} session init(s) replaced.")
    if errors:
        print(f"Errors: {len(errors)} file(s) skipped — {errors}")

    # Update migration_state.json
    state["fallback_processed"] = fallback_processed
    state["fallback_count"] = len(fallback_processed)
    if errors:
        state["fallback_errors"] = errors
    save_state(state_path, state)

    return 0


if __name__ == "__main__":
    sys.exit(main())
