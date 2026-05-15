#!/usr/bin/env python3
"""
Bridge script: inject #EWI markers into SCOS conversion output files.

Reads Reports/Issues.csv (produced by generate_scos_reports.py) and injects
EWI-fixer-compatible markers into the corresponding Output/ source files.

For Python files:  #EWI: SPRKCNTPY1000 => <description>
For Scala files:   //EWI: SPRKCNTSCL1000 => <description>

The EWI fixer expects these markers to locate and resolve issues. The SCOS
skill produces '# SCOS:' / '// SCOS:' comments; this script adds the
structured EWI markers that downstream tools consume.

Usage:
    python scos_to_ewi_bridge.py \
        --workload-dir /path/to/Conversion-SCOS-<timestamp> \
        --language python

    The workload-dir should contain:
        Reports/Issues.csv   (source of EWI codes)
        Output/              (migrated source files)
"""

import argparse
import csv
import os
import re
import sys
from pathlib import Path


def build_ewi_marker(code: str, description: str, language: str) -> str:
    """Build an EWI marker comment for the given language.

    Descriptions are flattened to a single line (newlines replaced with spaces)
    to ensure the marker is a single-line comment.
    """
    prefix = "//" if language == "scala" else "#"
    # Flatten multi-line descriptions to single line
    desc_flat = " ".join(description.split()).strip()
    return f"{prefix}EWI: {code} => {desc_flat}"


def is_ewi_marker(line: str, language: str) -> bool:
    """Check if a line already contains an EWI marker."""
    prefix = "//" if language == "scala" else "#"
    pattern = rf"^\s*{re.escape(prefix)}EWI:\s+\S+"
    return bool(re.match(pattern, line))


def find_code_pattern(lines: list[str], target_line: int, code_snippet: str) -> int | None:
    """Fuzzy-match a code pattern within +/- 5 lines of the target.

    Returns the 0-based line index where the pattern was found, or None.
    """
    if not code_snippet or not code_snippet.strip():
        return None

    snippet_stripped = code_snippet.strip()
    # Search window: target_line is 1-based, convert to 0-based
    center = target_line - 1
    start = max(0, center - 5)
    end = min(len(lines), center + 6)

    for i in range(start, end):
        if snippet_stripped in lines[i]:
            return i

    return None


def has_existing_marker(lines: list[str], code: str, description: str, target_idx: int, language: str) -> bool:
    """Check if an EWI marker with the given code+description already exists in the file.

    Uses a two-pass approach:
    1. Fast check: scan +/- 15 lines around target for the exact code (handles small drift)
    2. Full scan: search the entire file for the exact code+description combo

    The full scan is needed because cumulative line insertions can shift markers
    far from their original target (e.g., 20 injections in a 2400-line file).
    """
    # Flatten and truncate description for matching — markers may have been truncated
    desc_flat = " ".join(description.split()).strip()
    desc_key = desc_flat[:60].strip()

    # For empty descriptions, scan the full file for any marker with this code
    # near the target (empty-desc markers are ambiguous, so be conservative)
    if not desc_key:
        start = max(0, target_idx - 30)
        end = min(len(lines), target_idx + 31)
        for i in range(start, end):
            if is_ewi_marker(lines[i], language) and code in lines[i]:
                # Found a marker with this code nearby — treat as duplicate
                return True
        return False

    # Pass 1: local window check (fast path for most cases)
    start = max(0, target_idx - 15)
    end = min(len(lines), target_idx + 16)
    for i in range(start, end):
        if is_ewi_marker(lines[i], language) and code in lines[i]:
            if desc_key in lines[i]:
                return True

    # Pass 2: full file scan for exact code+description (handles large drift)
    for line in lines:
        if is_ewi_marker(line, language) and code in line and desc_key in line:
            return True

    return False


def inject_markers_into_file(
    file_path: str,
    issues: list[dict],
    language: str,
) -> int:
    """Inject EWI markers into a single source file.

    Returns the number of markers injected.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except OSError:
        return 0

    injected = 0
    # Sort issues by line number descending so insertions don't shift later indices
    sorted_issues = sorted(issues, key=lambda x: x["line"], reverse=True)

    for issue in sorted_issues:
        marker = build_ewi_marker(issue["code"], issue["description"], language)
        target_line = issue["line"]
        target_idx = target_line - 1  # 0-based

        # Idempotency: skip if this EWI code+description already has a marker nearby
        if has_existing_marker(lines, issue["code"], issue["description"], target_idx, language):
            continue

        # Determine insertion point
        insert_idx = target_idx
        if insert_idx < 0:
            insert_idx = 0
        if insert_idx > len(lines):
            insert_idx = len(lines)

        # Detect indentation from the target line
        indent = ""
        if 0 <= target_idx < len(lines):
            m = re.match(r"^(\s*)", lines[target_idx])
            if m:
                indent = m.group(1)

        lines.insert(insert_idx, f"{indent}{marker}\n")
        injected += 1

    if injected > 0:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(lines)
        except OSError:
            return 0

    return injected


def load_issues(csv_path: str) -> list[dict]:
    """Load Issues.csv and return structured issue records."""
    issues = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                line_str = row.get("Line", "").strip()
                line_num = int(line_str) if line_str.isdigit() else 0

                issues.append({
                    "code": row.get("Code", ""),
                    "description": row.get("Description", ""),
                    "category": row.get("Category", ""),
                    "file_id": row.get("FileId", ""),
                    "line": line_num,
                })
    except (OSError, csv.Error) as e:
        print(f"Error reading Issues.csv: {e}", file=sys.stderr)
    return issues


def group_issues_by_file(issues: list[dict]) -> dict[str, list[dict]]:
    """Group issues by their FileId."""
    grouped: dict[str, list[dict]] = {}
    for issue in issues:
        file_id = issue["file_id"]
        if not file_id:
            continue
        grouped.setdefault(file_id, []).append(issue)
    return grouped


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inject #EWI markers into SCOS conversion output files"
    )
    parser.add_argument(
        "--workload-dir",
        required=True,
        help="Path to the Conversion-SCOS-<timestamp> directory",
    )
    parser.add_argument(
        "--language",
        choices=["python", "scala"],
        default="python",
        help="Source language (default: python)",
    )

    args = parser.parse_args()

    workload_dir = os.path.abspath(args.workload_dir)
    reports_dir = os.path.join(workload_dir, "Reports")
    output_dir = os.path.join(workload_dir, "Output")
    issues_csv = os.path.join(reports_dir, "Issues.csv")

    if not os.path.isfile(issues_csv):
        print(f"Error: Issues.csv not found at {issues_csv}", file=sys.stderr)
        return 1

    if not os.path.isdir(output_dir):
        print(f"Error: Output directory not found at {output_dir}", file=sys.stderr)
        return 1

    print(f"SCOS-to-EWI Bridge")
    print(f"==================")
    print(f"  Workload:  {workload_dir}")
    print(f"  Language:  {args.language}")
    print(f"  Issues:    {issues_csv}")
    print(f"  Output:    {output_dir}")
    print()

    # Load issues
    issues = load_issues(issues_csv)
    if not issues:
        print("No issues found in Issues.csv. Nothing to inject.")
        return 0

    print(f"Loaded {len(issues)} issues from Issues.csv")

    # Group by file
    by_file = group_issues_by_file(issues)
    print(f"Issues span {len(by_file)} files")
    print()

    # Inject markers
    total_injected = 0
    files_modified = 0
    files_missing = 0

    for file_id, file_issues in sorted(by_file.items()):
        # Resolve file path: FileId is relative to Output/
        file_path = os.path.join(output_dir, file_id.lstrip("/"))

        if not os.path.isfile(file_path):
            files_missing += 1
            print(f"  SKIP (not found): {file_id}")
            continue

        # Filter to issues with line numbers (can't inject without a location)
        injectable = [i for i in file_issues if i["line"] > 0]
        if not injectable:
            continue

        count = inject_markers_into_file(file_path, injectable, args.language)
        if count > 0:
            files_modified += 1
            total_injected += count
            print(f"  {file_id}: {count} markers injected")

    print()
    print(f"Bridge complete!")
    print(f"  Markers injected: {total_injected}")
    print(f"  Files modified:   {files_modified}")
    print(f"  Files missing:    {files_missing}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
