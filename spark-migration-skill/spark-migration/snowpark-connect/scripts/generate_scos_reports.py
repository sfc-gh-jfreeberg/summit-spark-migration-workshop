#!/usr/bin/env python3
"""
Generate SCOS-compatible CSV reports from SCOS migration outputs.

Reads analysis.json and scanned SCOS comments from migrated files to produce:
  - Reports/Issues.csv
  - Reports/InputFilesInventory.csv
  - Reports/ArtifactDependencyInventory.csv
  - Reports/tool_execution.csv
  - Logs/SCOSMigration-Log-<timestamp>.log

These reports are compatible with dvp-sma-dashboard-generator.

Usage:
    python generate_scos_reports.py \
        --output-dir /path/to/output \
        --analysis /path/to/analysis.json \
        --source-dir /path/to/original/source \
        --project-name "MyProject" \
        --email "user@company.com" \
        --company "Company Inc" \
        --language python
"""

import argparse
import ast
import csv
import json
import os
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path

TOOL_VERSION = "scos-migration-1.0.0"

# SNOW-3347464: Scala migration header comment inserted deterministically
SCALA_MIGRATION_HEADER = "// SCOS Migration Output"
PYTHON_MIGRATION_HEADER = '"""\nSCOS Migration Output\n'

DATA_DIR = Path(__file__).parent / "data"

TECHNOLOGY_MAP = {
    ".py": "Python",
    ".scala": "Scala",
    ".sql": "SQL",
    ".ipynb": "Jupyter",
    ".r": "R",
    ".R": "R",
}

# Well-known third-party Python packages (top-level import name)
KNOWN_THIRD_PARTY_PYTHON = {
    "numpy", "pandas", "scipy", "sklearn", "matplotlib", "seaborn",
    "requests", "flask", "django", "sqlalchemy", "boto3", "botocore",
    "google", "azure", "pyspark", "databricks", "delta", "pyarrow",
    "yaml", "pyyaml", "toml", "dotenv", "pytest", "unittest",
    "snowflake", "cryptography", "paramiko", "jinja2", "click",
    "tqdm", "rich", "loguru", "celery", "redis", "kafka",
    "tensorflow", "torch", "keras", "xgboost", "lightgbm",
}

# Well-known third-party Scala/Java packages (prefix)
KNOWN_THIRD_PARTY_SCALA = {
    "org.apache.spark", "org.apache.hadoop", "org.apache.kafka",
    "org.apache.commons", "org.apache.http", "org.apache.log4j",
    "org.apache.avro", "org.apache.parquet", "org.apache.hive",
    "com.databricks", "io.delta", "org.scalatest", "org.scalactic",
    "com.typesafe", "akka", "play", "cats", "zio",
    "com.snowflake", "net.snowflake",
    # SNOW-3362688: Expanded third-party coverage
    "com.amazonaws", "com.google", "com.fasterxml", "com.microsoft",
    "com.twitter", "com.github", "com.hortonworks", "com.cloudera",
    "io.circe", "io.spray", "io.netty",
    "org.json4s", "org.slf4j", "org.log4s", "org.mockito", "org.specs2",
    "org.joda", "org.scalaj", "org.rogach",
    "net.liftweb", "net.ceedubs",
    "pureconfig", "scopt", "enumeratum", "shapeless", "monocle",
    "za.co.absa",
    "spark",  # unqualified spark.* imports
}

# SNOW-3362688: Scala/Java standard library prefixes
SCALA_JAVA_STDLIB = {
    "scala", "java", "javax", "jdk", "sun",
}

# SNOW-3362688: Detect user-defined packages from package declarations in source files
def detect_user_packages(source_dir: str) -> set[str]:
    """Scan Scala files for package declarations and return user package prefixes.

    Extracts `package com.company.project` declarations to identify which
    import prefixes belong to user code rather than third-party libraries.
    Returns a set of package prefixes (e.g., {"com.socgen.htr"}).
    """
    user_packages: set[str] = set()
    for root, _dirs, files in os.walk(source_dir):
        for fname in files:
            if not fname.endswith(".scala"):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("package ") and not line.startswith("package object"):
                            pkg = line[len("package "):].strip().rstrip("{;")
                            if pkg and not pkg.startswith("("):
                                user_packages.add(pkg)
                        # Stop scanning after first non-comment, non-package line
                        if line and not line.startswith("//") and not line.startswith("/*") and not line.startswith("*") and not line.startswith("package"):
                            break
            except OSError:
                continue
    return user_packages

# Python stdlib top-level modules (subset covering the most common)
PYTHON_STDLIB = {
    "abc", "argparse", "ast", "asyncio", "base64", "bisect",
    "calendar", "cmath", "codecs", "collections", "colorsys",
    "concurrent", "configparser", "contextlib", "copy", "csv",
    "ctypes", "dataclasses", "datetime", "decimal", "difflib",
    "dis", "email", "enum", "errno", "fcntl", "filecmp",
    "fnmatch", "fractions", "functools", "gc", "getpass", "glob",
    "gzip", "hashlib", "heapq", "hmac", "html", "http",
    "importlib", "inspect", "io", "itertools", "json", "keyword",
    "linecache", "locale", "logging", "lzma", "math", "mimetypes",
    "multiprocessing", "numbers", "operator", "os", "pathlib",
    "pickle", "pkgutil", "platform", "pprint", "profile",
    "queue", "random", "re", "reprlib", "resource",
    "secrets", "select", "shelve", "shlex", "shutil", "signal",
    "site", "socket", "sqlite3", "ssl", "stat", "statistics",
    "string", "struct", "subprocess", "sys", "syslog", "tempfile",
    "textwrap", "threading", "time", "timeit", "token", "tokenize",
    "traceback", "types", "typing", "unicodedata", "unittest",
    "urllib", "uuid", "venv", "warnings", "weakref", "xml",
    "xmlrpc", "zipfile", "zipimport", "zlib",
    "__future__", "builtins", "_thread",
}


def load_ewi_mapping(language: str) -> list[dict]:
    """Load EWI code mapping for the given language from CSV.

    Looks in data/<language>/ewi_code_mapping.csv first (new layout),
    then falls back to the legacy data/ewi_code_mapping.csv if the
    language-specific file does not exist.
    """
    lang_dir = DATA_DIR / language / "ewi_code_mapping.csv"
    if lang_dir.exists():
        mapping_path = lang_dir
    else:
        mapping_path = DATA_DIR / "ewi_code_mapping.csv"

    entries = []
    with open(mapping_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["language"] == language:
                entries.append(row)
    return entries


def resolve_ewi_code(
    mapping: list[dict],
    category: str,
    root_cause: str | None,
    element: str = "",
) -> dict:
    """
    Resolve the best EWI code for an issue based on category and root_cause keywords.

    Returns dict with: ewi_code, sma_category, description, doc_url
    """
    root_cause_lower = (root_cause or "").lower()

    # First pass: try keyword-based matching for specific codes
    for entry in mapping:
        kw = entry.get("keyword_pattern", "")
        if not kw:
            continue
        keywords = [k.strip().lower() for k in kw.split("|")]
        if any(k in root_cause_lower for k in keywords):
            desc = entry["description_template"].replace("{element}", element)
            return {
                "ewi_code": entry["ewi_code"],
                "sma_category": entry["sma_category"],
                "description": desc,
                "doc_url": entry["doc_url"],
            }

    # Second pass: match by snowpark_connect_category
    category_normalized = category.strip()
    for entry in mapping:
        if entry["snowpark_connect_category"] == category_normalized and not entry.get("keyword_pattern"):
            desc = entry["description_template"].replace("{element}", element)
            return {
                "ewi_code": entry["ewi_code"],
                "sma_category": entry["sma_category"],
                "description": desc,
                "doc_url": entry["doc_url"],
            }

    # Fallback to Generic
    for entry in mapping:
        if entry["snowpark_connect_category"] == "Generic":
            desc = entry["description_template"].replace("{element}", element)
            return {
                "ewi_code": entry["ewi_code"],
                "sma_category": entry["sma_category"],
                "description": desc,
                "doc_url": entry["doc_url"],
            }

    # Absolute fallback — use language-appropriate code prefix
    fallback_prefix = "SPRKCNTSCL" if any(
        e.get("ewi_code", "").startswith("SPRKCNTSCL") for e in mapping
    ) else "SPRKCNTPY"
    return {
        "ewi_code": f"{fallback_prefix}1000",
        "sma_category": "ConversionError",
        "description": f"The element '{element}' is not supported for Snowpark Connect",
        "doc_url": "",
    }


def classify_analysis_issue(issue: dict) -> str:
    """Determine the SCOS category from an analysis.json issue entry."""
    root_cause = (issue.get("root_cause") or "").lower()
    explanation = (issue.get("explanation") or "").lower()
    combined = root_cause + " " + explanation

    if any(kw in combined for kw in ["rdd", "parallelize", "sparkcontext.", ".rdd"]):
        return "RDD"
    if any(kw in combined for kw in ["pyspark.ml", "spark.ml", "org.apache.spark.ml", "ml pipeline", "vectorassembler"]):
        if "mllib" in combined:
            return "Unsupported Module"
        return "Unsupported Module"
    if any(kw in combined for kw in ["streaming", "dstream", "streamingcontext", "org.apache.spark.streaming"]):
        return "Unsupported Module"
    if any(kw in combined for kw in ["pyspark.mllib", "spark.mllib", "org.apache.spark.mllib"]):
        return "Unsupported Module"
    if any(kw in combined for kw in ["graphx", "org.apache.spark.graphx"]):
        return "Unsupported Module"
    if any(kw in combined for kw in ["sparksession", "getorcreate", "session.builder"]):
        return "SparkSession"
    if any(kw in combined for kw in ["sparkcontext"]):
        return "SparkContext"
    if any(kw in combined for kw in ["avro", "orc ", "delta format", "binaryfile"]):
        return "Unsupported Format"
    if "save mode" in combined:
        return "Unsupported Save Mode"
    if "no-op" in combined or "no op" in combined or "silently ignored" in combined:
        return "No-Op Config"
    if any(kw in combined for kw in ["udf", "cloudpickle", "serializ"]):
        return "UDF Serialization"
    if any(kw in combined for kw in ["performance", "stage", "slower"]):
        return "Performance Optimization"
    if "snowflake connector" in combined or "snowflakesession" in combined:
        return "Recommended Improvement"

    return "Generic"


# SNOW-3347464: Deterministic header insertion for Scala (and Python) output files
def ensure_migration_headers(migrated_dir: str, language: str) -> int:
    """Insert migration header into every output file that is missing one.

    For Scala files: prepends ``// SCOS Migration Output`` as line 1.
    For Python files: prepends a docstring header if absent.

    Returns the number of files that were patched (header was missing).
    """
    ext = ".scala" if language == "scala" else ".py"
    header_marker = "SCOS Migration Output"
    patched = 0

    for root, _dirs, files in os.walk(migrated_dir):
        for fname in files:
            if not fname.endswith(ext):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    # SNOW-3347464: Check first 5 lines for existing header
                    head_lines = [f.readline() for _ in range(5)]
                if any(header_marker in ln for ln in head_lines):
                    continue

                # Re-read full content and prepend header
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                if language == "scala":
                    new_content = SCALA_MIGRATION_HEADER + "\n" + content
                else:
                    new_content = (
                        '"""\nSCOS Migration Output\n'
                        "=====================\n"
                        f"Source File: {fname}\n"
                        f"Migrated on: {datetime.now().strftime('%Y-%m-%d')}\n"
                        "\nChanges Overview:\n"
                        "- Deterministic header added by report generator.\n"
                        "\nKnown Limitations:\n"
                        "- None\n"
                        '"""\n'
                    ) + content

                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(new_content)
                patched += 1
            except OSError:
                continue

    return patched


# SNOW-3347464: EWI deduplication helpers
def normalize_pattern(description: str) -> str:
    """Normalize an EWI description for dedup grouping.

    Strips whitespace, lowercases, and removes variable-length numeric
    literals and quoted identifiers so structurally identical patterns
    collapse into the same key.
    """
    s = description.strip().lower()
    # Replace quoted identifiers: "Foo", 'Bar', `Baz`
    s = re.sub(r'["\'][^"\']*["\']', '""', s)
    s = re.sub(r"`[^`]*`", '""', s)
    # Collapse runs of digits
    s = re.sub(r"\d+", "N", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s


def deduplicate_issues(rows: list[dict]) -> list[dict]:
    """Deduplicate Issues.csv rows by (Code, normalized Description).

    Groups identical EWI patterns and aggregates:
      - file_count: number of distinct files affected
      - affected_files: semicolon-separated list of FileId values

    Keeps the first occurrence's details for Description, Category, etc.
    """
    from collections import OrderedDict

    groups: OrderedDict[tuple[str, str], dict] = OrderedDict()

    for row in rows:
        key = (row["Code"], normalize_pattern(row["Description"]))
        if key not in groups:
            groups[key] = {
                **row,
                "_files": {row["FileId"]} if row["FileId"] else set(),
            }
        else:
            if row["FileId"]:
                groups[key]["_files"].add(row["FileId"])

    deduped = []
    for group in groups.values():
        files = sorted(group.pop("_files"))
        group["FileCount"] = str(len(files))
        group["AffectedFiles"] = ";".join(files)
        deduped.append(group)

    return deduped


def parse_scos_comment(line: str, line_num: int, file_path: str, language: str) -> dict | None:
    """Parse a SCOS comment line and return an issue dict, or None."""
    prefix = "//" if language == "scala" else "#"

    pattern = rf"^\s*{re.escape(prefix)}\s*SCOS:\s*(.*)"
    m = re.match(pattern, line)
    if not m:
        return None

    body = m.group(1).strip()

    if body.startswith("TODO -") or body.startswith("TODO:"):
        return {
            "snowpark_connect_category": "Snowpark Connect TODO",
            "description": body,
            "line": line_num,
            "file": file_path,
        }
    elif body.startswith("Performance tip -") or body.startswith("Performance tip:"):
        return {
            "snowpark_connect_category": "Snowpark Connect Performance",
            "description": body,
            "line": line_num,
            "file": file_path,
        }
    else:
        return {
            "snowpark_connect_category": "Snowpark Connect Fix",
            "description": body,
            "line": line_num,
            "file": file_path,
        }


def scan_scos_comments(migrated_dir: str, language: str) -> list[dict]:
    """Scan migrated files for SCOS comments and return issue dicts."""
    issues = []
    ext = ".scala" if language == "scala" else ".py"

    for root, _dirs, files in os.walk(migrated_dir):
        for fname in files:
            if not fname.endswith(ext):
                continue
            fpath = os.path.join(root, fname)
            rel_path = os.path.relpath(fpath, migrated_dir)
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    for i, line in enumerate(f, start=1):
                        parsed = parse_scos_comment(line, i, rel_path, language)
                        if parsed:
                            issues.append(parsed)
            except OSError:
                continue
    return issues


def generate_issues_csv(
    analysis_path: str,
    migrated_dir: str,
    output_dir: str,
    language: str,
    mapping: list[dict],
    source_dir: str = "",
) -> int:
    """Generate Reports/Issues.csv. Returns count of rows written."""
    reports_dir = os.path.join(output_dir, "Reports")
    os.makedirs(reports_dir, exist_ok=True)
    csv_path = os.path.join(reports_dir, "Issues.csv")

    rows = []

    # Source 1: analysis.json
    if os.path.exists(analysis_path):
        with open(analysis_path, "r", encoding="utf-8") as f:
            analysis = json.load(f)

        for issue in analysis:
            category = classify_analysis_issue(issue)
            code_snippet = issue.get("code", "")
            element = code_snippet[:80] if code_snippet else ""
            resolved = resolve_ewi_code(mapping, category, issue.get("root_cause"), element)

            lines_str = issue.get("lines", "")
            line_num = ""
            if issue.get("cell_id") is not None:
                # Notebook cell reference: format as cell:<cell_id>:<line>
                cell_line = lines_str.split("-")[0] if lines_str else "0"
                line_num = f"cell:{issue['cell_id']}:{cell_line}"
            elif lines_str:
                line_num = lines_str.split("-")[0]

            file_id = issue.get("file", "")
            if file_id and os.path.isabs(file_id):
                try:
                    if migrated_dir and os.path.commonpath([file_id, migrated_dir]) == os.path.normpath(migrated_dir):
                        file_id = os.path.relpath(file_id, migrated_dir)
                    elif source_dir and os.path.commonpath([file_id, source_dir]) == os.path.normpath(source_dir):
                        file_id = os.path.relpath(file_id, source_dir)
                    else:
                        output_idx = file_id.find("/Output/")
                        if output_idx >= 0:
                            file_id = file_id[output_idx + len("/Output/"):]
                except ValueError:
                    output_idx = file_id.find("/Output/")
                    if output_idx >= 0:
                        file_id = file_id[output_idx + len("/Output/"):]
            file_id = file_id.replace("\\", "/").lstrip("/")

            description = issue.get("root_cause") or issue.get("explanation") or resolved["description"]

            rows.append({
                "Code": resolved["ewi_code"],
                "Description": description,
                "Category": resolved["sma_category"],
                "FileId": file_id,
                "Line": line_num,
                "Column": "",
                "Url": resolved["doc_url"],
            })

    # Source 2: SCOS comments in migrated files
    if migrated_dir and os.path.isdir(migrated_dir):
        comments = scan_scos_comments(migrated_dir, language)
        for c in comments:
            resolved = resolve_ewi_code(mapping, c["snowpark_connect_category"], c["description"], "")
            rows.append({
                "Code": resolved["ewi_code"],
                "Description": c["description"],
                "Category": resolved["sma_category"],
                "FileId": c["file"].replace("\\", "/").lstrip("/"),
                "Line": str(c["line"]),
                "Column": "",
                "Url": resolved["doc_url"],
            })

    # SNOW-3347464: Deduplicate by (EWI code, normalized description pattern)
    deduped_rows = deduplicate_issues(rows)

    fieldnames = ["Code", "Description", "Category", "FileId", "Line", "Column", "Url",
                  "FileCount", "AffectedFiles"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped_rows)

    print(f"  Issues.csv: {len(deduped_rows)} rows written ({len(rows)} before dedup) to {csv_path}")
    return len(deduped_rows)


def generate_input_files_inventory(
    source_dir: str,
    output_dir: str,
    project_name: str,
    execution_id: str,
) -> int:
    """Generate Reports/InputFilesInventory.csv. Returns count of rows."""
    reports_dir = os.path.join(output_dir, "Reports")
    os.makedirs(reports_dir, exist_ok=True)
    csv_path = os.path.join(reports_dir, "InputFilesInventory.csv")

    rows = []
    for root, _dirs, files in os.walk(source_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            rel_path = os.path.relpath(fpath, source_dir).replace("\\", "/")
            ext = os.path.splitext(fname)[1].lower()
            tech = TECHNOLOGY_MAP.get(ext, "Other")

            try:
                stat = os.stat(fpath)
                byte_size = stat.st_size
            except OSError:
                byte_size = 0

            loc = 0
            char_len = 0
            parse_result = "Parsed"
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                    char_len = len(content)
                    loc = content.count("\n") + (1 if content and not content.endswith("\n") else 0)
            except OSError:
                parse_result = "Error"

            rows.append({
                "Element": fname,
                "ProjectId": project_name,
                "FileId": rel_path,
                "Count": 1,
                "SessionId": execution_id,
                "Extension": ext,
                "Technology": tech,
                "Bytes": byte_size,
                "CharacterLength": char_len,
                "LinesOfCode": loc,
                "ParseResult": parse_result,
                "Ignored": "False",
                "OriginFilePath": fpath,
            })

    fieldnames = [
        "Element", "ProjectId", "FileId", "Count", "SessionId", "Extension",
        "Technology", "Bytes", "CharacterLength", "LinesOfCode", "ParseResult",
        "Ignored", "OriginFilePath",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  InputFilesInventory.csv: {len(rows)} rows written to {csv_path}")
    return len(rows)


def extract_python_imports(file_path: str) -> list[tuple[str, str]]:
    """Extract import statements from a Python file. Returns list of (module, full_statement)."""
    imports = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            source = f.read()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append((alias.name, f"import {alias.name}"))
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module:
                    imports.append((module, f"from {module} import ..."))
    except (SyntaxError, OSError):
        pass
    return imports


def extract_scala_imports(file_path: str) -> list[tuple[str, str]]:
    """Extract import statements from a Scala file. Returns list of (package, full_statement)."""
    imports = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = re.match(r"^\s*import\s+([\w.]+(?:\.\{[^}]+\}|\.\*|\._)?)", line)
                if m:
                    full_import = m.group(1)
                    base = full_import.split(".{")[0].split("._")[0].split(".*")[0]
                    imports.append((base, f"import {full_import}"))
    except OSError:
        pass
    return imports


def classify_dependency(module: str, language: str, source_files: set[str], user_packages: set[str] | None = None) -> str:
    """Classify a dependency as UserCodeFile, ThirdPartyLibraries, or UnknownLibraries."""
    if language == "python":
        top_level = module.split(".")[0]

        # Check if it's a local file in the project
        module_as_path = module.replace(".", "/")
        candidates = [
            module_as_path + ".py",
            module_as_path + "/__init__.py",
            os.path.join(module_as_path, "__init__.py"),
        ]
        for c in candidates:
            if c in source_files:
                return "UserCodeFile"

        if top_level in PYTHON_STDLIB:
            return "ThirdPartyLibraries"
        if top_level in KNOWN_THIRD_PARTY_PYTHON:
            return "ThirdPartyLibraries"
        return "UnknownLibraries"

    else:  # scala
        top_level = module.split(".")[0]

        # SNOW-3362688: Check Scala/Java stdlib first
        if top_level in SCALA_JAVA_STDLIB:
            return "ThirdPartyLibraries"

        # SNOW-3362688: Check user-defined packages from package declarations
        if user_packages:
            for user_pkg in user_packages:
                if module.startswith(user_pkg):
                    return "UserCodeFile"

        # Check known third-party
        for prefix in KNOWN_THIRD_PARTY_SCALA:
            if module.startswith(prefix):
                return "ThirdPartyLibraries"

        # Check if it matches a local file
        module_as_path = module.replace(".", "/") + ".scala"
        if module_as_path in source_files:
            return "UserCodeFile"
        return "UnknownLibraries"


def generate_artifact_dependency_inventory(
    source_dir: str,
    output_dir: str,
    language: str,
    execution_id: str,
) -> int:
    """Generate Reports/ArtifactDependencyInventory.csv. Returns count of rows."""
    reports_dir = os.path.join(output_dir, "Reports")
    os.makedirs(reports_dir, exist_ok=True)
    csv_path = os.path.join(reports_dir, "ArtifactDependencyInventory.csv")

    ext = ".scala" if language == "scala" else ".py"
    extract_fn = extract_scala_imports if language == "scala" else extract_python_imports

    # Build set of all source files (relative paths)
    source_files = set()
    for root, _dirs, files in os.walk(source_dir):
        for fname in files:
            rel = os.path.relpath(os.path.join(root, fname), source_dir).replace("\\", "/")
            source_files.add(rel)

    # SNOW-3362688: Detect user-defined packages for Scala classification
    user_packages = detect_user_packages(source_dir) if language == "scala" else None

    rows = []
    for root, _dirs, files in os.walk(source_dir):
        for fname in files:
            if not fname.endswith(ext):
                continue
            fpath = os.path.join(root, fname)
            rel_path = os.path.relpath(fpath, source_dir).replace("\\", "/")

            imports = extract_fn(fpath)
            for module, _stmt in imports:
                dep_type = classify_dependency(module, language, source_files, user_packages=user_packages)
                rows.append({
                    "ExecutionId": execution_id,
                    "FileId": rel_path,
                    "Dependency": module,
                    "Type": dep_type,
                    "Success": "True",
                    "StatusDetail": "Parsed",
                    "Arguments": "",
                    "Location": "",
                    "IndirectDependencies": "",
                    "TotalIndirectDependencies": 0,
                    "DirectParents": "",
                    "TotalDirectParents": 0,
                    "IndirectParents": "",
                    "TotalIndirectParents": 0,
                })

    fieldnames = [
        "ExecutionId", "FileId", "Dependency", "Type", "Success", "StatusDetail",
        "Arguments", "Location", "IndirectDependencies", "TotalIndirectDependencies",
        "DirectParents", "TotalDirectParents", "IndirectParents", "TotalIndirectParents",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ArtifactDependencyInventory.csv: {len(rows)} rows written to {csv_path}")
    return len(rows)


def generate_tool_execution_csv(output_dir: str, execution_id: str) -> None:
    """Generate Reports/tool_execution.csv."""
    reports_dir = os.path.join(output_dir, "Reports")
    os.makedirs(reports_dir, exist_ok=True)
    csv_path = os.path.join(reports_dir, "tool_execution.csv")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ExecutionId", "ToolVersion"])
        writer.writeheader()
        writer.writerow({"ExecutionId": execution_id, "ToolVersion": TOOL_VERSION})

    print(f"  tool_execution.csv: written to {csv_path}")


def generate_log_file(
    output_dir: str,
    project_name: str,
    email: str,
    company: str,
    execution_id: str,
    source_dir: str,
    language: str = "python",
) -> None:
    """Generate Logs/<Language>SnowConvert-Log-<timestamp>.log."""
    logs_dir = os.path.join(output_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d.%H%M%S")
    lang_label = "Scala" if language == "scala" else "Python"
    log_path = os.path.join(logs_dir, f"{lang_label}SnowConvert-Log-{timestamp}.log")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"SCOS Migration Log\n")
        f.write(f"==================\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"ExecutionId: {execution_id}\n")
        f.write(f"ToolVersion: {TOOL_VERSION}\n")
        f.write(f"ProjectName: {project_name}\n")
        f.write(f"OwnerEmail: {email}\n")
        f.write(f"OwnerCompany: {company}\n")
        f.write(f"SourceDirectory: {source_dir}\n")
        f.write(f"OutputDirectory: {output_dir}\n")

    print(f"  Log file: written to {log_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate SCOS-compatible reports from SCOS migration outputs")
    parser.add_argument("--output-dir", required=True, help="Root output directory (Reports/ and Logs/ will be created here)")
    parser.add_argument("--analysis", default="analysis.json", help="Path to analysis.json (default: analysis.json)")
    parser.add_argument("--source-dir", required=True, help="Original source code directory (for InputFilesInventory)")
    parser.add_argument("--migrated-dir", default=None, help="Migrated _scos directory to scan for SCOS comments (auto-detected if not set)")
    parser.add_argument("--project-name", default="SCOS Migration", help="Project name")
    parser.add_argument("--email", default="", help="Customer email")
    parser.add_argument("--company", default="", help="Customer company")
    parser.add_argument("--language", choices=["python", "scala"], default="python", help="Source language (default: python)")

    args = parser.parse_args()

    output_dir = os.path.abspath(args.output_dir)
    source_dir = os.path.abspath(args.source_dir)
    analysis_path = os.path.abspath(args.analysis)

    # Auto-detect migrated directory
    migrated_dir = args.migrated_dir
    if migrated_dir is None:
        candidate = source_dir + "_scos"
        if os.path.isdir(candidate):
            migrated_dir = candidate
        else:
            migrated_dir = output_dir
    migrated_dir = os.path.abspath(migrated_dir) if migrated_dir else None

    execution_id = str(uuid.uuid4())

    print(f"SCOS Report Generator")
    print(f"=====================")
    print(f"  Output:    {output_dir}")
    print(f"  Analysis:  {analysis_path}")
    print(f"  Source:    {source_dir}")
    print(f"  Migrated:  {migrated_dir}")
    print(f"  Language:  {args.language}")
    print(f"  Execution: {execution_id}")
    print()

    # Load EWI mapping
    print("Loading EWI code mapping...")
    mapping = load_ewi_mapping(args.language)
    print(f"  Loaded {len(mapping)} mapping entries for {args.language}")
    print()

    # Generate all reports
    print("Generating reports...")

    # SNOW-3347464: Ensure every output file has a migration header before scanning
    if migrated_dir and os.path.isdir(migrated_dir):
        patched = ensure_migration_headers(migrated_dir, args.language)
        if patched:
            print(f"  Migration headers: inserted into {patched} files")
        else:
            print("  Migration headers: all files already have headers")
    print()

    issues_count = generate_issues_csv(
        analysis_path, migrated_dir, output_dir, args.language, mapping,
        source_dir=source_dir,
    )

    files_count = generate_input_files_inventory(
        source_dir, output_dir, args.project_name, execution_id
    )

    deps_count = generate_artifact_dependency_inventory(
        source_dir, output_dir, args.language, execution_id
    )

    generate_tool_execution_csv(output_dir, execution_id)

    generate_log_file(
        output_dir, args.project_name, args.email, args.company,
        execution_id, source_dir, args.language
    )

    print()
    print(f"Report generation complete!")
    print(f"  Issues:       {issues_count}")
    print(f"  Input files:  {files_count}")
    print(f"  Dependencies: {deps_count}")
    print(f"  Reports at:   {os.path.join(output_dir, 'Reports')}")
    print(f"  Logs at:      {os.path.join(output_dir, 'Logs')}")


if __name__ == "__main__":
    sys.exit(main() or 0)
