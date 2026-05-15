#!/bin/bash
# SNOW-3347465: Wrapper script that ensures report generation always runs,
# even if the migration agent is interrupted mid-workflow.
#
# This script wraps the SCOS migration skill execution and guarantees that
# generate_scos_reports.py runs as a final step whenever analysis.json exists.
#
# Usage:
#   ./run_scos_migration.sh --analysis <path> --source-dir <path> --output-dir <path> \
#     [--migrated-dir <path>] [--project-name <name>] [--email <email>] \
#     [--company <company>] [--language <lang>]
#
# Environment variables (set by caller or SKILL.md):
#   SKILL_DIR   — Path to the snowpark-connect skill directory (contains pyproject.toml)
#
# Exit codes:
#   0 — Reports generated successfully
#   1 — No analysis.json found (nothing to generate)
#   2 — Report generation failed

set -euo pipefail

# SNOW-3347465: Resolve skill directory from script location if not set
if [ -z "${SKILL_DIR:-}" ]; then
    SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
fi

# Parse arguments — pass all through to generate_scos_reports.py
ANALYSIS=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --analysis)
            ANALYSIS="$2"
            EXTRA_ARGS+=("$1" "$2")
            shift 2
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Default analysis path if not specified
if [ -z "$ANALYSIS" ]; then
    ANALYSIS="analysis.json"
    EXTRA_ARGS=("--analysis" "$ANALYSIS" "${EXTRA_ARGS[@]}")
fi

# SNOW-3347465: Always generate reports if analysis.json exists
if [ -f "$ANALYSIS" ]; then
    echo "Generating reports from $ANALYSIS..."
    uv run --project "$SKILL_DIR" \
        python "$SKILL_DIR/scripts/generate_scos_reports.py" \
        "${EXTRA_ARGS[@]}"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "Reports generated successfully."
    else
        echo "WARNING: Report generation failed with exit code $EXIT_CODE."
        exit 2
    fi
else
    echo "WARNING: No analysis.json found at '$ANALYSIS'. Reports cannot be generated."
    exit 1
fi
