#!/usr/bin/env python3
"""
SNOW-3385158: Deterministic external orchestrator for Phase 2 chunked dispatch.

Manages Phase 2 (Apply Fixes) dispatch by:
  1. Reading migration_state.json to get manifest, migrated_dir, skill_directory
  2. Splitting manifest into budget-aware chunks (default 80k tokens/chunk)
  3. Writing chunk assignments to migration_state.json under phase2_chunks
  4. Printing structured dispatch instructions for the LLM coordinator
  5. Running fallback_transform.py as a mandatory hard gate after all dispatches

Usage:
    python3 orchestrate_phases.py --state /path/to/migration_state.json --phase 2
    python3 orchestrate_phases.py --state /path/to/migration_state.json --phase 2 --budget 80000
    python3 orchestrate_phases.py --state /path/to/migration_state.json --phase 2 --language scala

Returns exit code 0 on success, 1 on configuration errors. Fallback failures
are logged but never fatal — coverage verification always runs.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

DEFAULT_BUDGET = 80_000
TOKENS_PER_FILE_OVERHEAD = 2_000
CHARS_PER_TOKEN = 4


def load_state(state_path: str) -> dict:
    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state_path: str, state: dict) -> None:
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def estimate_file_tokens(file_path: str) -> int:
    """Estimate token count: file_chars // 4 + 2000 overhead per file."""
    try:
        file_chars = os.path.getsize(file_path)
    except OSError:
        file_chars = 0
    return file_chars // CHARS_PER_TOKEN + TOKENS_PER_FILE_OVERHEAD


def build_chunks(manifest: list, migrated_dir: str, budget: int) -> list:
    """Split manifest into budget-aware chunks.

    Each chunk's estimated token cost stays within ``budget``. A single file
    that exceeds the budget on its own is placed in a dedicated chunk so it is
    never silently skipped.
    """
    chunks = []
    current_chunk = []
    current_tokens = 0

    for f in manifest:
        path = f if os.path.isabs(f) else os.path.join(migrated_dir, f)
        file_tokens = estimate_file_tokens(path)

        if current_tokens + file_tokens > budget and current_chunk:
            chunks.append(current_chunk)
            current_chunk = [f]
            current_tokens = file_tokens
        else:
            current_chunk.append(f)
            current_tokens += file_tokens

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def get_processed_files(state: dict) -> set:
    """Return all files already processed according to migration_state.json."""
    phase2 = state.get("2_fixes", {})
    files_done = set(phase2.get("files_done", []))
    processed = set(state.get("processed_files", []))
    return files_done | processed


def resolve_file_path(f: str, migrated_dir: str) -> str:
    return f if os.path.isabs(f) else os.path.join(migrated_dir, f)


def print_dispatch_plan(chunks: list, migrated_dir: str, language: str) -> None:
    """Print structured dispatch instructions for the LLM coordinator."""
    total_files = sum(len(c) for c in chunks)
    print()
    print("=" * 60)
    print("PHASE 2 DISPATCH PLAN")
    print("=" * 60)
    print(f"Total files : {total_files}")
    print(f"Total chunks: {len(chunks)}")
    print(f"Language    : {language}")
    print()

    for i, chunk in enumerate(chunks):
        chunk_tokens = sum(
            estimate_file_tokens(resolve_file_path(f, migrated_dir))
            for f in chunk
        )
        print(f"--- CHUNK {i + 1}/{len(chunks)} ---")
        print(f"Files: {len(chunk)} | Estimated tokens: ~{chunk_tokens:,}")
        print(f"CHUNK_MODE=chunked")
        print(f"CHUNK_ID={i + 1}")
        print(f"CHUNK_FILES={','.join(chunk)}")
        print()

    print("COORDINATOR INSTRUCTIONS:")
    print("  1. For each chunk, spawn agents/fixer.md with the CHUNK_MODE,")
    print("     CHUNK_ID, and CHUNK_FILES values shown above.")
    print("  2. After each agent exits, re-read migration_state.json.")
    print("  3. If pending_files is non-empty, re-run this script —")
    print("     it will recompute chunks from the remaining files.")
    print("  4. Repeat until all chunks are processed.")
    print()


def run_fallback(state_path: str, skill_directory: str, language: str) -> int:
    """Run fallback_transform.py as a mandatory hard gate.

    Always runs regardless of agent coverage. If all files were processed by
    sub-agents, fallback_transform.py is a fast no-op. If any were missed,
    it fills the gaps deterministically.
    """
    fallback_script = os.path.join(skill_directory, "scripts", "fallback_transform.py")
    if not os.path.exists(fallback_script):
        print(
            f"WARNING: fallback_transform.py not found at {fallback_script}",
            file=sys.stderr,
        )
        return 1

    cmd = [sys.executable, fallback_script, "--state", state_path]
    if language == "scala":
        # fallback_transform supports --language for EWI code selection
        cmd += ["--language", language]

    print("=" * 60)
    print("MANDATORY FALLBACK HARD GATE")
    print("=" * 60)
    print(f"Running: {' '.join(cmd)}")
    print("(Always runs — fills any files missed by sub-agents)")
    print()

    result = subprocess.run(cmd)
    return result.returncode


def verify_coverage(state: dict, state_path: str, manifest: list, migrated_dir: str) -> list:
    """Check all manifest files exist in migrated_dir. Returns list of missing files."""
    missing = []
    for f in manifest:
        path = resolve_file_path(f, migrated_dir)
        if not os.path.exists(path):
            missing.append(f)

    print("=" * 60)
    print("COVERAGE VERIFICATION")
    print("=" * 60)
    if missing:
        print(f"MISSING ({len(missing)} file(s)):")
        for m in missing:
            print(f"  - {m}")
        print()
        print("ACTION REQUIRED: Escalate to user — files still absent after fallback.")
    else:
        print(f"Coverage: 100% ({len(manifest)}/{len(manifest)} files present)")

    # Persist coverage result back to state
    state["pending_files"] = missing
    state["orchestrator_coverage_verified"] = len(missing) == 0
    save_state(state_path, state)

    return missing


def orchestrate_phase2(state_path: str, budget: int, language: str) -> int:
    """Main Phase 2 orchestration: chunk → dispatch plan → fallback → verify."""
    state = load_state(state_path)
    manifest: list = state.get("manifest", [])
    migrated_dir: str = state.get("migrated_dir", "")
    skill_directory: str = state.get("skill_directory", "")

    if not manifest:
        print("ERROR: manifest is empty in migration_state.json", file=sys.stderr)
        return 1

    if not migrated_dir:
        print("ERROR: migrated_dir not set in migration_state.json", file=sys.stderr)
        return 1

    print("SCOS Phase 2 Orchestrator")
    print("=========================")
    print(f"  State      : {state_path}")
    print(f"  Manifest   : {len(manifest)} file(s)")
    print(f"  Budget     : {budget:,} tokens/chunk")
    print(f"  Language   : {language}")
    print(f"  Output dir : {migrated_dir}")
    print()

    # Compute budget-aware chunks from the full manifest
    chunks = build_chunks(manifest, migrated_dir, budget)
    total_tokens = sum(
        estimate_file_tokens(resolve_file_path(f, migrated_dir)) for f in manifest
    )
    print(
        f"Chunking: {len(manifest)} files → {len(chunks)} chunk(s) "
        f"(~{total_tokens:,} total tokens, {budget:,} budget/chunk)"
    )

    # Write chunks and updated budget to state before printing dispatch plan
    state["phase2_chunks"] = chunks
    state["context_budget_tokens"] = budget
    save_state(state_path, state)

    # Print the dispatch plan for the LLM coordinator to act on
    print_dispatch_plan(chunks, migrated_dir, language)

    # Mandatory fallback hard gate — always runs, never conditional
    if skill_directory:
        fallback_rc = run_fallback(state_path, skill_directory, language)
        if fallback_rc != 0:
            print(
                f"WARNING: fallback_transform.py exited {fallback_rc} — "
                "some files may not have been transformed",
                file=sys.stderr,
            )
    else:
        print(
            "WARNING: skill_directory not set in migration_state.json — "
            "skipping fallback_transform.py",
            file=sys.stderr,
        )

    # Re-read state after fallback (fallback updates pending_files)
    state = load_state(state_path)
    migrated_dir = state.get("migrated_dir", migrated_dir)
    manifest = state.get("manifest", manifest)

    # Final coverage verification
    print()
    missing = verify_coverage(state, state_path, manifest, migrated_dir)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SNOW-3385158: External orchestrator for deterministic Phase 2 dispatch"
    )
    parser.add_argument(
        "--state",
        required=True,
        help="Path to migration_state.json",
    )
    parser.add_argument(
        "--phase",
        type=int,
        default=2,
        help="Migration phase to orchestrate (default: 2; only phase 2 is supported)",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=DEFAULT_BUDGET,
        help=f"Token budget per chunk (default: {DEFAULT_BUDGET:,})",
    )
    parser.add_argument(
        "--language",
        choices=["python", "scala"],
        default="python",
        help="Source language of the workload (default: python)",
    )
    args = parser.parse_args()

    state_path = os.path.abspath(args.state)
    if not os.path.exists(state_path):
        print(f"ERROR: migration_state.json not found: {state_path}", file=sys.stderr)
        return 1

    if args.phase != 2:
        print(
            f"ERROR: Phase {args.phase} is not supported. Only --phase 2 is implemented.",
            file=sys.stderr,
        )
        return 1

    return orchestrate_phase2(state_path, args.budget, args.language)


if __name__ == "__main__":
    sys.exit(main())
