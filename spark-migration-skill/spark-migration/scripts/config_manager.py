#!/usr/bin/env python3
"""
Config Manager – Configuration management for Snowpark Migrator

Handles listing, loading, creating, and saving project configurations.
Each project has its own JSON file under configurations/<project_name>.json.

Usage:
    from config_manager import list_configurations, load_configuration, ...

    # List available projects
    projects = list_configurations("/path/to/skill/configurations")

    # Load (and merge defaults into) a config
    cfg = load_configuration("/path/to/skill/configurations/my_project.json")

    # Create a brand-new config with defaults
    path, cfg = create_configuration("/path/to/skill/configurations", "my_project")

    # Persist updates
    cfg = save_configuration("/path/to/skill/configurations/my_project.json",
                             {"email": "user@co.com"})

CLI usage (from SKILL.md inline blocks):
    python3 config_manager.py list   <config_dir>
    python3 config_manager.py load   <config_path>
    python3 config_manager.py create <config_dir> <project_name>
    python3 config_manager.py save   <config_path> '<json_updates>'
"""

import json
import os
import sys
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULTS: Dict[str, str] = {
    "conversion_type": "scos",
    "enable_jupyter_conversion": "yes",
    "generate_checkpoints": "yes",
    "migration_status": "migrate",
    "run_dvp_orchestrator": "yes",
    "run_ewi_fixer": "yes",
    "run_ewi_fixer.ewi_comments": "mark",
    "run_ewi_fixer.ewi_scope": "only_pending",
    "run_notebook_migration": "yes",
    "run_stage_conversion": "yes",
    "run_stage_conversion.stage_name": "migration_stage",
    "sql_flavor": "SparkSql",
}

GLOBAL_CONFIG_NAME = "config.json"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_config_path(config_dir: str, project_name: str) -> str:
    """Return the full path for a project's configuration file.

    >>> get_config_path("/skill/configurations", "my_project")
    '/skill/configurations/my_project.json'
    """
    return os.path.join(config_dir, f"{project_name}.json")


def list_configurations(config_dir: str) -> List[str]:
    """Return sorted project names from the configurations directory.

    Scans *config_dir* for ``*.json`` files and returns their stems
    (filenames without the ``.json`` extension).  Returns an empty list
    when the directory does not exist or is empty.
    """
    if not os.path.isdir(config_dir):
        return []
    names = []
    for entry in os.listdir(config_dir):
        if entry.endswith(".json"):
            names.append(entry[: -len(".json")])
    return sorted(names)


def load_configuration(config_path: str) -> Dict[str, str]:
    """Read a configuration file, merge missing defaults, and persist.

    If any default keys are missing from the file on disk the merged
    result is written back (deterministic: ``sort_keys=True``).

    Raises ``FileNotFoundError`` when *config_path* does not exist.
    """
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        cfg: Dict[str, str] = json.load(fh)

    changed = _merge_defaults(cfg)
    if changed:
        _write(config_path, cfg)

    return cfg


def create_configuration(
    config_dir: str, project_name: str
) -> Tuple[str, Dict[str, str]]:
    """Create a new configuration with all defaults pre-populated.

    Creates the *config_dir* directory if it does not exist.
    Returns ``(config_path, config_dict)``.
    """
    os.makedirs(config_dir, exist_ok=True)
    config_path = get_config_path(config_dir, project_name)
    cfg: Dict[str, str] = dict(DEFAULTS)
    cfg["project_name"] = project_name
    _write(config_path, cfg)
    return config_path, cfg


def save_configuration(config_path: str, updates: Dict[str, str]) -> Dict[str, str]:
    """Merge *updates* into the existing configuration and persist.

    Reads the current file (or starts from an empty dict if it does not
    exist), applies *updates*, and writes the result deterministically.

    Returns the full merged configuration.
    """
    cfg: Dict[str, str] = {}
    if os.path.isfile(config_path):
        with open(config_path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    cfg.update(updates)
    _write(config_path, cfg)
    return cfg


def get_global_config_path(skill_dir: str) -> str:
    """Return the path to the global config file: ``<skill_dir>/config.json``."""
    return os.path.join(skill_dir, GLOBAL_CONFIG_NAME)


def load_global(skill_dir: str) -> Dict[str, str]:
    """Load the global configuration (``config.json`` at the skill root).

    Returns an empty dict if the file does not exist.
    """
    path = get_global_config_path(skill_dir)
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_global(skill_dir: str, updates: Dict[str, str]) -> Dict[str, str]:
    """Merge *updates* into the global config and persist.

    Creates the file if it does not exist.
    Returns the full merged configuration.
    """
    path = get_global_config_path(skill_dir)
    cfg: Dict[str, str] = {}
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    cfg.update(updates)
    _write(path, cfg)
    return cfg


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _merge_defaults(cfg: Dict[str, str]) -> bool:
    """Add missing default keys to *cfg* in-place.  Returns True if changed."""
    changed = False
    for key, value in DEFAULTS.items():
        if key not in cfg:
            cfg[key] = value
            changed = True
    return changed


def _write(path: str, cfg: Dict[str, str]) -> None:
    """Write *cfg* to *path* with deterministic formatting."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2, sort_keys=True)
        fh.write("\n")


# ---------------------------------------------------------------------------
# CLI entry-point  (used by SKILL.md inline blocks)
# ---------------------------------------------------------------------------


def _cli() -> None:
    """Minimal CLI so SKILL.md can call ``python3 config_manager.py <cmd> ...``."""
    if len(sys.argv) < 2:
        print("Usage: config_manager.py <list|load|create|save> ...", file=sys.stderr)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "list":
        # python3 config_manager.py list <config_dir>
        config_dir = sys.argv[2]
        names = list_configurations(config_dir)
        print(json.dumps(names))

    elif cmd == "load":
        # python3 config_manager.py load <config_path>
        config_path = sys.argv[2]
        cfg = load_configuration(config_path)
        print(json.dumps(cfg, indent=2, sort_keys=True))

    elif cmd == "create":
        # python3 config_manager.py create <config_dir> <project_name>
        config_dir = sys.argv[2]
        project_name = sys.argv[3]
        _, cfg = create_configuration(config_dir, project_name)
        print(json.dumps(cfg, indent=2, sort_keys=True))

    elif cmd == "save":
        # python3 config_manager.py save <config_path> '<json_updates>'
        config_path = sys.argv[2]
        updates = json.loads(sys.argv[3])
        cfg = save_configuration(config_path, updates)
        print(json.dumps(cfg, indent=2, sort_keys=True))

    elif cmd == "load-global":
        # python3 config_manager.py load-global <skill_dir>
        skill_dir = sys.argv[2]
        cfg = load_global(skill_dir)
        print(json.dumps(cfg, indent=2, sort_keys=True))

    elif cmd == "save-global":
        # python3 config_manager.py save-global <skill_dir> '<json_updates>'
        skill_dir = sys.argv[2]
        updates = json.loads(sys.argv[3])
        cfg = save_global(skill_dir, updates)
        print(json.dumps(cfg, indent=2, sort_keys=True))

    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    _cli()
