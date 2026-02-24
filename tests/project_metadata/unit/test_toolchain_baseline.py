"""Tests for repository toolchain baseline configuration."""

from __future__ import annotations

import tomllib
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def test_project_uses_python_311_baseline_in_pyproject() -> None:
    pyproject_path = _project_root() / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    assert pyproject["project"]["requires-python"] == ">=3.11"
    assert pyproject["tool"]["ruff"]["target-version"] == "py311"
    assert pyproject["tool"]["mypy"]["python_version"] == "3.11"


def test_project_uses_uv_style_metadata_without_poetry() -> None:
    pyproject_path = _project_root() / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    dev_dependencies = pyproject["dependency-groups"]["dev"]

    assert "black" not in dev_dependencies
    assert "black" not in pyproject["tool"]
    assert "poetry" not in pyproject["tool"]
    assert pyproject["build-system"]["build-backend"] != "poetry.core.masonry.api"


def test_dry_run_is_documented_without_non_spec_language() -> None:
    project_root = _project_root()

    files_to_check = (
        project_root / "README.md",
        project_root / "docs" / "spec-alignment.md",
        project_root / "docs" / "workflows.md",
        project_root / "src" / "simple_e2e_tester" / "cli.py",
    )
    for path in files_to_check:
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            lowered = line.lower()
            if "--dry-run" in lowered:
                assert "non-spec" not in lowered
                assert "not part of spec" not in lowered
                assert "strict spec contract" not in lowered

    workflows_doc = (project_root / "docs" / "workflows.md").read_text(encoding="utf-8")
    assert "Extension (non-spec):" not in workflows_doc
