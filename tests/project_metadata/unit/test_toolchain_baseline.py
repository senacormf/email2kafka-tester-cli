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