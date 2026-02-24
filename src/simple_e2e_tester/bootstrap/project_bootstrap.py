"""Bootstrap orchestration for local uv-managed environments."""

from __future__ import annotations

import shlex
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

CommandRunner = Callable[[tuple[str, ...], Path], None]


class BootstrapError(Exception):
    """Raised when project bootstrap commands fail."""


def bootstrap_project_environment(
    *, repo_root: Path, run_command: CommandRunner | None = None
) -> None:
    """Prepare `.venv`, install uv into it, and sync all dependency groups."""
    command_runner = run_command or _run_checked_command
    resolved_repo_root = repo_root.resolve()
    venv_python = _find_existing_venv_python(resolved_repo_root)
    if venv_python is None:
        command_runner((sys.executable, "-m", "venv", ".venv"), resolved_repo_root)
        venv_python = _default_venv_python_path(resolved_repo_root)

    command_runner(
        (str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "uv"),
        resolved_repo_root,
    )
    command_runner(
        (str(_default_venv_uv_path(resolved_repo_root)), "sync", "--all-groups"),
        resolved_repo_root,
    )


def _run_checked_command(command: tuple[str, ...], cwd: Path) -> None:
    """Run one bootstrap command and wrap subprocess errors with domain-friendly messages."""
    try:
        subprocess.run(list(command), cwd=cwd, check=True)
    except FileNotFoundError as exc:
        command_text = shlex.join(command)
        raise BootstrapError(f"Bootstrap command not found: {command_text}") from exc
    except subprocess.CalledProcessError as exc:
        command_text = shlex.join(command)
        raise BootstrapError(
            f"Bootstrap command failed with exit code {exc.returncode}: {command_text}"
        ) from exc


def _find_existing_venv_python(repo_root: Path) -> Path | None:
    """Return local virtual-environment python if present."""
    for candidate in _venv_python_candidates(repo_root):
        if candidate.exists():
            return candidate
    return None


def _venv_python_candidates(repo_root: Path) -> tuple[Path, Path]:
    return (
        repo_root / ".venv" / "bin" / "python",
        repo_root / ".venv" / "Scripts" / "python.exe",
    )


def _default_venv_python_path(repo_root: Path) -> Path:
    if sys.platform.startswith("win"):
        return repo_root / ".venv" / "Scripts" / "python.exe"
    return repo_root / ".venv" / "bin" / "python"


def _default_venv_uv_path(repo_root: Path) -> Path:
    if sys.platform.startswith("win"):
        return repo_root / ".venv" / "Scripts" / "uv.exe"
    return repo_root / ".venv" / "bin" / "uv"
