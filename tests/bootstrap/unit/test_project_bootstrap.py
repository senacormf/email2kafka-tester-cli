"""Tests for project bootstrap command orchestration."""

from __future__ import annotations

from pathlib import Path

from simple_e2e_tester.bootstrap.project_bootstrap import bootstrap_project_environment


def test_bootstrap_project_environment_runs_venv_creation_install_and_sync_when_venv_is_missing(
    tmp_path: Path,
) -> None:
    captured_calls: list[tuple[tuple[str, ...], Path]] = []

    def _fake_run(command: tuple[str, ...], cwd: Path) -> None:
        captured_calls.append((command, cwd))

    bootstrap_project_environment(repo_root=tmp_path, run_command=_fake_run)

    assert len(captured_calls) == 3
    assert captured_calls[0][0][1:4] == ("-m", "venv", ".venv")
    assert captured_calls[1][0][-4:] == ("install", "--upgrade", "pip", "uv")
    assert captured_calls[2][0][-2:] == ("sync", "--all-groups")
    assert captured_calls[0][1] == tmp_path


def test_bootstrap_project_environment_skips_venv_creation_when_local_virtual_environment_exists(
    tmp_path: Path,
) -> None:
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.touch()
    captured_calls: list[tuple[tuple[str, ...], Path]] = []

    def _fake_run(command: tuple[str, ...], cwd: Path) -> None:
        captured_calls.append((command, cwd))

    bootstrap_project_environment(repo_root=tmp_path, run_command=_fake_run)

    assert len(captured_calls) == 2
    assert captured_calls[0][0][0] == str(venv_python)
    assert captured_calls[1][0][-2:] == ("sync", "--all-groups")
