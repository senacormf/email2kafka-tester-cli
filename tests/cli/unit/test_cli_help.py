"""CLI smoke tests."""

from click.testing import CliRunner
from simple_e2e_tester.cli import cli


def test_cli_displays_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "generate-template" in result.output
    assert "run" in result.output
