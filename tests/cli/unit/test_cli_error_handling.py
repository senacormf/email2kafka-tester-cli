"""CLI error-handling tests."""

from __future__ import annotations

from simple_e2e_tester.cli import main


def test_missing_required_option_returns_clean_click_error(capsys) -> None:
    exit_code = main(["generate-template", "--output", "/tmp/out.xlsx"])
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "Missing option" in captured.err
    assert "--config" in captured.err
    assert "Traceback" not in captured.err


def test_unknown_option_returns_clean_click_error(capsys) -> None:
    exit_code = main(["generate-template", "--bogus"])
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "No such option: --bogus" in captured.err
    assert "Traceback" not in captured.err
