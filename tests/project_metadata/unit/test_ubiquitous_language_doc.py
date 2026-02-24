"""Tests for ubiquitous language documentation."""

from __future__ import annotations

from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def test_ubiquitous_language_doc_exists_with_core_terms() -> None:
    glossary_path = _project_root() / "docs" / "ubiquitous-language.md"
    assert glossary_path.exists(), "Expected docs/ubiquitous-language.md to exist."

    text = glossary_path.read_text(encoding="utf-8")
    required_terms = (
        "sender",
        "subject",
        "expected event",
        "actual event",
        "event schema",
        "matching field path",
        "test configuration",
        "test case",
        "enabled test case",
        "match result",
        "mismatch",
        "conflict",
        "unmatched expected event",
        "unmatched actual event",
        "send status",
        "run metadata",
        "attachment mode",
        "file-path mode",
        "text-to-pdf mode",
    )
    for term in required_terms:
        assert term in text.lower(), f"Expected glossary to include term: {term}"
