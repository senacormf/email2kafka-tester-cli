"""Shared template generation constants."""

from __future__ import annotations

TEMPLATE_SHEET_NAME = "TestCases"
SCHEMA_SHEET_NAME = "Schema"

METADATA_COLUMNS: tuple[str, ...] = ("ID", "Tags", "Enabled", "Notes")
INPUT_COLUMNS: tuple[str, ...] = ("FROM", "SUBJECT", "BODY", "ATTACHMENT")
