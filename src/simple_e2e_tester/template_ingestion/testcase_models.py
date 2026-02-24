"""Template ingestion entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

CellValue = object


@dataclass(frozen=True)
class TemplateTestCase:  # pylint: disable=too-many-instance-attributes
    """Normalized representation of an Excel testcase row."""

    row_number: int
    test_id: str
    tags: tuple[str, ...]
    enabled: bool
    notes: str
    from_address: str
    subject: str
    body: str
    attachment: str
    expected_values: Mapping[str, CellValue]


@dataclass(frozen=True)
class TemplateReadResult:
    """Result of ingesting a template workbook."""

    testcases: tuple[TemplateTestCase, ...]
