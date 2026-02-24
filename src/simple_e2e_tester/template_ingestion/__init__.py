"""Template ingestion exports."""

from .testcase_models import TemplateReadResult, TemplateTestCase
from .workbook_reader import TemplateValidationError, read_template

__all__ = [
    "TemplateReadResult",
    "TemplateTestCase",
    "TemplateValidationError",
    "read_template",
]
