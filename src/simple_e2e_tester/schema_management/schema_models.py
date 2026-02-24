"""Schema management entities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SchemaDocument:
    """Structured representation of a schema definition."""

    schema_type: str
    root: Any


@dataclass(frozen=True)
class FlattenedField:
    """Flattened schema field definition."""

    path: str
    definition: Any
