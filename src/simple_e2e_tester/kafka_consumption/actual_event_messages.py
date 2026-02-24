"""Kafka consumption entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ActualEventMessage:
    """Decoded Kafka message ready for matching/validation."""

    key: str | None
    value: Mapping[str, Any]
    timestamp: datetime
    flattened: Mapping[str, Any]
