"""Results writing entities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path


class MatchStatus(str, Enum):
    """Rendered status in output workbook match column."""

    OK = "OK"
    SEND_FAILED = "SEND_FAILED"
    SKIPPED = "SKIPPED"
    CONFLICT = "CONFLICT"
    NOT_FOUND = "NOT_FOUND"


@dataclass(frozen=True)
class RunMetadata:
    """Metadata rendered into the RunInfo sheet."""

    run_start: datetime
    input_path: Path
    output_path: Path
    kafka_topic: str
    timeout_seconds: int
    sent_ok: int
