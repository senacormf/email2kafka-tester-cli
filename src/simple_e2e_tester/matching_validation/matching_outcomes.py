"""Matching and validation domain entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class ExpectedEvent:
    """Expected event derived from template/workbook data."""

    expected_event_id: str
    enabled: bool
    sender: str
    subject: str
    expected_values: Mapping[str, object]


@dataclass(frozen=True)
class ActualEvent:
    """Actual event derived from Kafka message payload data."""

    flattened: Mapping[str, object]


@dataclass(frozen=True)
class FieldMismatch:
    """Difference between expected and actual value for one schema field."""

    field: str
    expected: str
    actual: str


@dataclass(frozen=True)
class ValidatedMatch:
    """Represents one actual event evaluated against one expected event."""

    expected_event: ExpectedEvent
    actual_event: ActualEvent
    mismatches: tuple[FieldMismatch, ...]

    @property
    def is_ok(self) -> bool:
        """Return True when no mismatches are present."""
        return not self.mismatches


@dataclass(frozen=True)
class MatchingConflict:
    """Actual event could not be resolved to exactly one expected event."""

    actual_event: ActualEvent
    candidate_expected_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class MatchValidationResult:
    """Outcome of matching and validation over a batch of actual events."""

    matches: tuple[ValidatedMatch, ...]
    conflicts: tuple[MatchingConflict, ...]
    unmatched_actual_events: tuple[ActualEvent, ...]
    unmatched_expected_event_ids: tuple[str, ...]
