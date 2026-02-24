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
class ObservedEvent:
    """Observed event derived from Kafka message payload data."""

    flattened: Mapping[str, object]


@dataclass(frozen=True)
class FieldMismatch:
    """Difference between expected and actual value for one schema field."""

    field: str
    expected: str
    actual: str


@dataclass(frozen=True)
class ValidatedMatch:
    """Represents one observed event evaluated against one expected event."""

    expected_event: ExpectedEvent
    observed_event: ObservedEvent
    mismatches: tuple[FieldMismatch, ...]

    @property
    def is_ok(self) -> bool:
        """Return True when no mismatches are present."""
        return not self.mismatches


@dataclass(frozen=True)
class MatchingConflict:
    """Observed event could not be resolved to exactly one expected event."""

    observed_event: ObservedEvent
    candidate_expected_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class MatchValidationResult:
    """Outcome of matching and validation over a batch of observed events."""

    matches: tuple[ValidatedMatch, ...]
    conflicts: tuple[MatchingConflict, ...]
    unmatched_observed_events: tuple[ObservedEvent, ...]
    unmatched_expected_event_ids: tuple[str, ...]
