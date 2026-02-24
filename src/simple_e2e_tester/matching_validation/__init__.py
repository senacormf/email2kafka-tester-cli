"""Matching and validation domain exports."""

from .case_evaluator import match_and_validate
from .event_boundary_mappers import to_actual_events, to_expected_events
from .matching_outcomes import (
    ActualEvent,
    ExpectedEvent,
    FieldMismatch,
    MatchingConflict,
    MatchValidationResult,
    ValidatedMatch,
)

__all__ = [
    "ExpectedEvent",
    "ActualEvent",
    "FieldMismatch",
    "MatchValidationResult",
    "MatchingConflict",
    "ValidatedMatch",
    "to_expected_events",
    "to_actual_events",
    "match_and_validate",
]
