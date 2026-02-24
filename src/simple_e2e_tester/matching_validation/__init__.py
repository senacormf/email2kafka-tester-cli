"""Matching and validation domain exports."""

from .case_evaluator import match_and_validate
from .event_boundary_mappers import to_expected_events, to_observed_events
from .matching_outcomes import (
    ExpectedEvent,
    FieldMismatch,
    MatchingConflict,
    MatchValidationResult,
    ObservedEvent,
    ValidatedMatch,
)

__all__ = [
    "ExpectedEvent",
    "ObservedEvent",
    "FieldMismatch",
    "MatchValidationResult",
    "MatchingConflict",
    "ValidatedMatch",
    "to_expected_events",
    "to_observed_events",
    "match_and_validate",
]
