"""Adapters from external boundary models to matching-domain models."""

from __future__ import annotations

from collections.abc import Sequence

from simple_e2e_tester.kafka_consumption.observed_event_messages import ObservedEventMessage
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase

from .matching_outcomes import ExpectedEvent, ObservedEvent


def to_expected_events(testcases: Sequence[TemplateTestCase]) -> tuple[ExpectedEvent, ...]:
    """Convert template testcases into expected matching events."""
    return tuple(
        ExpectedEvent(
            expected_event_id=testcase.test_id,
            enabled=testcase.enabled,
            sender=testcase.from_address,
            subject=testcase.subject,
            expected_values=testcase.expected_values,
        )
        for testcase in testcases
    )


def to_observed_events(messages: Sequence[ObservedEventMessage]) -> tuple[ObservedEvent, ...]:
    """Convert Kafka messages into observed matching events."""
    return tuple(ObservedEvent(flattened=message.flattened) for message in messages)
