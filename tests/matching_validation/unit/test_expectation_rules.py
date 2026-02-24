"""Expectation rule parsing tests."""

from __future__ import annotations

from simple_e2e_tester.matching_validation.expectation_rules import (
    ExpectationRuleKind,
    parse_expectation_rule,
)


def test_parse_none_and_blank_as_ignore() -> None:
    assert parse_expectation_rule(None).kind == ExpectationRuleKind.IGNORE
    assert parse_expectation_rule("   ").kind == ExpectationRuleKind.IGNORE


def test_parse_must_be_empty_token() -> None:
    rule = parse_expectation_rule("MUSS_LEER_SEIN")
    assert rule.kind == ExpectationRuleKind.MUST_BE_EMPTY
    assert rule.expected_value == "MUSS_LEER_SEIN"


def test_parse_tolerance_expression() -> None:
    rule = parse_expectation_rule("3,14+-0,2")
    assert rule.kind == ExpectationRuleKind.TOLERANCE
    assert rule.expected_value == "3,14+-0,2"


def test_parse_exact_string_and_non_string_values() -> None:
    string_rule = parse_expectation_rule("value")
    number_rule = parse_expectation_rule(5)

    assert string_rule.kind == ExpectationRuleKind.EXACT
    assert string_rule.expected_value == "value"
    assert number_rule.kind == ExpectationRuleKind.EXACT
    assert number_rule.expected_value == 5
