"""Scenario-style integration tests for core run behaviors."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from openpyxl import load_workbook
from simple_e2e_tester.configuration.runtime_settings import MatchingConfig, SchemaConfig
from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.kafka_consumption.observed_event_messages import ObservedEventMessage
from simple_e2e_tester.matching_validation.case_evaluator import match_and_validate
from simple_e2e_tester.matching_validation.event_boundary_mappers import (
    to_expected_events,
    to_observed_events,
)
from simple_e2e_tester.matching_validation.matching_outcomes import MatchValidationResult
from simple_e2e_tester.results_writing import RunMetadata, write_results_workbook
from simple_e2e_tester.schema_management import flatten_schema, load_schema_document
from simple_e2e_tester.template_generation import TEMPLATE_SHEET_NAME, generate_template_workbook
from simple_e2e_tester.template_ingestion.workbook_reader import read_template


def _schema_config() -> SchemaConfig:
    return SchemaConfig(
        schema_type="json_schema",
        text=json.dumps(
            {
                "type": "object",
                "properties": {
                    "sender": {"type": "string"},
                    "subject": {"type": "string"},
                    "score": {"type": "number"},
                },
            }
        ),
        source_path=None,
    )


def _kafka_message(*, sender: str, subject: str, score: float) -> ObservedEventMessage:
    flattened = {
        "sender": sender,
        "subject": subject,
        "score": score,
    }
    return ObservedEventMessage(
        key=None,
        value=flattened,
        timestamp=datetime.now(UTC),
        flattened=flattened,
    )


def _write_case_sheet(
    tmp_path: Path,
    *,
    first_subject: str,
    second_subject: str | None = None,
    expected_score: str = "1,50+-0,1",
) -> tuple[Path, list]:
    schema_config = _schema_config()
    fields = flatten_schema(load_schema_document(schema_config))
    template_path = tmp_path / "cases.xlsx"
    generate_template_workbook(schema_config, fields, template_path)

    workbook = load_workbook(template_path)
    sheet = workbook[TEMPLATE_SHEET_NAME]
    header_map = {
        sheet.cell(row=2, column=col).value: col for col in range(1, sheet.max_column + 1)
    }

    sheet.cell(row=3, column=header_map["ID"]).value = "TC-1"
    sheet.cell(row=3, column=header_map["FROM"]).value = "sender@example.com"
    sheet.cell(row=3, column=header_map["SUBJECT"]).value = first_subject
    sheet.cell(row=3, column=header_map["score"]).value = expected_score

    if second_subject is not None:
        sheet.cell(row=4, column=header_map["ID"]).value = "TC-2"
        sheet.cell(row=4, column=header_map["FROM"]).value = "sender@example.com"
        sheet.cell(row=4, column=header_map["SUBJECT"]).value = second_subject
        sheet.cell(row=4, column=header_map["score"]).value = expected_score

    workbook.save(template_path)
    return template_path, fields


def test_given_enabled_expected_event_when_observed_event_matches_then_case_is_ok(
    tmp_path: Path,
) -> None:
    schema_config = _schema_config()
    template_path, fields = _write_case_sheet(tmp_path, first_subject="Subject-A")
    testcases = read_template(template_path, [field.path for field in fields]).testcases

    result = match_and_validate(
        to_expected_events(testcases),
        to_observed_events(
            [_kafka_message(sender="sender@example.com", subject="Subject-A", score=1.55)]
        ),
        MatchingConfig(from_field="sender", subject_field="subject"),
        fields,
    )

    assert len(result.matches) == 1
    assert result.matches[0].expected_event.expected_event_id == "TC-1"
    assert result.matches[0].mismatches == ()
    assert result.conflicts == ()
    assert result.unmatched_expected_event_ids == ()
    assert schema_config.schema_type == "json_schema"


def test_given_sender_collision_when_subject_matches_one_case_then_single_case_is_selected(
    tmp_path: Path,
) -> None:
    template_path, fields = _write_case_sheet(
        tmp_path,
        first_subject="Subject-A",
        second_subject="Subject-B",
    )
    testcases = read_template(template_path, [field.path for field in fields]).testcases

    result = match_and_validate(
        to_expected_events(testcases),
        to_observed_events(
            [_kafka_message(sender="sender@example.com", subject="Subject-B", score=1.5)]
        ),
        MatchingConfig(from_field="sender", subject_field="subject"),
        fields,
    )

    assert len(result.matches) == 1
    assert result.matches[0].expected_event.expected_event_id == "TC-2"
    assert result.conflicts == ()
    assert result.unmatched_expected_event_ids == ("TC-1",)


def test_given_send_failure_when_writing_run_report_then_status_is_send_failed(
    tmp_path: Path,
) -> None:
    schema_config = _schema_config()
    template_path, fields = _write_case_sheet(tmp_path, first_subject="Subject-A")
    testcases = read_template(template_path, [field.path for field in fields]).testcases
    output_path = tmp_path / "results.xlsx"

    write_results_workbook(
        template_path=template_path,
        output_path=output_path,
        schema_config=schema_config,
        schema_fields=fields,
        testcases=testcases,
        match_result=MatchValidationResult(
            matches=(),
            conflicts=(),
            unmatched_observed_events=(),
            unmatched_expected_event_ids=("TC-1",),
        ),
        run_metadata=RunMetadata(
            run_start=datetime(2026, 2, 23, 18, 0, tzinfo=UTC),
            input_path=template_path,
            output_path=output_path,
            kafka_topic="topic-a",
            timeout_seconds=600,
            sent_ok=0,
        ),
        send_status_by_test_id={"TC-1": SendStatus.FAILED},
    )

    workbook = load_workbook(output_path)
    sheet = workbook[TEMPLATE_SHEET_NAME]
    assert sheet.cell(row=3, column=sheet.max_column).value == "SEND_FAILED"

    run_info_sheet = workbook["RunInfo"]
    run_info = {
        run_info_sheet.cell(row=row, column=1).value: run_info_sheet.cell(row=row, column=2).value
        for row in range(1, 20)
    }
    assert run_info["passed"] == 0
    assert run_info["failed"] == 1


def test_given_tolerance_expression_when_actual_value_crosses_boundaries_then_only_in_range_is_ok(
    tmp_path: Path,
) -> None:
    template_path, fields = _write_case_sheet(
        tmp_path,
        first_subject="Subject-A",
        expected_score="3,14+-0,2",
    )
    testcases = read_template(template_path, [field.path for field in fields]).testcases

    in_range_result = match_and_validate(
        to_expected_events(testcases),
        to_observed_events(
            [_kafka_message(sender="sender@example.com", subject="Subject-A", score=3.30)]
        ),
        MatchingConfig(from_field="sender", subject_field="subject"),
        fields,
    )
    out_of_range_result = match_and_validate(
        to_expected_events(testcases),
        to_observed_events(
            [_kafka_message(sender="sender@example.com", subject="Subject-A", score=3.50)]
        ),
        MatchingConfig(from_field="sender", subject_field="subject"),
        fields,
    )

    assert in_range_result.matches[0].mismatches == ()
    assert len(out_of_range_result.matches[0].mismatches) == 1
