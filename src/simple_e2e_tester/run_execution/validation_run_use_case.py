"""Run execution use-case service."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from simple_e2e_tester.configuration import ConfigurationError, load_configuration
from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.email_sending.email_dispatch import (
    EmailCompositionError,
    ExpectedEventDispatcher,
    SynchronousSMTPClient,
    validate_attachments_for_testcases,
)
from simple_e2e_tester.kafka_consumption.observed_event_reader import (
    ObservedEventDecodeError,
    ObservedEventReader,
)
from simple_e2e_tester.matching_validation import match_and_validate
from simple_e2e_tester.matching_validation.event_boundary_mappers import (
    to_expected_events,
    to_observed_events,
)
from simple_e2e_tester.matching_validation.matching_outcomes import MatchValidationResult
from simple_e2e_tester.results_writing import RunMetadata, write_results_workbook
from simple_e2e_tester.schema_management import SchemaError, flatten_schema, load_schema_document
from simple_e2e_tester.template_ingestion.workbook_reader import (
    TemplateValidationError,
    read_template,
)

from .run_contracts import RunArtifacts, RunOutcome, RunRequest


class RunExecutionError(Exception):
    """Raised when a run use case cannot be completed."""


@dataclass(frozen=True)
class _RunExecution:
    """Execution details used to write run outputs."""

    send_status_by_test_id: dict[str, SendStatus]
    sent_ok: int
    match_result: MatchValidationResult


def execute_email_kafka_validation_run(
    request: RunRequest,
    *,
    email_sender_cls=None,
    kafka_service_cls=None,
    smtp_client_factory: Callable[[], SynchronousSMTPClient] | None = None,
) -> RunOutcome:
    """Execute one full email-kafka validation run and return run outcome."""
    resolved_email_sender_cls = email_sender_cls or ExpectedEventDispatcher
    resolved_kafka_service_cls = kafka_service_cls or ObservedEventReader
    resolved_smtp_client_factory = smtp_client_factory or SynchronousSMTPClient

    artifacts = _load_run_artifacts(request.config_path, request.input_path)
    kafka_service = _prepare_kafka_service(
        artifacts=artifacts,
        dry_run=request.dry_run,
        kafka_service_cls=resolved_kafka_service_cls,
    )
    run_start = datetime.now(UTC)
    execution = (
        _execute_dry_run(artifacts.testcases)
        if request.dry_run
        else _execute_live_run(
            artifacts=artifacts,
            run_start=run_start,
            kafka_service=kafka_service,
            email_sender_cls=resolved_email_sender_cls,
            smtp_client_factory=resolved_smtp_client_factory,
        )
    )

    output_path = _resolve_output_path(request.input_path, request.output_dir)
    run_metadata = RunMetadata(
        run_start=run_start,
        input_path=Path(request.input_path).resolve(),
        output_path=output_path.resolve(),
        kafka_topic=artifacts.configuration.kafka.topic,
        timeout_seconds=artifacts.configuration.kafka.timeout_seconds,
        sent_ok=execution.sent_ok,
    )
    write_results_workbook(
        template_path=request.input_path,
        output_path=output_path,
        schema_config=artifacts.configuration.schema,
        schema_fields=artifacts.fields,
        testcases=artifacts.testcases,
        match_result=execution.match_result,
        run_metadata=run_metadata,
        send_status_by_test_id=execution.send_status_by_test_id,
    )
    return RunOutcome(
        output_path=output_path.resolve(),
        sent_ok=execution.sent_ok,
        dry_run=request.dry_run,
    )


def _resolve_output_path(input_path: str, output_dir: str | None) -> Path:
    input_file = Path(input_path)
    destination = Path(output_dir) if output_dir else input_file.parent
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return destination / f"{input_file.stem}-results-{timestamp}.xlsx"


def _validate_run_mode_schema(schema_type: str) -> None:
    if schema_type != "avsc":
        raise ValueError("Run mode requires schema.avsc for Kafka decoding.")


def _load_run_artifacts(config_path: str, input_path: str) -> RunArtifacts:
    try:
        configuration = load_configuration(config_path)
        fields = flatten_schema(load_schema_document(configuration.schema))
        testcases = read_template(input_path, [field.path for field in fields]).testcases
    except (ConfigurationError, SchemaError, TemplateValidationError, OSError, ValueError) as exc:
        raise RunExecutionError(str(exc)) from exc
    return RunArtifacts(
        configuration=configuration,
        fields=tuple(fields),
        testcases=testcases,
        attachments_base=Path(input_path).resolve().parent,
    )


def _prepare_kafka_service(
    *,
    artifacts: RunArtifacts,
    dry_run: bool,
    kafka_service_cls,
):
    if dry_run:
        return None
    try:
        _validate_run_mode_schema(artifacts.configuration.schema.schema_type)
        validate_attachments_for_testcases(
            artifacts.testcases,
            attachments_base=artifacts.attachments_base,
        )
        return kafka_service_cls(
            kafka_settings=artifacts.configuration.kafka,
            schema_fields=artifacts.fields,
            schema_config=artifacts.configuration.schema,
        )
    except (EmailCompositionError, ObservedEventDecodeError, ValueError) as exc:
        raise RunExecutionError(str(exc)) from exc


def _execute_dry_run(testcases) -> _RunExecution:
    send_status_by_test_id = {
        testcase.test_id: SendStatus.SKIPPED for testcase in testcases if testcase.enabled
    }
    expected_events = to_expected_events(testcases)
    match_result = MatchValidationResult(
        matches=(),
        conflicts=(),
        unmatched_observed_events=(),
        unmatched_expected_event_ids=tuple(
            expected_event.expected_event_id
            for expected_event in expected_events
            if expected_event.enabled
        ),
    )
    return _RunExecution(
        send_status_by_test_id=send_status_by_test_id,
        sent_ok=0,
        match_result=match_result,
    )


def _execute_live_run(
    *,
    artifacts: RunArtifacts,
    run_start: datetime,
    kafka_service,
    email_sender_cls,
    smtp_client_factory: Callable[[], SynchronousSMTPClient],
) -> _RunExecution:
    sender = email_sender_cls(
        smtp_client=smtp_client_factory(),
        smtp_settings=artifacts.configuration.smtp,
        mail_settings=artifacts.configuration.mail,
        attachments_base=artifacts.attachments_base,
    )
    kafka_future: Future[list] | None = None
    with ThreadPoolExecutor(max_workers=1) as executor:
        if kafka_service:
            kafka_future = executor.submit(_read_observed_event_messages, kafka_service, run_start)
        send_results = sender.send_all(artifacts.testcases)
        kafka_messages = kafka_future.result() if kafka_future else []

    send_status_by_test_id = {result.test_id: result.status for result in send_results}
    sent_ok = sum(1 for result in send_results if result.status == SendStatus.SENT)
    sent_testcases = [
        testcase
        for testcase in artifacts.testcases
        if send_status_by_test_id.get(testcase.test_id) == SendStatus.SENT
    ]
    expected_events = to_expected_events(sent_testcases)
    observed_events = to_observed_events(kafka_messages)
    match_result = match_and_validate(
        expected_events,
        observed_events,
        artifacts.configuration.matching,
        artifacts.fields,
    )
    return _RunExecution(
        send_status_by_test_id=send_status_by_test_id,
        sent_ok=sent_ok,
        match_result=match_result,
    )


def _read_observed_event_messages(kafka_service, run_start: datetime) -> list:
    return list(kafka_service.consume_from(run_start))
