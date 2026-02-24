# Architecture

## Design style

The project is organized by business domain (DDD-style), aligned with `AGENTS.md`.
Each domain owns its entities, services, and tests.

## Domain map

| Domain | Responsibility | Key entry points |
| --- | --- | --- |
| `configuration` | Parse and validate YAML/JSON config files. | `load_configuration` |
| `schema_management` | Parse AVSC/JSON Schema and flatten field paths. | `load_schema_document`, `flatten_schema` |
| `template_generation` | Generate input Excel templates from schema fields. | `generate_template_workbook` |
| `template_ingestion` | Read and validate filled templates into test case objects. | `read_template` |
| `email_sending` | Compose and send emails via SMTP (parallelized). | `compose_email`, `EmailSender` |
| `kafka_consumption` | Consume Kafka records from run start and decode Avro payloads. | `KafkaConsumerService.consume_from` |
| `matching_validation` | Match observed events to expected events and validate expected values. | `match_and_validate` |
| `results_writing` | Produce output workbook with actuals, match result, and run metadata. | `write_results_workbook` |
| `run_execution` | Execute the run use case from request to output workbook. | `execute_email_kafka_validation_run` |
| `cli` | Expose command-line adapters for `generate-template` and `run`. | `cli` group commands |

## Runtime flows

## `generate-template`

1. Load config.
2. Parse and flatten schema.
3. Generate workbook with:
   - `TestCases` sheet: grouped headers `Metadata | Input | Expected`
   - `Schema` sheet: `schema_type`, `schema_hash`, `schema_text`

## `run`

1. Load config and flatten schema.
2. Read/validate input workbook.
3. Record `run_start` timestamp.
4. Send enabled testcases via SMTP (parallelism from config).
5. Consume Kafka records with timestamp `>= run_start`.
6. Match/validate records against testcases.
7. Write output workbook:
   - Original columns + `Actual` + `Match`
   - `Schema` and `RunInfo` sheets

## External boundaries

- SMTP server (`smtplib`) for sending test emails.
- Kafka broker (`confluent-kafka`) for reading produced events.
- Excel IO (`openpyxl`) for template/result workbooks.

## Architectural invariants currently enforced

- Exactly one schema type must be configured (`avsc` or `json_schema`).
- Matching fields (`matching.from_field`, `matching.subject_field`) must exist in flattened schema.
- Template column order must match generated schema-derived fields.
- IDs must be unique; enabled rows must have unique `(FROM, SUBJECT)` pairs.
