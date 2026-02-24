# simple-e2e-tester

Schema-driven E2E test runner for validating an `Email -> Kafka` flow.

It provides two CLI modes:
- `generate-template`: build an Excel input template from an AVSC or JSON Schema.
- `run`: execute test rows, send emails via SMTP, consume Kafka results, validate expected vs actual values, and write a result workbook.

The implementation follows a domain-first structure and TDD workflow as defined in `AGENTS.md`.

## Current implementation status

Implemented and verified by tests:
- Configuration loading and validation (YAML/JSON).
- Schema flattening for AVSC and JSON Schema.
- Template generation (`Metadata`, `Input`, `Expected`) plus `Schema` sheet.
- Template ingestion and sanity checks (IDs, sender/subject uniqueness, email format, schema column alignment).
- SMTP email composition/sending with concurrency and attachment handling.
- Kafka consumption with Avro binary decoding (including Confluent wire header).
- Matching and validation rules (`IGNORE`, `MUSS_LEER_SEIN`, float tolerance syntax like `3,14+-0,2`) over expected/observed events.
- Result workbook writing (`Actual`, `Match`, `RunInfo`, `Schema`).

Known behavior to be aware of:
- `run` currently supports Kafka decode for `avsc` schemas only. `json_schema` in run mode raises a clear decode error.
- The CLI currently exposes a non-spec extension: `run --dry-run`.

See `/docs/spec-alignment.md` for a precise spec-vs-implementation map.

## Quick start

1. Install dependencies:
```bash
.venv/bin/uv sync --all-groups
```

All runtime and quality commands below use the repository wrapper `./scripts/uvrun`.

2. Generate a template:
```bash
./scripts/uvrun python -m simple_e2e_tester generate-template \
  --config /path/to/config.yaml \
  --output /path/to/template.xlsx
```

3. Fill test rows in the generated workbook (`TestCases` sheet).

4. Run the test execution:
```bash
./scripts/uvrun python -m simple_e2e_tester run \
  --config /path/to/config.yaml \
  --input /path/to/template.xlsx \
  --output-dir /path/to/results
```

Optional smoke mode:
```bash
./scripts/uvrun python -m simple_e2e_tester run \
  --config /path/to/config.yaml \
  --input /path/to/template.xlsx \
  --dry-run
```

## Documentation

- Overview and navigation: `docs/docs-overview.md`
- Architecture and domain boundaries: `docs/architecture.md`
- Configuration reference: `docs/configuration.md`
- CLI workflow and workbook semantics: `docs/workflows.md`
- Spec alignment and known gaps: `docs/spec-alignment.md`
- Testing and quality gates: `docs/testing-and-quality.md`
- Authoritative requirement baseline: `application-spec.md`
- Development process and constraints: `AGENTS.md`

## Repository layout

```text
src/simple_e2e_tester/
  configuration/
  schema_management/
  template_generation/
  template_ingestion/
  email_sending/
  kafka_consumption/
  run_execution/
  matching_validation/
  results_writing/
  cli.py
tests/
  <domain>/{unit,integration}
samples/
docs/
```
