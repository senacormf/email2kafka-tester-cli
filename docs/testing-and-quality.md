# Testing and Quality

The repository follows a TDD-oriented workflow and domain-focused test layout from `AGENTS.md`.

## Test layout

```text
tests/
  <domain>/
    unit/
    integration/
```

Current domains with tests include:
- `cli`
- `configuration`
- `schema_management`
- `template_generation`
- `template_ingestion`
- `email_sending`
- `kafka_consumption`
- `matching_validation`
- `results_writing`

## Standard commands

Run all tests:

```bash
./scripts/uvrun pytest -q
```

Run domain-focused suites:

```bash
./scripts/uvrun pytest tests/<domain>/unit -q
./scripts/uvrun pytest tests/<domain>/integration -q
```

## Recommended quality gates

```bash
./scripts/uvrun ruff check .
./scripts/uvrun mypy src tests
./scripts/uvrun pylint src/simple_e2e_tester
./scripts/uvrun pytest -q
```

## Latest local verification during this documentation pass

- `./scripts/uvrun pytest -q` passed.
