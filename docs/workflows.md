# CLI Workflows

## Commands

## `generate-template`

```bash
python -m simple_e2e_tester generate-template --config <config> --output <template.xlsx>
```

Behavior:
- Loads and validates config.
- Flattens schema fields.
- Writes template workbook with sheets:
  - `TestCases`
  - `Schema`

## `run`

```bash
python -m simple_e2e_tester run --config <config> --input <filled-template.xlsx> [--output-dir <dir>]
```

Extension (non-spec):

```bash
python -m simple_e2e_tester run --config <config> --input <filled-template.xlsx> --dry-run
```

Behavior:
- Reads and validates input workbook.
- Sends enabled testcase emails (`--dry-run` skips SMTP/Kafka).
- Consumes Kafka records at/after run start time.
- Matches observed events to expected events and validates values.
- Writes results workbook named:
  - `<input_stem>-results-<YYYYMMDD-HHMMSS>.xlsx`

## Workbook contract

## Input template (`TestCases` sheet)

Row 1 group headers:
- `Metadata`
- `Input`
- `Expected`

Row 2 column headers:
- Metadata: `ID`, `Tags`, `Enabled`, `Notes`
- Input: `FROM`, `SUBJECT`, `BODY`, `ATTACHMENT`
- Expected: flattened schema field names

## Result workbook (`TestCases` sheet)

Adds:
- `Actual` group with same schema-derived field list as `Expected`
- `Match` group with one `Match` column

Also includes:
- `Schema` sheet: schema type/hash/text
- `RunInfo` sheet: run metadata and counts

## Attachment handling

`ATTACHMENT` cell is parsed line-by-line:
- File path mode: first non-empty line starts with a path prefix (`./`, `/`, `.\\`, `C:\`).
- Text-to-PDF mode: otherwise, all text is rendered as a generated PDF attachment.

## Matching and validation rules

- Candidate selection:
  - First by sender (`matching.from_field`).
  - If multiple candidates, disambiguate by subject (`matching.subject_field`).
  - If unresolved, mark conflict.
- Expected cell semantics:
  - Empty -> ignored.
  - `MUSS_LEER_SEIN` -> actual must be empty/null.
  - Float tolerance expressions supported:
    - `v+-t`
    - `v+t`
    - `v-t`
  - Decimal comma is supported (for example `3,14+-0,2`).
- Match output:
  - `OK` if no mismatches.
  - Otherwise newline-separated blocks:
    - `expected: <value>`
    - `actual: <value>`

## Row status values in `Match`

- `OK`: matched and validation passed.
- mismatch text: matched with validation differences.
- `NOT_FOUND`: enabled testcase not matched.
- `CONFLICT`: ambiguous message-to-testcase matching.
- `SEND_FAILED`: SMTP send failed.
- `SKIPPED`: disabled row or dry-run skipped send.
