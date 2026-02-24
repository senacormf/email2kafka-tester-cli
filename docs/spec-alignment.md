# Spec Alignment

This document compares current implementation behavior to `application-spec.md`.

Status legend:
- `Implemented`: behavior exists and is covered by tests.
- `Partial`: some but not all required behavior is implemented.
- `Gap`: expected behavior is not implemented.
- `Extension`: additional behavior beyond spec.

## Summary matrix

| Area | Status | Notes |
| --- | --- | --- |
| CLI commands (`generate-template`, `run`) | Implemented | Both modes exist and are test-covered. |
| Config file requirement and parsing | Implemented | YAML/JSON supported; schema/matching validation enforced. |
| Schema flattening and collision checks | Implemented | AVSC + JSON Schema flattening with duplicate-path errors. |
| Template generation (grouped headers + schema sheet) | Implemented | `Metadata/Input/Expected` + `Schema` sheet generated. |
| Template ingestion sanity checks | Partial | IDs, FROM/SUBJECT uniqueness, header/schema alignment are validated; merged-cell shape is not strictly validated. |
| SMTP send concurrency and composition | Implemented | Parallel send and attachment support are implemented. |
| Kafka consume window (>= run start) | Implemented | Timestamp filter exists in consumer service. |
| Kafka decode in run mode for JSON Schema config | Gap | Run mode decode supports AVSC only; JSON Schema fails with explicit error. |
| Stop condition `all matched OR timeout` | Partial | Timeout stop exists; early stop after all matches is not implemented. |
| Output workbook structure (`Actual` + `Match` + RunInfo/Schema) | Implemented | Includes duplicate rows for multiple matches. |
| Validation semantics (`IGNORE`, `MUSS_LEER_SEIN`, float tolerance) | Implemented | Includes German decimal parsing for tolerance expressions. |
| Logging requirements | Gap | Structured summary/conflict logging is minimal currently. |
| Strict CLI contract from spec | Extension | `run --dry-run` exists although not part of spec contract. |

## Key implementation-specific notes

- If `schema.json_schema` is configured, run mode raises a clear Kafka decode error during consumption.
- If `kafka.group_id` is omitted, current code falls back to a fixed consumer group ID.
- Attachment path detection includes a permissive suffix heuristic not explicitly required by the spec.

## Documentation intent

This file is descriptive, not normative.  
Normative product requirements remain in `application-spec.md`.
