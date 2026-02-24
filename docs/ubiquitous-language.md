# Ubiquitous Language

This glossary defines canonical terms used across configuration, template workbooks,
runtime behavior, and reporting.

## Canonical Terms

| Canonical term | Meaning |
| --- | --- |
| sender | The identity used to correlate a sent test email with an observed result event. |
| subject | The subject value used to disambiguate candidate matches for the same sender. |
| expected event | A normalized expected event derived from a workbook row, including expected field values. |
| observed event | A normalized event derived from Kafka payload data for matching and validation. |
| matching field path | A schema-flattened field path used to extract sender/subject for matching. |

## Boundary Mapping

| Boundary | External name | Canonical term |
| --- | --- | --- |
| Workbook input | `FROM` | sender |
| Workbook input | `SUBJECT` | subject |
| Configuration | `matching.from_field` | sender matching field path |
| Configuration | `matching.subject_field` | subject matching field path |
| Kafka payload | configured schema path value | observed sender / observed subject |

## Notes

- Canonical terms are used in domain-facing APIs.
- External labels remain unchanged at adapters and IO boundaries.
