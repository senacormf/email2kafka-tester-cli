# Configuration Reference

`--config` is mandatory for both CLI modes. YAML and JSON are supported.

## Top-level keys

| Key | Required | Description |
| --- | --- | --- |
| `schema` | Yes | Schema source and type (`avsc` or `json_schema`). |
| `matching` | Yes | Field names used to match Kafka records to testcase rows. |
| `smtp` | Yes | SMTP connection and parallel sending settings. |
| `mail` | Yes | Destination mailbox settings. |
| `kafka` | Yes | Kafka topic/consumer settings. |

## `schema`

Exactly one schema type is allowed:
- `schema.avsc`
- `schema.json_schema`

For the chosen schema type, define exactly one source:
- `inline`: schema JSON string
- `path`: file path to schema JSON

### Example (AVSC via path)

```yaml
schema:
  avsc:
    path: ./schemas/result.avsc
```

### Example (JSON Schema inline)

```yaml
schema:
  json_schema:
    inline: |
      {
        "type": "object",
        "properties": {
          "sender": {"type": "string"},
          "subject": {"type": "string"}
        }
      }
```

## `matching`

Both values are required and must reference flattened schema paths.

```yaml
matching:
  from_field: sender
  subject_field: subject
```

Nested example:

```yaml
matching:
  from_field: envelope.from
  subject_field: envelope.subject
```

## `smtp`

| Field | Required | Default | Notes |
| --- | --- | --- | --- |
| `host` | Yes | - | SMTP host |
| `port` | Yes | - | SMTP port |
| `username` | No | `null` | Optional auth user |
| `password` | No | `null` | Optional auth password |
| `use_ssl` | No | `false` | Use SMTPS |
| `use_starttls` | No | `not use_ssl` | STARTTLS when SSL is off |
| `timeout_seconds` | No | `30` | SMTP timeout |
| `parallelism` | No | `8` | Concurrent sends |

## `mail`

| Field | Required | Default |
| --- | --- | --- |
| `to_address` | Yes | - |
| `cc` | No | empty list |
| `bcc` | No | empty list |

## `kafka`

| Field | Required | Default | Notes |
| --- | --- | --- | --- |
| `bootstrap_servers` | Yes | - | String (`"a:9092,b:9092"`) or string list |
| `topic` | Yes | - | Topic to consume |
| `group_id` | No | `null` | If omitted, current code uses fixed fallback |
| `security` | No | `{}` | Passed through to `confluent-kafka` consumer config |
| `timeout_seconds` | No | `600` | Global consume timeout |
| `poll_interval_ms` | No | `500` | Poll interval |
| `auto_offset_reset` | No | `"latest"` | Lower-cased by loader |

## Minimal runnable config (AVSC run mode)

```yaml
schema:
  avsc:
    path: ./samples/sample-avsc-schema.json
matching:
  from_field: sender_address
  subject_field: message_subject
smtp:
  host: smtp.example.com
  port: 587
mail:
  to_address: qa@example.com
kafka:
  bootstrap_servers: localhost:9092
  topic: result-topic
```

## Important runtime note

Run mode Kafka decoding currently supports `avsc` only.  
When `schema.json_schema` is configured, run mode fails with a clear decode error.
