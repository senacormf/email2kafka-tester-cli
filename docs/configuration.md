# Configuration Reference

`--config` is mandatory for both CLI modes. The test configuration supports YAML and JSON.

## Scaffold a test configuration

Use `generate-config` to create a commented YAML test configuration template with placeholders:

```bash
python e2k-tester generate-config --output ./config.yaml
```

The generated template includes all supported fields with `<REQUIRED>` / `<OPTIONAL>` placeholders.
`generate-config` fails if the output file already exists.

## Top-level keys

| Key | Required | Description |
| --- | --- | --- |
| `schema` | Yes | Event schema source and type (`avsc` or `json_schema`). |
| `matching` | Yes | Field names used to match Kafka records to test case rows. |
| `smtp` | Yes | SMTP connection and parallel sending settings. |
| `mail` | Yes | Destination mailbox settings. |
| `kafka` | Yes | Kafka topic/consumer settings. |

## `schema`

Exactly one event schema type is allowed:
- `schema.avsc`
- `schema.json_schema`

For the chosen event schema type, define exactly one source:
- `inline`: event schema JSON string
- `path`: file path to event schema JSON

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

Both values are required and must reference flattened event schema paths.

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
| `parallelism` | No | `4` | Concurrent sends |

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

Run mode Kafka decoding supports both configured schema types:
- `schema.avsc`: AVSC binary decode (with Confluent wire header support)
- `schema.json_schema`: UTF-8 JSON object payload decode (with Confluent wire header support)
