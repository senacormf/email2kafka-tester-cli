Program specification: Schema-driven E2E Email → Kafka validation runner

1. Purpose

Implement a script that supports two modes:
	1.	Generate template: read a required config file containing either an AVSC or JSON-Schema (inline or as path), derive all output fields from the schema, and generate a German-flavor Excel input template where testers fill in email inputs and expected values.
	2.	Run tests: read a filled input template, send emails via SMTP concurrently, consume Kafka results known to be produced after the run start time, match results to test cases, extract actual values from messages using the schema, validate expected vs actual, and write a new output Excel per run. Output Excel preserves grouped/merged header layout and includes embedded schema in a separate sheet.

Non-goals:
	•	No OCR in the script.
	•	No CLI flags for individual config values; a config file path is mandatory.

⸻

2. High-level flow

2.1 Template generation mode
	1.	Load config.
	2.	Load schema (exactly one of AVSC or JSON-Schema must be provided).
	3.	Derive flattened field list from schema (full schema).
	4.	Generate an .xlsx template with grouped headers:
	•	Metadata
	•	Input
	•	Expected (schema-derived columns)
	•	(No actual/match columns in the input template; optional — see 6.1)
	5.	Write schema text + hash into a Schema sheet.

2.2 Test run mode
	1.	Load config.
	2.	Load schema (exactly one, as above).
	3.	Read input .xlsx.
	4.	Sanity checks (Excel structure, IDs uniqueness, sender+subject uniqueness, schema compatibility, attachment validity).
	5.	Record run_start_time.
	6.	Send all emails via SMTP concurrently (default 4 parallel senders, configurable).
	7.	Consume Kafka from timestamps >= run_start_time until:
	•	all test cases matched and validated, or
	•	global timeout reached (default 10 minutes).
	8.	Write output .xlsx:
	•	includes all original columns
	•	adds “Actual” group columns (same schema-derived field list)
	•	adds a single “Match” group column (aggregated mismatches)
	•	preserves merged header layout and formatting
	•	embeds schema + hash in Schema sheet, plus run metadata in RunInfo sheet

⸻

3. Configuration

3.1 Config file required
	•	Script must require --config <path> (or equivalent positional arg).
	•	No CLI flags for config values other than the config file and the mode (generate vs run) and input/output paths.

3.2 Config format
	•	YAML and JSON must be supported.

3.3 Schema specification (exactly one)

Config must contain exactly one of:

Option A: AVSC
	•	schema.avsc.inline (string) OR schema.avsc.path (file path)

Option B: JSON-Schema
	•	schema.json_schema.inline (string) OR schema.json_schema.path (file path)

Rules:
	•	If both AVSC and JSON-Schema are provided → error.
	•	If none provided → error.
	•	If both inline and path are provided for the same schema type → error.
	•	Inline schema may be multi-line.

3.4 Matching field configuration (mandatory)
	•	matching.from_field (string) — name of schema field in Kafka message containing sender
	•	matching.subject_field (string) — name of schema field in Kafka message containing subject

Rules:
	•	Both must be present in config.
	•	Both must exist in the schema-derived flattened field set; otherwise → error.

3.5 SMTP
	•	smtp.host (string)
	•	smtp.port (int)
	•	smtp.username (string, optional)
	•	smtp.password (string, optional)
	•	smtp.use_starttls (bool, default true if not using SSL)
	•	smtp.use_ssl (bool, default false)
	•	smtp.timeout_seconds (int, default 30)
	•	smtp.parallelism (int, default 4)

3.6 Mail destination
	•	mail.to_address (string) — destination inbox/address monitored by the system
	•	optional: mail.cc (list), mail.bcc (list)

3.7 Kafka
	•	kafka.bootstrap_servers (string or list)
	•	kafka.topic (string)
	•	kafka.security (object; optional; supports SASL/SSL as needed)
	•	kafka.group_id (string; optional). If absent, generate unique per run.
	•	kafka.timeout_seconds (int, default 600)
	•	kafka.poll_interval_ms (int, default 500)
	•	kafka.auto_offset_reset (string, default latest)

Consumption rule:
	•	Only consider messages with Kafka timestamp >= run_start_time.

⸻

4. Schema → flattened fields

4.1 Flattening rules
	•	Use the full schema as expected output definition.
	•	Flatten nested objects into dot notation:
	•	Example: customer.address.zip
	•	Arrays and maps:
	•	Do not create multiple columns.
	•	Represent the entire value as one JSON string in a single column for that field.
	•	Collision policy:
	•	If flattening produces duplicate column names → error listing the colliding schema paths.

4.2 Field order
	•	Deterministic depth-first traversal, preserving schema field order where applicable (AVSC fields order; JSON-schema properties iteration order if preserved by parser; if not, sort keys lexicographically).

4.3 Types and Excel representation
	•	Integers: Excel numeric cell.
	•	Floats/doubles/decimal-like: Excel numeric cell.
	•	Booleans: Excel boolean cell.
	•	Strings/enums: Excel text cell.
	•	Nullables/unions:
	•	Actual values may be missing/null; treat as empty for comparison by default (see 9).
	•	Arrays/maps/objects that become JSON string: Excel text cell.

⸻

5. Excel templates

5.1 Visual layout

All generated and produced files must use grouped headers with merged cells in row 1:

Row 1 (merged across group columns):
	•	Metadata | Input | Expected | Actual | Match

Row 2:
	•	actual column headers (no prefixes).

Rule for repeating schema fields:
	•	Under Expected: see schema-derived flattened column names.
	•	Under Actual: repeat the same column names, same order.
	•	Under Match: a single column (name configurable; default Match).

5.2 Metadata columns (included in template)

Under Metadata group:
	•	ID (required; unique)
	•	Tags (optional; not used for execution; included for user filtering)
	•	Optional recommended (implement as fixed columns in template):
	•	Enabled (boolean; default TRUE; if FALSE skip test row)
	•	Notes (free text)

5.3 Input columns (included in template)

Under Input group:
	•	FROM (required; validated)
	•	SUBJECT (required)
	•	BODY (optional)
	•	ATTACHMENT (optional)

5.4 Template generation output

Generated template file contains:
	•	Metadata group columns
	•	Input group columns
	•	Expected group columns (derived from schema)
	•	No actual/match columns required in the input template (recommended), but allowed if you want a single “master template”.
Decision: generate template with Expected only (no Actual/Match). In run mode, output adds Actual/Match.

5.5 Output file (test run)

Output .xlsx must contain:
	•	all columns from the input template unchanged (metadata + input + expected)
	•	plus an Actual group containing all schema fields (same names/order)
	•	plus a Match group containing exactly one column:
	•	OK if no mismatches
	•	otherwise a newline-separated list of mismatch blocks, each:
	•	expected: <expected value>\nactual: <actual value>
	•	Preserve merged group headers and formatting.

5.6 Embedded schema

Add a sheet named Schema containing:
	•	schema_type: avsc or json_schema
	•	schema_hash: SHA-256 of schema text (hex)
	•	schema_text: exact schema text (verbatim)

Add a RunInfo sheet (in run mode) with:
	•	run start timestamp (UTC + timezone)
	•	input file path/name
	•	output file path/name
	•	kafka topic
	•	timeout seconds
	•	counts: total/enabled, sent ok, matched, passed, failed, not found, conflicts

⸻

6. Attachment handling

6.1 Parsing ATTACHMENT
	•	Split by \n, accept optional \r (handle Windows and Linux).
	•	Find first non-empty line:
	•	If it starts with ./ or / or .\ or matches ^[A-Za-z]:\\ → treat as file-path mode.
	•	Otherwise → treat entire cell as text-to-PDF mode.

6.2 File-path mode
	•	Each non-empty line is a path; must exist as a file on executing machine; else pre-run error.
	•	Detect MIME type representative of common email clients:
	•	Prefer extension mapping (mimetypes) with optional signature sniffing if available; do not over-engineer.
	•	Attach each file with filename and Content-Type.

6.3 Text-to-PDF mode
	•	Create one PDF from the text (UTF-8).
	•	Attach as application/pdf, filename default attachment.pdf.

⸻

7. Email sending

7.1 Message structure

For each enabled test case:
	•	Envelope recipient: mail.to_address
	•	Header From: testcase FROM
	•	Header Subject: testcase SUBJECT
	•	Body: multipart/alternative with:
	•	text/plain: BODY (may be empty)
	•	text/html: BODY wrapped in standard HTML template (same for all), with formatting so it’s not a 1:1 copy of text (e.g., extra header line, <p>, <br>)

7.2 Concurrency
	•	Send concurrently with a limit smtp.parallelism (default 4).
	•	Record per testcase:
	•	send timestamp
	•	send success/failure and error

Failure handling default:
	•	If send fails for a testcase, mark it failed in output and do not wait for Kafka match for that testcase.

⸻

8. Kafka consumption and decoding

8.1 Windowing
	•	run_start_time captured at start of run mode (before sending emails).
	•	Only consider Kafka messages with timestamp >= run_start_time.

8.2 Decoding
	•	Kafka payload is Avro-encoded according to schema (AVSC preferred).
	•	If JSON-Schema is provided, the implementation must still decode messages correctly:
	•	either via conversion to AVSC or via an agreed decoder; if not feasible, fail fast with a clear error (do not silently mis-parse).
	•	Decoded message becomes a record from which flattened fields are derived.

8.3 Extract actual values
	•	For each flattened schema field column name:
	•	extract that field from the decoded record:
	•	nested objects follow dot path
	•	arrays/maps stringify to normalized JSON string for cell value
	•	Write extracted values into Actual columns.

⸻

9. Matching Kafka messages to test cases

9.1 Uniqueness constraint
	•	(FROM, SUBJECT) must be unique among enabled test cases; enforced pre-run.

9.2 Matching algorithm

For each decoded Kafka message (within time window):
	1.	Extract msg_from and msg_subject from schema fields matching.from_field and matching.subject_field.
	2.	Find candidates by sender:
	•	candidates = testcases where FROM == msg_from (normalized string match)
	3.	If exactly one candidate → match it.
	4.	If multiple:
	•	filter by SUBJECT == msg_subject
	•	if exactly one → match it
	•	else → CONFLICT error (record conflict details)
	5.	If none:
	•	mark as unmatched (ignored for now), keep consuming

Termination:
	•	Stop when all enabled testcases have a match OR timeout.

Multiple results per testcase:
	•	If more than one Kafka message matches a testcase:
	•	write a warning in logs
	•	output behavior:
	•	add one output row per matched result message (duplicate the testcase row)
	•	each row contains its own Actual + Match evaluation

⸻

10. Validation rules

10.1 Expected value semantics

For each expected field cell:
	•	Empty cell → IGNORE (no validation; still record actual)
	•	Literal MUSS_LEER_SEIN → actual must be empty/null after normalization
	•	Otherwise:
	•	if schema type is floating-point:
	•	allow tolerance syntax using German decimal comma:
	•	v+-t (both bounds)
	•	v+t (upper bound only)
	•	v-t (lower bound only)
	•	Example: 3,1415+-0,2, 3,1415+0,3, 3,1415-0,1
	•	else exact match after normalization

10.2 German numeric parsing and Excel types
	•	The Excel is German flavor; expected numeric strings may use comma decimals.
	•	Store numeric actual cells as numbers.
	•	Parsing tolerance expressions:
	•	parse v and t using German decimal comma.
	•	comparison uses numeric values.
	•	Integers:
	•	exact numeric equality.

10.3 Match column output (single aggregated column)
	•	If no mismatches → OK
	•	Else the Match cell contains a newline-separated list of mismatches; each mismatch block:
	•	expected: <expected value>\nactual: <actual value>
	•	Include only fields that were actually validated (not ignored).

Normalization defaults:
	•	Trim leading/trailing whitespace for string comparisons.
	•	Treat null/missing actual as empty.

⸻

11. Sanity checks (pre-run)

Fail fast before sending any email if any enabled testcase violates:
	1.	Excel structure:
	•	row 1 group seems present (merged headers) and row 2 contains column names
	•	required columns exist: ID, FROM, SUBJECT
	2.	IDs:
	•	non-empty, unique
	3.	(FROM, SUBJECT) uniqueness among enabled rows
	4.	Email format validation for FROM:
	•	simple regex supporting plus alias:
	•	^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$
	5.	Attachment validity (file mode):
	•	all files exist
	6.	Schema compatibility:
	•	flattened field list derived successfully
	•	matching fields exist in schema
	•	expected columns in Excel match the schema-derived list (same names, same count) OR, if template was generated by the script, this should always hold; otherwise error.
	7.	Schema collisions on flattening:
	•	error

⸻

12. Modes, commands, files

12.1 Modes
	•	generate-template
	•	inputs: --config <file> and --output <xlsx>
	•	output: template .xlsx + Schema sheet
	•	run
	•	inputs: --config <file> and --input <xlsx> and optional --output-dir <dir>
	•	output: a new results .xlsx named:
	•	<input_basename>-results-<YYYYMMDD-HHMMSS>.xlsx

No other CLI flags for config values.

⸻

13. Logging and errors
	•	Log run summary counts and detailed conflicts/mismatches.
	•	Do not log secrets (SMTP password, Kafka credentials).
	•	Provide clear error messages including:
	•	missing schema fields
	•	flatten collisions
	•	conflicts with candidate IDs
	•	template/schema mismatch

⸻

14. Acceptance criteria

Implementation is correct if:
	1.	Template generation produces a grouped Excel with schema-derived expected columns and embedded schema sheet.
	2.	Run mode reads the template, sends emails concurrently (default 4), consumes Kafka messages after run start time, decodes using provided schema, extracts actual fields, matches by sender then subject, and validates with ignore + MUSS_LEER_SEIN + float tolerance (German decimal comma).
	3.	Output Excel preserves grouped/merged headers, includes all input columns + actual columns + single aggregated match column, and embeds schema + run info.
	4.	Conflicts are detected and surfaced; template/schema mismatches fail fast.