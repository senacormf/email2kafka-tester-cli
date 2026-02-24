"""Configuration loader service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from simple_e2e_tester.schema_management.schema_projection import (
    SchemaError,
    flatten_schema,
    load_schema_document,
)

from .runtime_settings import (
    Configuration,
    KafkaSettings,
    MailSettings,
    MatchingConfig,
    SchemaConfig,
    SMTPSettings,
)


class ConfigurationError(Exception):
    """Raised when the configuration file is invalid."""


def load_configuration(config_path: Path | str) -> Configuration:
    """Load and validate the configuration file."""
    path = Path(config_path)
    if not path.exists():
        raise ConfigurationError(f"Configuration file not found: {path}")

    text = path.read_text(encoding="utf-8")
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as exc:  # pragma: no cover - exercised indirectly
        raise ConfigurationError(f"Failed to parse configuration file: {exc}") from exc

    if parsed is None:
        parsed = {}

    if not isinstance(parsed, Mapping):
        raise ConfigurationError("Configuration root must be a mapping.")

    schema = _parse_schema_section(parsed.get("schema"), path.parent)
    try:
        schema_document = load_schema_document(schema)
        flattened_fields = flatten_schema(schema_document)
    except SchemaError as exc:
        raise ConfigurationError(str(exc)) from exc
    field_names = {field.path for field in flattened_fields}

    matching = _parse_matching_section(parsed.get("matching"), available_fields=field_names)
    smtp = _parse_smtp_section(parsed.get("smtp"))
    mail = _parse_mail_section(parsed.get("mail"))
    kafka = _parse_kafka_section(parsed.get("kafka"))

    return Configuration(
        path=path,
        schema=schema,
        matching=matching,
        smtp=smtp,
        mail=mail,
        kafka=kafka,
    )


def _parse_schema_section(value: Any, base_path: Path) -> SchemaConfig:
    section = _require_mapping(value, "schema")
    type_candidates = [key for key in ("avsc", "json_schema") if section.get(key)]
    if len(type_candidates) != 1:
        raise ConfigurationError(
            "Exactly one event schema type (avsc or json_schema) must be provided."
        )

    schema_type = type_candidates[0]
    definition = section[schema_type]
    text, source_path = _load_schema_definition(definition, base_path)
    if not text.strip():
        raise ConfigurationError("Schema text cannot be empty.")

    return SchemaConfig(schema_type=schema_type, text=text, source_path=source_path)


def _load_schema_definition(definition: Any, base_path: Path) -> tuple[str, Path | None]:
    if isinstance(definition, str):
        return definition, None
    mapping = _require_mapping(definition, "schema definition")
    inline = mapping.get("inline")
    path_value = mapping.get("path")
    if inline and path_value:
        raise ConfigurationError("Schema definition must not set both inline and path.")
    if inline:
        if not isinstance(inline, str):
            raise ConfigurationError("Schema inline value must be a string.")
        return inline, None
    if path_value:
        if not isinstance(path_value, str):
            raise ConfigurationError("Schema path must be a string.")
        schema_path = _resolve_path(base_path, path_value)
        if not schema_path.exists():
            raise ConfigurationError(f"Schema file not found: {schema_path}")
        text = schema_path.read_text(encoding="utf-8")
        return text, schema_path
    raise ConfigurationError("Schema definition requires either inline or path.")


def _parse_matching_section(value: Any, *, available_fields: set[str]) -> MatchingConfig:
    section = _require_mapping(value, "matching")
    from_field = _require_non_empty_string(section.get("from_field"), "matching.from_field")
    subject_field = _require_non_empty_string(
        section.get("subject_field"), "matching.subject_field"
    )
    for field_name, label in (
        (from_field, "matching.from_field"),
        (subject_field, "matching.subject_field"),
    ):
        if field_name not in available_fields:
            raise ConfigurationError(f"{label} '{field_name}' does not exist in schema.")
    return MatchingConfig(from_field=from_field, subject_field=subject_field)


def _parse_smtp_section(value: Any) -> SMTPSettings:
    section = _require_mapping(value, "smtp")
    host = _require_non_empty_string(section.get("host"), "smtp.host")
    port = _require_positive_int(section.get("port"), "smtp.port")
    username = _optional_string(section.get("username"), "smtp.username")
    password = _optional_string(section.get("password"), "smtp.password")
    use_ssl = bool(section.get("use_ssl", False))
    use_starttls = section.get("use_starttls")
    use_starttls_bool = not use_ssl if use_starttls is None else bool(use_starttls)
    timeout_seconds = _require_positive_int(
        section.get("timeout_seconds", 30), "smtp.timeout_seconds"
    )
    parallelism = _require_positive_int(section.get("parallelism", 4), "smtp.parallelism")
    return SMTPSettings(
        host=host,
        port=port,
        username=username,
        password=password,
        use_starttls=use_starttls_bool,
        use_ssl=use_ssl,
        timeout_seconds=timeout_seconds,
        parallelism=parallelism,
    )


def _parse_mail_section(value: Any) -> MailSettings:
    section = _require_mapping(value, "mail")
    to_address = _require_non_empty_string(section.get("to_address"), "mail.to_address")
    cc = _normalize_string_sequence(section.get("cc"))
    bcc = _normalize_string_sequence(section.get("bcc"))
    return MailSettings(to_address=to_address, cc=cc, bcc=bcc)


def _parse_kafka_section(value: Any) -> KafkaSettings:
    section = _require_mapping(value, "kafka")
    bootstrap_servers = _normalize_bootstrap_servers(section.get("bootstrap_servers"))
    topic = _require_non_empty_string(section.get("topic"), "kafka.topic")
    group_id = _optional_string(section.get("group_id"), "kafka.group_id")
    security = section.get("security") or {}
    if not isinstance(security, Mapping):
        raise ConfigurationError("kafka.security must be a mapping.")
    timeout_seconds = _require_positive_int(
        section.get("timeout_seconds", 600), "kafka.timeout_seconds"
    )
    poll_interval_ms = _require_positive_int(
        section.get("poll_interval_ms", 500), "kafka.poll_interval_ms"
    )
    auto_offset_reset_raw = section.get("auto_offset_reset", "latest")
    auto_offset_reset = _require_non_empty_string(
        auto_offset_reset_raw, "kafka.auto_offset_reset"
    ).lower()
    return KafkaSettings(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        security=dict(security),
        timeout_seconds=timeout_seconds,
        poll_interval_ms=poll_interval_ms,
        auto_offset_reset=auto_offset_reset,
    )


def _normalize_bootstrap_servers(value: Any) -> tuple[str, ...]:
    if value is None:
        raise ConfigurationError("kafka.bootstrap_servers is required.")
    servers: list[str] = []
    if isinstance(value, str):
        servers = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, Sequence):
        for item in value:
            if not isinstance(item, str):
                raise ConfigurationError("kafka.bootstrap_servers entries must be strings.")
            stripped = item.strip()
            if stripped:
                servers.append(stripped)
    else:
        raise ConfigurationError("kafka.bootstrap_servers must be a string or list of strings.")
    if not servers:
        raise ConfigurationError("kafka.bootstrap_servers must contain at least one server.")
    return tuple(servers)


def _normalize_string_sequence(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        stripped = value.strip()
        return (stripped,) if stripped else ()
    if isinstance(value, Sequence):
        normalized = []
        for item in value:
            if not isinstance(item, str):
                raise ConfigurationError("mail.cc/bcc entries must be strings.")
            stripped = item.strip()
            if stripped:
                normalized.append(stripped)
        return tuple(normalized)
    raise ConfigurationError("mail.cc/bcc must be a string or list of strings.")


def _resolve_path(base_path: Path, raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        return (base_path / candidate).resolve()
    return candidate


def _require_mapping(value: Any, section_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"Configuration section '{section_name}' is required.")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ConfigurationError(f"{field_name} must be a string.")
    stripped = value.strip()
    if not stripped:
        raise ConfigurationError(f"{field_name} must not be empty.")
    return stripped


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"{field_name} must be a string.")
    stripped = value.strip()
    return stripped or None


def _require_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ConfigurationError(f"{field_name} must be an integer.")
    if not isinstance(value, int):
        raise ConfigurationError(f"{field_name} must be an integer.")
    if value <= 0:
        raise ConfigurationError(f"{field_name} must be greater than zero.")
    return value
