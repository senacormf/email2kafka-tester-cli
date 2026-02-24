"""Configuration domain entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SchemaConfig:
    """Normalized schema settings."""

    schema_type: str
    text: str
    source_path: Path | None


@dataclass(frozen=True)
class MatchingConfig:
    """Field names used for matching Kafka records with testcases."""

    from_field: str
    subject_field: str


@dataclass(frozen=True)
class SMTPSettings:  # pylint: disable=too-many-instance-attributes
    """SMTP server connectivity configuration."""

    host: str
    port: int
    username: str | None
    password: str | None
    use_starttls: bool
    use_ssl: bool
    timeout_seconds: int
    parallelism: int


@dataclass(frozen=True)
class MailSettings:
    """Destination mailbox configuration."""

    to_address: str
    cc: tuple[str, ...]
    bcc: tuple[str, ...]


@dataclass(frozen=True)
class KafkaSettings:
    """Kafka consumer configuration."""

    bootstrap_servers: tuple[str, ...]
    topic: str
    group_id: str | None
    security: Mapping[str, object]
    timeout_seconds: int
    poll_interval_ms: int
    auto_offset_reset: str


@dataclass(frozen=True)
class Configuration:
    """Top-level configuration aggregate."""

    path: Path
    schema: SchemaConfig
    matching: MatchingConfig
    smtp: SMTPSettings
    mail: MailSettings
    kafka: KafkaSettings
