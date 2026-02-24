"""Configuration domain exports."""

from .loader import ConfigurationError, load_configuration
from .runtime_settings import (
    Configuration,
    KafkaSettings,
    MailSettings,
    MatchingConfig,
    SchemaConfig,
    SMTPSettings,
)

__all__ = [
    "Configuration",
    "KafkaSettings",
    "MailSettings",
    "MatchingConfig",
    "SchemaConfig",
    "SMTPSettings",
    "ConfigurationError",
    "load_configuration",
]
