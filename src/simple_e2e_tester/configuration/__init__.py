"""Configuration domain exports."""

from .config_scaffold_builder import (
    DEFAULT_CONFIG_FILENAME,
    build_placeholder_configuration,
    write_placeholder_configuration,
)
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
    "DEFAULT_CONFIG_FILENAME",
    "build_placeholder_configuration",
    "write_placeholder_configuration",
]
