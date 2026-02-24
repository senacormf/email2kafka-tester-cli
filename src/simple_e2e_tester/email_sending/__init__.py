"""Email sending exports."""

from .delivery_outcomes import EmailSendResult, SendStatus
from .email_dispatch import (
    EmailCompositionError,
    ExpectedEventDispatcher,
    SynchronousSMTPClient,
    compose_email,
)

__all__ = [
    "SendStatus",
    "EmailSendResult",
    "EmailCompositionError",
    "ExpectedEventDispatcher",
    "SynchronousSMTPClient",
    "compose_email",
]
