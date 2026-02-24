"""Email sending domain entities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum


class SendStatus(str, Enum):
    """Email sending outcome status."""

    SENT = "sent"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class EmailSendResult:
    """Outcome of attempting to send a testcase email."""

    test_id: str
    status: SendStatus
    sent_at: datetime | None
    error_message: str | None

    @staticmethod
    def sent(test_id: str) -> EmailSendResult:
        return EmailSendResult(
            test_id=test_id,
            status=SendStatus.SENT,
            sent_at=datetime.now(UTC),
            error_message=None,
        )

    @staticmethod
    def failed(test_id: str, error: Exception) -> EmailSendResult:
        return EmailSendResult(
            test_id=test_id,
            status=SendStatus.FAILED,
            sent_at=None,
            error_message=str(error),
        )

    @staticmethod
    def skipped(test_id: str) -> EmailSendResult:
        return EmailSendResult(
            test_id=test_id,
            status=SendStatus.SKIPPED,
            sent_at=None,
            error_message=None,
        )
