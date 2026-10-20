"""Typed errors mirroring the server's error envelope `{"error": {"code", "message"}}`."""

from __future__ import annotations

from typing import Any


class BossmanError(Exception):
    """Base class. `status` is the HTTP status (0 = transport failure)."""

    def __init__(self, message: str, *, status: int = 0, code: str = "error", body: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.body = body or {}

    @property
    def transient(self) -> bool:
        return self.status == 0 or self.status == 429 or self.status >= 500


class Unavailable(BossmanError):
    """BOSSman could not be reached (network error / 5xx)."""


class Unauthorized(BossmanError):
    pass


class Forbidden(BossmanError):
    pass


class NotFound(BossmanError):
    pass


class Conflict(BossmanError):
    pass


class Fenced(Conflict):
    """Another process took over this agent identity. Stop working and exit."""


class LeaseConflict(Conflict):
    """The task lease is no longer ours (expired or reassigned)."""


class Deadlock(Conflict):
    """The requested lock would close a wait-for cycle; back off and release what you hold."""


class LockTimeout(BossmanError):
    pass


class RateLimited(BossmanError):
    @property
    def retry_after_s(self) -> float:
        return float(self.body.get("retry_after_s") or 1.0)


class DuplicateAgent(Conflict):
    @property
    def existing_agent_id(self) -> str | None:
        return self.body.get("existing_agent_id")


class OutputInvalid(BossmanError):
    """The task output failed its output_schema; the attempt was recorded as failed."""


class PolicyDenied(BossmanError):
    """The control plane refused the action."""

    def __init__(self, message: str, decision: dict[str, Any]):
        super().__init__(message, status=403, code="policy_denied", body=decision)
        self.decision = decision


class ApprovalTimeout(BossmanError):
    pass


class CircuitOpen(BossmanError):
    def __init__(self, key: str, retry_after_s: float = 0.0):
        super().__init__(f"circuit {key!r} is open (retry in {retry_after_s:.0f}s)", status=503, code="circuit_open")
        self.key = key
        self.retry_after_s = retry_after_s


_BY_CODE = {
    "fenced": Fenced,
    "lease_conflict": LeaseConflict,
    "lease_lost": LeaseConflict,
    "deadlock": Deadlock,
    "lock_timeout": LockTimeout,
    "rate_limited": RateLimited,
    "spawn_limit": RateLimited,
    "duplicate_agent": DuplicateAgent,
    "output_invalid": OutputInvalid,
}
_BY_STATUS = {401: Unauthorized, 403: Forbidden, 404: NotFound, 409: Conflict, 423: LockTimeout, 429: RateLimited}


def from_response(status: int, body: Any) -> BossmanError:
    err = body.get("error", {}) if isinstance(body, dict) else {}
    code = err.get("code", "error")
    message = err.get("message", f"HTTP {status}")
    cls = _BY_CODE.get(code) or _BY_STATUS.get(status) or (Unavailable if status >= 500 else BossmanError)
    return cls(message, status=status, code=code, body=err)
