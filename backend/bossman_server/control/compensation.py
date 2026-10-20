"""
control.compensation
────────────────────
Compensators (ALAS, zylos.md §Part6): when a tool call fails, decide how to
compensate instead of blindly retrying —

  retry       same call again after a full-jitter delay (transient errors)
  retry       with *modified parameters* (longer timeout, smaller batch,
              fewer max_tokens) when the error says the request was too big/slow
  substitute  switch to an alternative tool/model the agent offered
  give_up     non-transient: retrying cannot help (fail fast, report)

The shared circuit for the dependency is consulted first: an open circuit
means substitute or give up, never pile on.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

MAX_ATTEMPTS = 4


@dataclass
class CompensationRequest:
    tool: str
    error_class: str | None = None
    status_code: int | None = None
    error: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    attempt: int = 1
    alternatives: list[str] = field(default_factory=list)
    retry_after_s: float | None = None


@dataclass
class CompensationDecision:
    action: str                       # retry | substitute | give_up
    reason: str
    delay_s: float = 0.0
    params: dict[str, Any] = field(default_factory=dict)
    substitute: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"action": self.action, "reason": self.reason, "delay_s": round(self.delay_s, 3),
                "params": self.params, "substitute": self.substitute}


def _backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    return random.uniform(0, min(cap, base * 2 ** max(0, attempt - 1)))


def decide(req: CompensationRequest, *, circuit_state: str | None = None) -> CompensationDecision:
    cls = (req.error_class or "").lower()
    code = req.status_code
    text = req.error.lower()

    def substitute_or(reason: str) -> CompensationDecision:
        if req.alternatives:
            return CompensationDecision("substitute", reason, substitute=req.alternatives[0], params=dict(req.params))
        return CompensationDecision("give_up", reason)

    if circuit_state == "OPEN":
        return substitute_or(f"circuit for {req.tool!r} is OPEN")
    if code in (401, 403) or cls in {"credential", "unauthorized", "forbidden", "permission"}:
        return CompensationDecision("give_up", "credential/permission failure — retrying cannot fix it")
    if req.attempt >= MAX_ATTEMPTS:
        return substitute_or(f"{req.attempt} attempts exhausted")

    too_big = code == 413 or cls in {"context_length", "payload_too_large"} or "context length" in text \
        or "too many tokens" in text or "maximum context" in text
    if too_big:
        params = dict(req.params)
        if "max_tokens" in params and isinstance(params["max_tokens"], (int, float)):
            params["max_tokens"] = max(64, int(params["max_tokens"] * 0.5))
        if "batch_size" in params and isinstance(params["batch_size"], int):
            params["batch_size"] = max(1, params["batch_size"] // 2)
        if "limit" in params and isinstance(params["limit"], int):
            params["limit"] = max(1, params["limit"] // 2)
        params["truncate_input"] = True
        return CompensationDecision("retry", "request too large — retrying smaller", _backoff(1, 0.2), params)

    if cls in {"timeout", "deadline"} or code in (408, 504) or "timed out" in text:
        if req.attempt >= 2 and req.alternatives:
            return substitute_or("still timing out with a longer timeout — switching to an alternative")
        params = dict(req.params)
        t = params.get("timeout") or params.get("timeout_s")
        key = "timeout" if "timeout" in params else "timeout_s"
        params[key] = min(300, (t if isinstance(t, (int, float)) else 30) * 2)
        return CompensationDecision("retry", "timeout — retrying with a longer timeout", _backoff(req.attempt), params)

    if code == 429 or cls in {"rate_limit", "rate_limited"}:
        delay = req.retry_after_s if req.retry_after_s is not None else _backoff(req.attempt, 2.0, 60.0)
        return CompensationDecision("retry", "rate limited — backing off", delay, dict(req.params))

    if (code is not None and code >= 500) or cls in {"server_error", "unavailable", "connection", "network"}:
        if req.attempt >= 2 and req.alternatives:
            return substitute_or("dependency unavailable — switching to an alternative")
        return CompensationDecision("retry", "transient server error", _backoff(req.attempt), dict(req.params))

    if code is not None and 400 <= code < 500 or cls in {"validation", "bad_request", "not_found"}:
        return CompensationDecision("give_up", "client error — the request itself is wrong")

    return CompensationDecision("retry", "unclassified error — one cautious retry", _backoff(req.attempt),
                                dict(req.params)) if req.attempt < 2 else substitute_or("unclassified error persisted")
