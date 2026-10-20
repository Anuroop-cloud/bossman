"""
Client-side resilience primitives (usable with or without a BOSSman server).

  is_transient(exc)            classify an exception: retry it or fail fast
  retry_with_backoff(fn, …)    exponential backoff with FULL jitter, transient-only
  DegradationLadder            primary model → fallback model → cache → static → queue
  SemanticCache                similarity-keyed response cache for ladder level 3

zylos.md §Part2/§Part4. The server may move an agent down (or up) the ladder
at any time with FALLBACK_MODEL / RESTORE_MODEL / SET_DEGRADATION directives.
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")

TRANSIENT_STATUS = {408, 409, 425, 429, 500, 502, 503, 504, 529}
NON_TRANSIENT_STATUS = {400, 401, 403, 404, 405, 410, 413, 422}


def status_of(exc: BaseException) -> int | None:
    for attr in ("status_code", "status"):
        v = getattr(exc, attr, None)
        if isinstance(v, int):
            return v
    resp = getattr(exc, "response", None)
    v = getattr(resp, "status_code", None)
    return v if isinstance(v, int) else None


def error_class_of(exc: BaseException) -> str:
    name = type(exc).__name__.lower()
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or "timeout" in name:
        return "timeout"
    if isinstance(exc, (ConnectionError, OSError)) or "connect" in name or "network" in name:
        return "connection"
    if "ratelimit" in name or "rate_limit" in name:
        return "rate_limit"
    if "auth" in name or "permission" in name or "forbidden" in name:
        return "credential"
    if isinstance(exc, (ValueError, TypeError, KeyError)) or "validation" in name:
        return "validation"
    return name[:64]


def is_transient(exc: BaseException) -> bool:
    status = status_of(exc)
    if status is not None:
        if status in TRANSIENT_STATUS or status >= 500:
            return True
        if status in NON_TRANSIENT_STATUS:
            return False
    transient = getattr(exc, "transient", None)
    if isinstance(transient, bool):
        return transient
    return error_class_of(exc) in {"timeout", "connection", "rate_limit"}


def full_jitter(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    return random.uniform(0, min(cap, base * 2 ** max(0, attempt)))


async def retry_with_backoff(
    fn: Callable[[], Awaitable[T]],
    *,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_if: Callable[[BaseException], bool] = is_transient,
    on_retry: Callable[[int, BaseException, float], Any] | None = None,
) -> T:
    """zylos.md §Part2: only transient errors are retried; full jitter avoids thundering herds."""
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001
            if attempt == max_retries or not retry_if(exc):
                raise
            retry_after = getattr(exc, "retry_after_s", None)
            delay = float(retry_after) if isinstance(retry_after, (int, float)) else full_jitter(
                attempt, base_delay, max_delay)
            if on_retry is not None:
                on_retry(attempt + 1, exc, delay)
            await asyncio.sleep(delay)
    raise RuntimeError("unreachable")


# ── Semantic cache ───────────────────────────────────────────────────────────

_WORD = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> set[str]:
    return set(_WORD.findall(text.lower()))


class SemanticCache:
    """
    Response cache keyed by similarity. The default similarity is token
    Jaccard (no embedding model needed); pass `similarity=` to use embeddings.
    """

    def __init__(self, *, threshold: float = 0.85, max_entries: int = 1000, ttl_s: float = 3600,
                 similarity: Callable[[str, str], float] | None = None) -> None:
        self.threshold = threshold
        self.max_entries = max_entries
        self.ttl_s = ttl_s
        self._similarity = similarity
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def _sim(self, a: str, b: str) -> float:
        if self._similarity is not None:
            return self._similarity(a, b)
        ta, tb = _tokens(a), _tokens(b)
        return len(ta & tb) / len(ta | tb) if ta and tb else float(a == b)

    def put(self, prompt: str, value: Any) -> None:
        self._items[prompt] = (time.monotonic(), value)
        self._items.move_to_end(prompt)
        while len(self._items) > self.max_entries:
            self._items.popitem(last=False)

    def get(self, prompt: str) -> Any | None:
        now = time.monotonic()
        if prompt in self._items and now - self._items[prompt][0] <= self.ttl_s:
            return self._items[prompt][1]
        best, best_score = None, 0.0
        for key, (at, value) in self._items.items():
            if now - at > self.ttl_s:
                continue
            score = self._sim(prompt, key)
            if score > best_score:
                best, best_score = value, score
        return best if best_score >= self.threshold else None


# ── Degradation ladder ───────────────────────────────────────────────────────


class QueuedForLater(Exception):
    """Ladder level 5: the request was accepted and queued; no answer now."""


@dataclass
class LadderResult:
    value: Any
    level: int          # 0 full · 1 fallback model · 2 cache · 3 static · 4 queued
    source: str
    error: str | None = None

    @property
    def degraded(self) -> bool:
        return self.level > 0


class DegradationLadder:
    """
    Explicit, observable graceful degradation (zylos.md §Part4):

        L1 primary model   L2 fallback model   L3 cache   L4 static answer   L5 queue

    `level` is the floor the server asked for (directive), so the ladder starts
    there; a failing rung falls through to the next. The rung actually used is
    reported back to BOSSman so degradation is visible on the dashboard.
    """

    def __init__(
        self,
        *,
        primary: Callable[[str], Awaitable[Any]],
        fallback: Callable[[str], Awaitable[Any]] | None = None,
        cache: SemanticCache | None = None,
        static: Callable[[str], Any] | Any | None = None,
        queue: Callable[[str], Awaitable[Any]] | None = None,
        agent: Any | None = None,
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.cache = cache
        self.static = static
        self.queue = queue
        self.agent = agent
        self.last_level = 0

    @property
    def floor(self) -> int:
        return int(getattr(self.agent, "degradation_level", 0) or 0)

    async def run(self, prompt: str) -> LadderResult:
        errors: list[str] = []
        rungs: list[tuple[int, str, Callable[[], Awaitable[Any]] | None]] = [
            (0, "primary", lambda: self.primary(prompt)),
            (1, "fallback", (lambda: self.fallback(prompt)) if self.fallback else None),
        ]
        for level, source, call in rungs:
            if level < self.floor or call is None:
                continue
            try:
                value = await call()
                if self.cache is not None:
                    self.cache.put(prompt, value)
                return self._done(LadderResult(value, level, source, "; ".join(errors) or None))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{source}: {type(exc).__name__}: {str(exc)[:120]}")
        if self.cache is not None and self.floor <= 2:
            hit = self.cache.get(prompt)
            if hit is not None:
                return self._done(LadderResult(hit, 2, "cache", "; ".join(errors) or None))
        if self.static is not None and self.floor <= 3:
            value = self.static(prompt) if callable(self.static) else self.static
            return self._done(LadderResult(value, 3, "static", "; ".join(errors) or None))
        if self.queue is not None:
            await self.queue(prompt)
            self._done(LadderResult(None, 4, "queued", "; ".join(errors) or None))
            raise QueuedForLater("; ".join(errors) or "queued by degradation ladder")
        raise RuntimeError("degradation ladder exhausted: " + "; ".join(errors))

    def _done(self, r: LadderResult) -> LadderResult:
        self.last_level = r.level
        if self.agent is not None and hasattr(self.agent, "report"):
            self.agent.report(degradation_level=max(self.floor, r.level))
        return r
