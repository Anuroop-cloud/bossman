"""
control.limits
──────────────
LimitService — cluster-wide concurrency caps + token-bucket rate limits.

zylos.md §Part3: "the combination of a semaphore (concurrency cap) and a
token bucket (rate cap) handles the two orthogonal dimensions of resource
contention". The in-process LLMSemaphore/TokenBucket only protect one
process; these limits are shared by every agent in the fleet, wherever it
runs, because their state lives on the coordination backend.

A permit is a lease: holders that crash stop counting against the cap once
the permit TTL lapses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus
from bossman_server.infra.backend import CoordinationBackend

log = logging.getLogger(__name__)

SPECS = "bossman:limit-specs"
NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class LimitError(Exception):
    code = "limit_error"


class LimitTimeout(LimitError):
    code = "rate_limited"

    def __init__(self, name: str, retry_after_s: float) -> None:
        super().__init__(f"limit {name!r} saturated; retry after {retry_after_s:.2f}s")
        self.name = name
        self.retry_after_s = retry_after_s


class UnknownLimit(LimitError):
    code = "not_found"


@dataclass
class LimitSpec:
    name: str
    max_concurrent: int | None = None    # semaphore cap
    rate_per_s: float | None = None      # token-bucket refill
    burst: float | None = None           # token-bucket capacity
    permit_ttl_s: float = 120.0
    description: str = ""

    def validate(self) -> None:
        if not NAME.match(self.name):
            raise ValueError(f"invalid limit name {self.name!r}")
        if self.max_concurrent is None and self.rate_per_s is None:
            raise ValueError("a limit needs max_concurrent and/or rate_per_s")
        if self.max_concurrent is not None and self.max_concurrent < 1:
            raise ValueError("max_concurrent must be ≥ 1")
        if self.rate_per_s is not None and self.rate_per_s <= 0:
            raise ValueError("rate_per_s must be > 0")
        if self.rate_per_s is not None and not self.burst:
            self.burst = max(1.0, self.rate_per_s)


@dataclass
class Permit:
    permit_id: str
    name: str
    agent_id: str
    cost: float
    acquired_at: float
    expires_at: float


def default_specs() -> list[LimitSpec]:
    return [LimitSpec(
        name="llm",
        max_concurrent=int(os.getenv("LLM_MAX_CONCURRENT", "20")),
        rate_per_s=float(os.getenv("LLM_RATE_PER_SECOND", "2.0")),
        burst=float(os.getenv("LLM_BURST", "10.0")),
        description="Fleet-wide LLM call gate (concurrency + requests/second)",
    )]


class LimitService:
    def __init__(self, backend: CoordinationBackend, bus: EventBus) -> None:
        self._backend = backend
        self._bus = bus
        self._specs: dict[str, LimitSpec] = {}

    async def start(self) -> None:
        stored = await self._backend.hgetall(SPECS)
        for raw in stored.values():
            spec = LimitSpec(**json.loads(raw))
            self._specs[spec.name] = spec
        for spec in default_specs():
            if spec.name not in self._specs:
                await self.define(spec)

    async def define(self, spec: LimitSpec) -> LimitSpec:
        spec.validate()
        self._specs[spec.name] = spec
        await self._backend.hset(SPECS, spec.name, json.dumps(asdict(spec)))
        return spec

    async def remove(self, name: str) -> bool:
        existed = self._specs.pop(name, None) is not None
        await self._backend.hdel(SPECS, name)
        return existed

    async def spec(self, name: str) -> LimitSpec:
        if name not in self._specs:
            raw = await self._backend.hget(SPECS, name)
            if raw is None:
                raise UnknownLimit(f"unknown limit {name!r}")
            self._specs[name] = LimitSpec(**json.loads(raw))
        return self._specs[name]

    # ── Mutex (short backend lock around read-modify-write) ──────────────────

    async def _with_mutex(self, name: str, fn):  # noqa: ANN001, ANN202
        key = f"bossman:limit:{name}:mutex"
        owner = uuid.uuid4().hex
        for _ in range(200):
            if await self._backend.acquire_lock(key, owner, 2000):
                try:
                    return await fn()
                finally:
                    await self._backend.release_lock(key, owner)
            await asyncio.sleep(random.uniform(0.001, 0.01))
        raise LimitError(f"limit {name!r} mutex contention")

    # ── Acquire / release ────────────────────────────────────────────────────

    async def acquire(self, name: str, agent_id: str, *, cost: float = 1.0, wait_s: float = 30.0,
                      namespace: str | None = None) -> Permit:
        spec = await self.spec(name)
        if spec.burst is not None and cost > spec.burst:
            raise ValueError(f"cost {cost} exceeds burst capacity {spec.burst} of {name!r}")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, wait_s)
        starved_reported = False
        while True:
            permit, retry_after = await self._with_mutex(name, lambda: self._try(spec, agent_id, cost))
            if permit is not None:
                return permit
            if not starved_reported:
                starved_reported = True
                await self._bus.publish(BossmanEvent.create(
                    EventType.RESOURCE_STARVATION, agent_id=agent_id, namespace=namespace,
                    resource_name=name,
                    message=f"Limit {name!r} saturated for {agent_id} (retry in {retry_after:.2f}s)",
                    payload={"bucket": name, "tokens_needed": cost, "retry_after_s": round(retry_after, 3)},
                ))
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise LimitTimeout(name, retry_after)
            await asyncio.sleep(min(remaining, max(0.01, retry_after) * random.uniform(0.5, 1.0)))

    async def _try(self, spec: LimitSpec, agent_id: str, cost: float) -> tuple[Permit | None, float]:
        now = time.time()
        pkey = f"bossman:limit:{spec.name}:permits"
        permits = {k: json.loads(v) for k, v in (await self._backend.hgetall(pkey)).items()}
        for pid, p in list(permits.items()):
            if p["expires_at"] < now:
                await self._backend.hdel(pkey, pid)
                permits.pop(pid)
        retry_after = 0.0
        if spec.max_concurrent is not None and len(permits) >= spec.max_concurrent:
            soonest = min(p["expires_at"] for p in permits.values())
            retry_after = max(0.05, min(1.0, soonest - now))
            return None, retry_after

        bkey = f"bossman:limit:{spec.name}:bucket"
        tokens = None
        if spec.rate_per_s is not None:
            state = await self._backend.hgetall(bkey)
            cap = float(spec.burst or spec.rate_per_s)
            tokens = float(state.get("tokens", cap))
            last = float(state.get("ts", now))
            tokens = min(cap, tokens + max(0.0, now - last) * spec.rate_per_s)
            if tokens < cost:
                await self._backend.hset(bkey, "tokens", str(tokens))
                await self._backend.hset(bkey, "ts", str(now))
                return None, (cost - tokens) / spec.rate_per_s
            tokens -= cost
            await self._backend.hset(bkey, "tokens", str(tokens))
            await self._backend.hset(bkey, "ts", str(now))

        permit = Permit(permit_id=f"{spec.name}~{uuid.uuid4().hex[:16]}", name=spec.name,
                        agent_id=agent_id, cost=cost, acquired_at=now,
                        expires_at=now + spec.permit_ttl_s)
        if spec.max_concurrent is not None:
            await self._backend.hset(pkey, permit.permit_id, json.dumps(asdict(permit)))
        return permit, 0.0

    async def release(self, permit_id: str) -> bool:
        name = permit_id.split("~", 1)[0]
        pkey = f"bossman:limit:{name}:permits"
        existed = await self._backend.hget(pkey, permit_id) is not None
        await self._backend.hdel(pkey, permit_id)
        return existed

    async def snapshot(self) -> list[dict[str, Any]]:
        out = []
        now = time.time()
        for name in sorted(self._specs):
            spec = self._specs[name]
            permits = [json.loads(v) for v in (await self._backend.hgetall(f"bossman:limit:{name}:permits")).values()]
            live = [p for p in permits if p["expires_at"] >= now]
            bucket = await self._backend.hgetall(f"bossman:limit:{name}:bucket")
            tokens = None
            if spec.rate_per_s is not None:
                cap = float(spec.burst or spec.rate_per_s)
                t = float(bucket.get("tokens", cap))
                last = float(bucket.get("ts", now))
                tokens = round(min(cap, t + max(0.0, now - last) * spec.rate_per_s), 2)
            out.append({
                **asdict(spec),
                "in_use": len(live),
                "holders": sorted({p["agent_id"] for p in live}),
                "tokens_available": tokens,
                "utilisation": round(len(live) / spec.max_concurrent, 4) if spec.max_concurrent else None,
            })
        return out
