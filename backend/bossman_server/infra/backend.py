"""
infra.backend
─────────────
CoordinationBackend — the small slice of "shared infrastructure" BOSSman
needs to run as more than one process:

  - TTL locks      (leader election, distributed resource locks)
  - pub/sub        (cross-node event fan-out)
  - hash KV        (durable agent registry snapshots)

Two implementations:
  InMemoryBackend  single-process / tests / local dev (default)
  RedisBackend     real deployments (redis.asyncio)

Both honour identical semantics so the rest of the system never branches
on which one is active.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Awaitable, Callable

log = logging.getLogger(__name__)

MessageHandler = Callable[[str], Awaitable[None]]
Unsubscribe = Callable[[], Awaitable[None]]


class CoordinationBackend(ABC):
    # ── Locks ─────────────────────────────────────────────────────────────────
    @abstractmethod
    async def acquire_lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        """Take the lock if free (or already ours). Returns True on success."""

    @abstractmethod
    async def renew_lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        """Extend the TTL only if we still own it."""

    @abstractmethod
    async def release_lock(self, key: str, owner: str) -> bool:
        """Release only if we own it."""

    @abstractmethod
    async def lock_owner(self, key: str) -> str | None: ...

    # ── Pub/sub ───────────────────────────────────────────────────────────────
    @abstractmethod
    async def publish(self, channel: str, message: str) -> None: ...

    @abstractmethod
    async def subscribe(self, channel: str, handler: MessageHandler) -> Unsubscribe: ...

    # ── Hash KV ───────────────────────────────────────────────────────────────
    @abstractmethod
    async def hset(self, name: str, field: str, value: str) -> None: ...

    @abstractmethod
    async def hdel(self, name: str, field: str) -> None: ...

    @abstractmethod
    async def hgetall(self, name: str) -> dict[str, str]: ...

    async def hget(self, name: str, field: str) -> str | None:
        return (await self.hgetall(name)).get(field)

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:  # pragma: no cover - optional
        return None


# ── In-memory ────────────────────────────────────────────────────────────────


class InMemoryBackend(CoordinationBackend):
    """Share one instance between several 'nodes' to simulate a cluster."""

    def __init__(self) -> None:
        self._locks: dict[str, tuple[str, float]] = {}
        self._subs: dict[str, list[MessageHandler]] = defaultdict(list)
        self._hashes: dict[str, dict[str, str]] = defaultdict(dict)

    def _live_owner(self, key: str) -> str | None:
        entry = self._locks.get(key)
        if entry is None:
            return None
        owner, expires = entry
        if expires <= time.monotonic():
            del self._locks[key]
            return None
        return owner

    async def acquire_lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        current = self._live_owner(key)
        if current not in (None, owner):
            return False
        self._locks[key] = (owner, time.monotonic() + ttl_ms / 1000)
        return True

    async def renew_lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        if self._live_owner(key) != owner:
            return False
        self._locks[key] = (owner, time.monotonic() + ttl_ms / 1000)
        return True

    async def release_lock(self, key: str, owner: str) -> bool:
        if self._live_owner(key) != owner:
            return False
        del self._locks[key]
        return True

    async def lock_owner(self, key: str) -> str | None:
        return self._live_owner(key)

    async def publish(self, channel: str, message: str) -> None:
        for handler in list(self._subs.get(channel, [])):
            try:
                await handler(message)
            except Exception:  # noqa: BLE001 - never break publishers
                log.exception("pubsub handler failed on %s", channel)

    async def subscribe(self, channel: str, handler: MessageHandler) -> Unsubscribe:
        self._subs[channel].append(handler)

        async def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._subs[channel].remove(handler)

        return _unsub

    async def hset(self, name: str, field: str, value: str) -> None:
        self._hashes[name][field] = value

    async def hdel(self, name: str, field: str) -> None:
        self._hashes[name].pop(field, None)

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self._hashes.get(name, {}))

    async def hget(self, name: str, field: str) -> str | None:
        return self._hashes.get(name, {}).get(field)


# ── Redis ────────────────────────────────────────────────────────────────────


class RedisBackend(CoordinationBackend):
    """Backed by redis.asyncio. Pass `client=` to inject (e.g. fakeredis)."""

    def __init__(self, url: str | None = None, client=None) -> None:  # noqa: ANN001
        if client is None:
            import redis.asyncio as aioredis

            client = aioredis.from_url(url, decode_responses=True)
        self._r = client
        self._tasks: set[asyncio.Task] = set()

    async def acquire_lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        if await self._r.set(key, owner, nx=True, px=ttl_ms):
            return True
        return await self.renew_lock(key, owner, ttl_ms)  # re-entrant for same owner

    async def _if_owner(self, key: str, owner: str, action: str, ttl_ms: int = 0) -> bool:
        """Optimistic compare-and-act using WATCH (no Lua dependency)."""
        from redis.exceptions import WatchError

        async with self._r.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(key)
                if await pipe.get(key) != owner:
                    await pipe.unwatch()
                    return False
                pipe.multi()
                if action == "renew":
                    pipe.pexpire(key, ttl_ms)
                else:
                    pipe.delete(key)
                await pipe.execute()
                return True
            except WatchError:
                return False

    async def renew_lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        return await self._if_owner(key, owner, "renew", ttl_ms)

    async def release_lock(self, key: str, owner: str) -> bool:
        return await self._if_owner(key, owner, "release")

    async def lock_owner(self, key: str) -> str | None:
        return await self._r.get(key)

    async def publish(self, channel: str, message: str) -> None:
        await self._r.publish(channel, message)

    async def subscribe(self, channel: str, handler: MessageHandler) -> Unsubscribe:
        pubsub = self._r.pubsub()
        await pubsub.subscribe(channel)

        async def _pump() -> None:
            try:
                async for msg in pubsub.listen():
                    if msg.get("type") != "message":
                        continue
                    try:
                        await handler(msg["data"])
                    except Exception:  # noqa: BLE001
                        log.exception("pubsub handler failed on %s", channel)
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(_pump(), name=f"redis-sub:{channel}")
        self._tasks.add(task)

        async def _unsub() -> None:
            task.cancel()
            self._tasks.discard(task)
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()

        return _unsub

    async def hset(self, name: str, field: str, value: str) -> None:
        await self._r.hset(name, field, value)

    async def hdel(self, name: str, field: str) -> None:
        await self._r.hdel(name, field)

    async def hgetall(self, name: str) -> dict[str, str]:
        return await self._r.hgetall(name)

    async def hget(self, name: str, field: str) -> str | None:
        return await self._r.hget(name, field)

    async def ping(self) -> bool:
        try:
            return bool(await self._r.ping())
        except Exception:  # noqa: BLE001
            return False

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        await self._r.aclose()
