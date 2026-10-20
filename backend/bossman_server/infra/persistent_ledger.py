"""
infra.persistent_ledger
───────────────────────
PersistentLedger — a RecoveryLedger that mirrors itself to a LedgerStore.

The engine and evaluator keep using the synchronous in-memory API
(`append`, `for_agent`, …). Writes happen in the background:

  * `append()` schedules an immediate upsert.
  * `sync()` upserts every entry whose serialised form changed since the
    last write — this catches later mutations such as the Evaluator
    marking an attempt verified. A runtime loop calls it periodically.

Persistence is best-effort: a store outage is logged and retried on the
next sync, never raised into the recovery path.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from bossman_server.contracts.recovery_ledger import RecoveryAttempt
from bossman_server.infra.ledger_store import LedgerStore
from bossman_server.recovery.recovery_engine import RecoveryLedger

log = logging.getLogger(__name__)


class PersistentLedger(RecoveryLedger):
    def __init__(self, store: LedgerStore) -> None:
        super().__init__()
        self._store = store
        self._written: dict[str, str] = {}  # attempt_id → last persisted JSON
        self._pending: set[asyncio.Task] = set()
        self._loop_task: asyncio.Task | None = None

    def append(self, attempt: RecoveryAttempt) -> None:
        super().append(attempt)
        try:
            task = asyncio.get_running_loop().create_task(self._write(attempt))
        except RuntimeError:  # no running loop (sync caller) — sync() will catch it
            return
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _write(self, attempt: RecoveryAttempt) -> None:
        doc = attempt.model_dump_json()
        if self._written.get(attempt.attempt_id) == doc:
            return
        try:
            await self._store.upsert(attempt)
            self._written[attempt.attempt_id] = doc
        except Exception:  # noqa: BLE001
            log.exception("ledger persist failed for %s (will retry)", attempt.attempt_id)

    async def sync(self) -> int:
        """Persist all changed entries. Returns number written."""
        changed = [
            e for e in list(self.entries)
            if self._written.get(e.attempt_id) != e.model_dump_json()
        ]
        for e in changed:
            await self._write(e)
        return sum(1 for e in changed if e.attempt_id in self._written)

    async def restore(self, limit: int = 1000) -> int:
        """Load history from the store into memory (on startup / leader takeover)."""
        known = {e.attempt_id for e in self.entries}
        loaded = 0
        for attempt in await self._store.recent(limit):
            if attempt.attempt_id in known:
                continue
            self.entries.append(attempt)
            self._written[attempt.attempt_id] = attempt.model_dump_json()
            loaded += 1
        self.entries.sort(key=lambda e: e.failure_detected_at)
        return loaded

    async def start_sync_loop(self, interval_s: float = 2.0) -> None:
        if self._loop_task:
            return

        async def _loop() -> None:
            while True:
                await asyncio.sleep(interval_s)
                await self.sync()

        self._loop_task = asyncio.create_task(_loop(), name="bossman:ledger-sync")

    async def stop(self) -> None:
        if self._loop_task:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task
            self._loop_task = None
        if self._pending:
            await asyncio.gather(*list(self._pending), return_exceptions=True)
        await self.sync()  # final flush
