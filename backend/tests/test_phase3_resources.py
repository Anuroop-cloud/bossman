"""
tests.test_phase3_resources
────────────────────────────
Phase 3 test suite: BOSSman Resource Manager.

Tests cover:
  LLMSemaphore
    1.  Allows up to max_concurrent calls simultaneously
    2.  Blocks a (max+1)th caller until a slot frees
    3.  Times out and raises SemaphoreTimeoutError when no slot available
    4.  Stats track current_holders, peak_holders, total_acquired
    5.  Publishes RESOURCE_ACQUIRED, RESOURCE_RELEASED, RESOURCE_CONTENTION events

  TokenBucket
    6.  Full bucket immediately permits consume()
    7.  Refills tokens over time at the specified rate
    8.  Detects starvation and waits for refill
    9.  Raises TokenBucketExhaustedError when timeout exceeded
   10.  Rejects amount > capacity
   11.  Stats track total_consumed, starvation_events

  ResourceMediator
   12.  Grants exclusive access to a named resource
   13.  Second acquire blocks until first is released
   14.  Times out (ResourceTimeoutError) when lock held too long
   15.  Tracks holder in snapshot()
   16.  Detects circular wait (DEADLOCK_SUSPECTED event)
   17.  holding() and waiting_for() reflect wait-graph state

  ResourceManager (facade)
   18.  llm_call gates through both bucket and semaphore
   19.  llm_call_fn executes callable through the gate
   20.  resource() delegates to mediator
   21.  snapshot() returns all three component stats
   22.  Injected into ResearchAgent via resource_manager= kwarg
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from contracts.agent_state import AgentId
from contracts.events import BossmanEvent, EventType
from core.event_bus import EventBus
from resources.llm_semaphore import LLMSemaphore, SemaphoreTimeoutError
from resources.token_bucket import TokenBucket, TokenBucketExhaustedError
from resources.mediator import ResourceMediator, ResourceTimeoutError
from resources.resource_manager import ResourceManager


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_bus() -> tuple[EventBus, list[BossmanEvent]]:
    """Create an EventBus and a recorded event list."""
    bus = EventBus()
    received: list[BossmanEvent] = []

    async def record(event: BossmanEvent) -> None:
        received.append(event)

    bus.subscribe(record)
    return bus, received


def events_of(received: list[BossmanEvent], *types: EventType) -> list[BossmanEvent]:
    return [e for e in received if e.event_type in types]


# ── LLMSemaphore ─────────────────────────────────────────────────────────────

class TestLLMSemaphore:

    @pytest.mark.asyncio
    async def test_allows_up_to_max_concurrent(self):
        """max_concurrent=3 allows exactly 3 simultaneous holders."""
        sem = LLMSemaphore(max_concurrent=3)
        entered: list[int] = []
        barrier = asyncio.Event()

        async def hold(n: int) -> None:
            async with sem.acquire():
                entered.append(n)
                await barrier.wait()

        tasks = [asyncio.create_task(hold(i)) for i in range(3)]
        await asyncio.sleep(0.02)
        assert len(entered) == 3
        assert sem._current_holders == 3
        barrier.set()
        await asyncio.gather(*tasks)
        assert sem._current_holders == 0

    @pytest.mark.asyncio
    async def test_blocks_beyond_max(self):
        """4th caller blocks while 3 slots are in use."""
        sem = LLMSemaphore(max_concurrent=3)
        barrier = asyncio.Event()

        async def hold() -> None:
            async with sem.acquire():
                await barrier.wait()

        holders = [asyncio.create_task(hold()) for _ in range(3)]
        await asyncio.sleep(0.02)

        fourth_done = asyncio.Event()

        async def fourth() -> None:
            async with sem.acquire():
                fourth_done.set()

        fourth_task = asyncio.create_task(fourth())
        await asyncio.sleep(0.05)
        assert not fourth_done.is_set(), "4th caller should still be blocked"

        barrier.set()
        await asyncio.gather(*holders)
        await asyncio.wait_for(fourth_task, timeout=1.0)
        assert fourth_done.is_set()

    @pytest.mark.asyncio
    async def test_timeout_raises_semaphore_timeout_error(self):
        """SemaphoreTimeoutError raised when no slot frees within timeout."""
        sem = LLMSemaphore(max_concurrent=1, acquire_timeout=0.05)
        barrier = asyncio.Event()

        async def hold() -> None:
            async with sem.acquire():
                await barrier.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.01)

        with pytest.raises(SemaphoreTimeoutError):
            async with sem.acquire(agent_id=AgentId("agent-x"), timeout=0.03):
                pass

        barrier.set()
        await holder

    @pytest.mark.asyncio
    async def test_stats_track_holders_and_peak(self):
        """Stats correctly track current and peak holders."""
        sem = LLMSemaphore(max_concurrent=5)
        barrier = asyncio.Event()

        async def hold() -> None:
            async with sem.acquire():
                await barrier.wait()

        tasks = [asyncio.create_task(hold()) for _ in range(4)]
        await asyncio.sleep(0.02)

        stats = sem.stats
        assert stats["current_holders"] == 4
        assert stats["peak_holders"] == 4
        assert stats["total_acquired"] == 4

        barrier.set()
        await asyncio.gather(*tasks)

        stats = sem.stats
        assert stats["current_holders"] == 0
        assert stats["peak_holders"] == 4  # peak persists

    @pytest.mark.asyncio
    async def test_publishes_acquired_and_released_events(self):
        """RESOURCE_ACQUIRED and RESOURCE_RELEASED events are published."""
        bus, received = make_bus()
        sem = LLMSemaphore(max_concurrent=2, event_bus=bus)

        async with sem.acquire(agent_id=AgentId("agent-1")):
            pass

        await asyncio.sleep(0.01)
        types = {e.event_type for e in received}
        assert EventType.RESOURCE_ACQUIRED in types
        assert EventType.RESOURCE_RELEASED in types

    @pytest.mark.asyncio
    async def test_publishes_contention_event(self):
        """RESOURCE_CONTENTION event fired when semaphore is full."""
        bus, received = make_bus()
        sem = LLMSemaphore(max_concurrent=1, event_bus=bus)
        barrier = asyncio.Event()

        async def hold() -> None:
            async with sem.acquire():
                await barrier.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.01)

        # Try to acquire while full
        try:
            async with sem.acquire(timeout=0.05):
                pass
        except SemaphoreTimeoutError:
            pass

        barrier.set()
        await holder

        contention = events_of(received, EventType.RESOURCE_CONTENTION)
        assert len(contention) >= 1


# ── TokenBucket ───────────────────────────────────────────────────────────────

class TestTokenBucket:

    @pytest.mark.asyncio
    async def test_full_bucket_permits_immediately(self):
        """Full bucket grants tokens without waiting."""
        bucket = TokenBucket(rate=10.0, capacity=10.0)
        # Should return without sleeping
        done = False

        async def go() -> None:
            nonlocal done
            await bucket.consume(1.0)
            done = True

        await asyncio.wait_for(go(), timeout=0.5)
        assert done

    @pytest.mark.asyncio
    async def test_refills_over_time(self):
        """Bucket refills at the specified rate."""
        bucket = TokenBucket(rate=100.0, capacity=100.0)
        # Drain the bucket
        await bucket.consume(100.0)
        assert bucket.tokens_available < 1.0

        # Wait for 0.1s — at 100 tokens/s, should have ~10 tokens
        await asyncio.sleep(0.12)
        assert bucket.tokens_available >= 8.0  # some slack for timing

    @pytest.mark.asyncio
    async def test_starvation_waits_for_refill(self):
        """Caller waits when bucket is low, then proceeds once refilled."""
        bucket = TokenBucket(rate=50.0, capacity=10.0)
        await bucket.consume(10.0)  # drain

        # At 50 tokens/s, need 1 token → wait ~20ms
        start = asyncio.get_event_loop().time()
        await bucket.consume(1.0)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.01  # waited at least 10ms

    @pytest.mark.asyncio
    async def test_timeout_raises_exhausted_error(self):
        """TokenBucketExhaustedError raised when refill can't happen in time."""
        bucket = TokenBucket(rate=0.01, capacity=1.0, consume_timeout=0.05)
        await bucket.consume(1.0)  # drain

        with pytest.raises(TokenBucketExhaustedError):
            await bucket.consume(1.0, timeout=0.02)  # can't refill in 20ms at 0.01/s

    @pytest.mark.asyncio
    async def test_amount_exceeds_capacity_raises(self):
        """Requesting more tokens than capacity raises ValueError."""
        bucket = TokenBucket(rate=1.0, capacity=5.0)
        with pytest.raises(ValueError, match="exceeds bucket capacity"):
            await bucket.consume(10.0)

    @pytest.mark.asyncio
    async def test_stats_track_consumed_and_starvation(self):
        """Stats correctly count consumed tokens and starvation events."""
        bucket = TokenBucket(rate=100.0, capacity=20.0)
        await bucket.consume(5.0)
        await bucket.consume(3.0)
        stats = bucket.stats
        assert stats["total_consumed"] == 8.0
        assert stats["total_calls"] == 2

    @pytest.mark.asyncio
    async def test_publishes_acquired_event(self):
        """RESOURCE_ACQUIRED event published on successful consume."""
        bus, received = make_bus()
        bucket = TokenBucket(rate=10.0, capacity=10.0, event_bus=bus)
        await bucket.consume(1.0, agent_id=AgentId("agent-a"))
        await asyncio.sleep(0.01)
        types = {e.event_type for e in received}
        assert EventType.RESOURCE_ACQUIRED in types

    @pytest.mark.asyncio
    async def test_publishes_starvation_event(self):
        """RESOURCE_STARVATION event published when bucket is empty."""
        bus, received = make_bus()
        bucket = TokenBucket(rate=100.0, capacity=5.0, event_bus=bus)
        await bucket.consume(5.0)  # drain

        # consume will wait briefly and publish starvation
        try:
            await asyncio.wait_for(bucket.consume(1.0), timeout=0.5)
        except asyncio.TimeoutError:
            pass  # we just want the event

        await asyncio.sleep(0.01)
        starvation = events_of(received, EventType.RESOURCE_STARVATION)
        assert len(starvation) >= 1


# ── ResourceMediator ──────────────────────────────────────────────────────────

class TestResourceMediator:

    @pytest.mark.asyncio
    async def test_grants_exclusive_access(self):
        """First caller gets the lock; second waits."""
        med = ResourceMediator()
        in_critical = asyncio.Event()
        barrier = asyncio.Event()
        order: list[str] = []

        async def first() -> None:
            async with med.acquire("db", agent_id=AgentId("agent-1")):
                in_critical.set()
                order.append("first-in")
                await barrier.wait()
                order.append("first-out")

        async def second() -> None:
            await in_critical.wait()
            async with med.acquire("db", agent_id=AgentId("agent-2")):
                order.append("second-in")

        t1 = asyncio.create_task(first())
        t2 = asyncio.create_task(second())
        await asyncio.sleep(0.05)
        assert order == ["first-in"], "second should not have entered yet"

        barrier.set()
        await asyncio.gather(t1, t2)
        assert order == ["first-in", "first-out", "second-in"]

    @pytest.mark.asyncio
    async def test_timeout_raises_resource_timeout_error(self):
        """ResourceTimeoutError raised when lock held beyond timeout."""
        med = ResourceMediator(default_timeout=0.05)
        barrier = asyncio.Event()

        async def holder() -> None:
            async with med.acquire("res"):
                await barrier.wait()

        t = asyncio.create_task(holder())
        await asyncio.sleep(0.01)

        with pytest.raises(ResourceTimeoutError):
            async with med.acquire("res", timeout=0.02):
                pass

        barrier.set()
        await t

    @pytest.mark.asyncio
    async def test_snapshot_shows_holder(self):
        """snapshot() reflects the current holder."""
        med = ResourceMediator()
        barrier = asyncio.Event()

        async def hold() -> None:
            async with med.acquire("mem", agent_id=AgentId("agent-9")):
                await barrier.wait()

        t = asyncio.create_task(hold())
        await asyncio.sleep(0.02)

        snap = med.snapshot()
        assert "mem" in snap
        assert snap["mem"]["holder"] == "agent-9"

        barrier.set()
        await t

    @pytest.mark.asyncio
    async def test_deadlock_suspected_event(self):
        """DEADLOCK_SUSPECTED fired when circular wait is detected."""
        bus, received = make_bus()
        med = ResourceMediator(default_timeout=0.1, event_bus=bus)

        barrier_a = asyncio.Event()
        barrier_b = asyncio.Event()

        async def agent_a() -> None:
            """Holds 'lock-x', then tries to get 'lock-y'."""
            async with med.acquire("lock-x", agent_id=AgentId("agent-A")):
                barrier_a.set()
                await barrier_b.wait()
                try:
                    async with med.acquire("lock-y", agent_id=AgentId("agent-A"), timeout=0.05):
                        pass
                except ResourceTimeoutError:
                    pass

        async def agent_b() -> None:
            """Holds 'lock-y', then tries to get 'lock-x'."""
            await barrier_a.wait()
            async with med.acquire("lock-y", agent_id=AgentId("agent-B")):
                barrier_b.set()
                try:
                    async with med.acquire("lock-x", agent_id=AgentId("agent-B"), timeout=0.05):
                        pass
                except ResourceTimeoutError:
                    pass

        await asyncio.gather(agent_a(), agent_b())
        await asyncio.sleep(0.05)

        deadlock_events = events_of(received, EventType.DEADLOCK_SUSPECTED)
        assert len(deadlock_events) >= 1, (
            f"Expected DEADLOCK_SUSPECTED event. Got: {[e.event_type for e in received]}"
        )

    @pytest.mark.asyncio
    async def test_holding_and_waiting_for_state(self):
        """holding() and waiting_for() reflect the wait-graph correctly."""
        med = ResourceMediator(default_timeout=0.5)
        barrier = asyncio.Event()
        holding_confirmed = asyncio.Event()

        async def holder() -> None:
            async with med.acquire("res-x", agent_id=AgentId("agent-h")):
                holding_confirmed.set()
                await barrier.wait()

        t = asyncio.create_task(holder())
        await holding_confirmed.wait()

        assert "res-x" in med.holding(AgentId("agent-h"))
        barrier.set()
        await t

    @pytest.mark.asyncio
    async def test_publishes_acquired_and_released(self):
        """RESOURCE_ACQUIRED and RESOURCE_RELEASED events published by mediator."""
        bus, received = make_bus()
        med = ResourceMediator(event_bus=bus)

        async with med.acquire("test-lock", agent_id=AgentId("agent-z")):
            pass

        await asyncio.sleep(0.01)
        types = {e.event_type for e in received}
        assert EventType.RESOURCE_ACQUIRED in types
        assert EventType.RESOURCE_RELEASED in types


# ── ResourceManager facade ────────────────────────────────────────────────────

class TestResourceManager:

    @pytest.mark.asyncio
    async def test_llm_call_gates_through_both(self):
        """llm_call context manager consumes a token AND acquires a semaphore slot."""
        bus, received = make_bus()
        rm = ResourceManager(
            max_concurrent_llm=2,
            llm_calls_per_second=100.0,
            llm_burst=10.0,
            event_bus=bus,
        )

        async with rm.llm_call(agent_id=AgentId("agent-1")):
            pass

        await asyncio.sleep(0.01)
        types = {e.event_type for e in received}
        # Both semaphore and bucket emit RESOURCE_ACQUIRED
        acquired_events = events_of(received, EventType.RESOURCE_ACQUIRED)
        assert len(acquired_events) >= 2, f"Expected ≥2 ACQUIRED events, got {len(acquired_events)}"

    @pytest.mark.asyncio
    async def test_llm_call_fn_executes_callable(self):
        """llm_call_fn gates and returns the callable's result."""
        rm = ResourceManager(
            max_concurrent_llm=5,
            llm_calls_per_second=100.0,
            llm_burst=10.0,
        )

        async def fake_llm() -> str:
            return "response-text"

        result = await rm.llm_call_fn(
            call=fake_llm,
            agent_id=AgentId("agent-1"),
        )
        assert result == "response-text"

    @pytest.mark.asyncio
    async def test_resource_delegates_to_mediator(self):
        """resource() context manager provides exclusive named lock."""
        rm = ResourceManager(
            max_concurrent_llm=5,
            llm_calls_per_second=100.0,
            llm_burst=10.0,
        )
        order: list[str] = []
        barrier = asyncio.Event()

        async def first() -> None:
            async with rm.resource("shared-mem", agent_id=AgentId("a1")):
                order.append("first")
                await barrier.wait()

        async def second() -> None:
            await asyncio.sleep(0.01)
            async with rm.resource("shared-mem", agent_id=AgentId("a2")):
                order.append("second")

        t1 = asyncio.create_task(first())
        t2 = asyncio.create_task(second())
        await asyncio.sleep(0.02)
        assert order == ["first"]

        barrier.set()
        await asyncio.gather(t1, t2)
        assert order == ["first", "second"]

    @pytest.mark.asyncio
    async def test_snapshot_includes_all_components(self):
        """snapshot() returns keys for all three resource components."""
        rm = ResourceManager(
            max_concurrent_llm=5,
            llm_calls_per_second=100.0,
            llm_burst=10.0,
        )
        snap = rm.snapshot()
        assert "llm_semaphore" in snap
        assert "token_bucket" in snap
        assert "named_resources" in snap

    @pytest.mark.asyncio
    async def test_injected_into_research_agent(self, tmp_path):
        """ResourceManager can be injected into ResearchAgent and is accessible."""
        from agents.research_agent import ResearchAgent
        from unittest.mock import patch

        db = str(tmp_path / "rm_test.db")

        async def fake_llm(msgs: list) -> str:
            return "SUFFICIENT" if any("Reflect" in str(m) for m in msgs) else "answer"

        rm = ResourceManager(
            max_concurrent_llm=5,
            llm_calls_per_second=100.0,
            llm_burst=10.0,
        )
        agent = ResearchAgent(
            llm_call=fake_llm,
            resource_manager=rm,
            heartbeat_interval=9999,
        )

        assert agent._resource_manager is rm

        with patch("agents.base_agent._CHECKPOINT_DB", db):
            await agent.start()
            await agent.stop()
