"""
control.quality
───────────────
Catch silent quality degradation (zylos.md §Part1.5, §Part8 L3/L4):

  * Quality scores (L4) — agents, evaluators or the optional LLM judge report
    a 0..1 score per output; a rolling mean below the threshold raises
    QUALITY_DEGRADED → SILENT_DEGRADATION incident.
  * Known-answer probes (L3 functional) — agents register probes (input +
    expectation); BOSSman schedules RUN_PROBE directives and grades the
    answers server-side. A wrong answer from a "healthy" agent is exactly
    the failure liveness checks miss.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from typing import Any

from bossman_server.contracts.directives import DirectiveType
from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.documents import DocumentStore
from bossman_server.core.event_bus import EventBus

log = logging.getLogger(__name__)

PROBES = "probes"
WINDOW = 20
WARN_BELOW = 0.7
FAIL_BELOW = 0.5
MIN_SAMPLES = 3


@dataclass
class ProbeSpec:
    name: str
    input: Any
    expect: dict[str, Any]           # {"equals": x} | {"contains": s} | {"regex": r} | {"json_subset": {...}}
    interval_s: float = 300.0
    timeout_s: float = 60.0
    agent_id: str = ""
    last_run_at: float = 0.0
    pending_since: float = 0.0
    directive_id: str | None = None

    def validate(self) -> None:
        if not re.match(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$", self.name):
            raise ValueError(f"invalid probe name {self.name!r}")
        kinds = {"equals", "contains", "regex", "json_subset"}
        if len(self.expect) != 1 or next(iter(self.expect)) not in kinds:
            raise ValueError(f"expect must have exactly one of {sorted(kinds)}")
        if "regex" in self.expect:
            re.compile(self.expect["regex"])
        if self.interval_s < 10:
            raise ValueError("interval_s must be ≥ 10")


def grade(expect: dict[str, Any], output: Any) -> tuple[bool, str]:
    kind, want = next(iter(expect.items()))
    text = output if isinstance(output, str) else json.dumps(output, default=str, sort_keys=True)
    if kind == "equals":
        ok = output == want or (isinstance(output, str) and output.strip() == str(want).strip())
        return ok, f"expected exactly {want!r}"
    if kind == "contains":
        return str(want).lower() in text.lower(), f"expected output to contain {want!r}"
    if kind == "regex":
        return re.search(want, text) is not None, f"expected output to match /{want}/"
    if kind == "json_subset":
        if not isinstance(output, dict):
            return False, "expected a JSON object"
        return all(output.get(k) == v for k, v in want.items()), f"expected fields {want}"
    return False, "unknown expectation"


class QualityService:
    def __init__(self, docs: DocumentStore, bus: EventBus, registry: Any, directives: Any) -> None:
        self._docs = docs
        self._bus = bus
        self._registry = registry
        self._directives = directives
        self._scores: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=WINDOW))
        self._degraded: set[str] = set()
        self.judge: Any | None = None   # QualityJudge when an LLM is configured

    # ── Scores (L4) ──────────────────────────────────────────────────────────

    async def report(self, agent_id: str, score: float, *, task_id: str | None = None,
                     evaluator: str = "agent", note: str = "") -> dict[str, Any]:
        if not 0.0 <= score <= 1.0:
            raise ValueError("score must be within 0..1")
        state = await self._registry.get(agent_id)
        if state is None:
            raise KeyError(agent_id)
        window = self._scores[agent_id]
        window.append(score)
        mean = sum(window) / len(window)
        state.quality_score = round(mean, 4)
        state.quality_samples = len(window)
        if len(window) >= MIN_SAMPLES and mean < FAIL_BELOW:
            state.info.metadata["quality_degraded"] = True
            state.info.metadata["degradation_note"] = f"rolling quality {mean:.2f} < {FAIL_BELOW} over {len(window)} samples"
        elif mean >= WARN_BELOW and state.info.metadata.get("quality_degraded"):
            state.info.metadata.pop("quality_degraded", None)
            state.info.metadata.pop("degradation_note", None)
            self._degraded.discard(agent_id)
        await self._registry.sync(agent_id)
        await self._bus.publish(BossmanEvent.create(
            EventType.QUALITY_REPORTED, agent_id=agent_id, task_id=task_id, namespace=state.info.namespace,
            message=f"Quality {score:.2f} for {state.info.name} by {evaluator} (rolling {mean:.2f})",
            payload={"score": score, "rolling": round(mean, 4), "evaluator": evaluator, "note": note[:300]}))
        if len(window) >= MIN_SAMPLES and mean < FAIL_BELOW and agent_id not in self._degraded:
            self._degraded.add(agent_id)
            await self._bus.publish(BossmanEvent.create(
                EventType.QUALITY_DEGRADED, agent_id=agent_id, namespace=state.info.namespace,
                message=f"{state.info.name} output quality degraded: rolling {mean:.2f} < {FAIL_BELOW}",
                payload={"rolling_quality": round(mean, 4), "samples": len(window), "threshold": FAIL_BELOW}))
        return {"rolling": round(mean, 4), "samples": len(window)}

    # ── Probes (L3 functional) ───────────────────────────────────────────────

    async def register_probes(self, agent_id: str, probes: list[ProbeSpec]) -> list[dict[str, Any]]:
        out = []
        for p in probes:
            p.agent_id = agent_id
            p.validate()
            existing = await self._docs.get(PROBES, f"{agent_id}:{p.name}")
            if existing:
                p.last_run_at = existing.get("last_run_at", 0.0)
            await self._docs.put(PROBES, f"{agent_id}:{p.name}", asdict(p))
            out.append(asdict(p))
        return out

    async def probes_for(self, agent_id: str) -> list[ProbeSpec]:
        rows = await self._docs.list(PROBES, id_prefix=f"{agent_id}:")
        return [ProbeSpec(**{k: v for k, v in r.items() if not k.startswith("_")}) for r in rows]

    async def schedule_due(self) -> int:
        """Leader duty: issue RUN_PROBE directives for due probes; fail timed-out ones."""
        issued = 0
        now = time.time()
        for row in await self._docs.list(PROBES, limit=100_000):
            p = ProbeSpec(**{k: v for k, v in row.items() if not k.startswith("_")})
            state = await self._registry.get(p.agent_id)
            if state is None or not state.is_alive:
                await self._docs.delete(PROBES, f"{p.agent_id}:{p.name}")
                continue
            if state.info.transport != "remote" or state.paused:
                continue
            if p.pending_since and now - p.pending_since > p.timeout_s:
                await self._record(p, state, ok=False, detail=f"no answer within {p.timeout_s:.0f}s")
                continue
            if not p.pending_since and now - p.last_run_at >= p.interval_s * random.uniform(0.9, 1.1):
                d = await self._directives.issue(p.agent_id, DirectiveType.RUN_PROBE,
                                                 {"probe": p.name, "input": p.input},
                                                 reason="scheduled known-answer probe", ttl_s=p.timeout_s,
                                                 namespace=state.info.namespace)
                p.pending_since = now
                p.directive_id = d.directive_id
                await self._docs.put(PROBES, f"{p.agent_id}:{p.name}", asdict(p))
                issued += 1
        return issued

    async def submit_result(self, agent_id: str, name: str, *, output: Any = None,
                            error: str | None = None) -> dict[str, Any]:
        row = await self._docs.get(PROBES, f"{agent_id}:{name}")
        if row is None:
            raise KeyError(name)
        p = ProbeSpec(**{k: v for k, v in row.items() if not k.startswith("_")})
        state = await self._registry.get(agent_id)
        if error:
            ok, detail = False, f"probe raised: {error[:200]}"
        else:
            ok, detail = grade(p.expect, output)
            detail = "matched expectation" if ok else detail
        return await self._record(p, state, ok=ok, detail=detail)

    async def _record(self, p: ProbeSpec, state: Any, *, ok: bool, detail: str) -> dict[str, Any]:
        p.last_run_at = time.time()
        p.pending_since = 0.0
        await self._docs.put(PROBES, f"{p.agent_id}:{p.name}", asdict(p))
        result = {"passed": ok, "detail": detail, "at": p.last_run_at}
        if state is not None:
            state.probe_status[p.name] = result
            await self._registry.sync(state.agent_id)
            await self._bus.publish(BossmanEvent.create(
                EventType.PROBE_PASSED if ok else EventType.PROBE_FAILED, agent_id=state.agent_id,
                namespace=state.info.namespace,
                message=f"Probe {p.name!r} {'passed' if ok else 'FAILED'} for {state.info.name}: {detail}",
                payload={"probe": p.name, "passed": ok, "detail": detail}))
        return result


class QualityJudge:
    """LLM-as-judge sampling of completed task outputs (optional)."""

    PROMPT = """You grade the output of an AI agent for a task. Respond with JSON only:
{{"score": <0.0-1.0>, "reason": "<max 200 chars>"}}
Score 1.0 = fully correct and complete, 0.0 = wrong, empty or off-task.

TASK: {title}
DESCRIPTION: {description}
INPUT: {input}
OUTPUT: {output}"""

    def __init__(self, llm_call: Any, quality: QualityService, sample_rate: float) -> None:
        self._llm = llm_call
        self._quality = quality
        self.sample_rate = sample_rate
        self.judged = 0
        self.errors = 0

    async def maybe_judge(self, task: Any) -> None:
        if not task.assigned_to or random.random() >= self.sample_rate:
            return
        prompt = self.PROMPT.format(
            title=task.title[:300], description=task.description[:2000],
            input=json.dumps(task.input_data, default=str)[:4000],
            output=json.dumps(task.output_data, default=str)[:8000])
        try:
            raw = (await self._llm(prompt)).strip().strip("`")
            if raw.startswith("json"):
                raw = raw[4:]
            data = json.loads(raw)
            score = max(0.0, min(1.0, float(data["score"])))
            await self._quality.report(task.assigned_to, score, task_id=task.task_id, evaluator="llm-judge",
                                       note=str(data.get("reason", ""))[:200])
            self.judged += 1
        except Exception as exc:  # noqa: BLE001 - a judge failure never affects the task
            self.errors += 1
            log.warning("quality judge failed for %s: %s", task.task_id, exc)
