"""
control.metrics
───────────────
Prometheus exposition (`GET /metrics`). A custom collector samples live
runtime state at scrape time, so there are no counters to keep in sync and
nothing is registered globally (safe with several apps per process).
"""

from __future__ import annotations

import asyncio
from typing import Any

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily


class RuntimeCollector:
    def __init__(self) -> None:
        self.snapshot: dict[str, Any] = {}

    def collect(self):  # noqa: ANN201
        s = self.snapshot
        if not s:
            return
        g = GaugeMetricFamily("bossman_agents", "Agents by namespace and status", labels=["namespace", "status"])
        for (ns, st), n in s["agents"].items():
            g.add_metric([ns, st], n)
        yield g
        g = GaugeMetricFamily("bossman_tasks", "Tasks by status", labels=["status"])
        for st, n in s["tasks"].items():
            g.add_metric([st], n)
        yield g
        yield GaugeMetricFamily("bossman_dlq_depth", "Dead-lettered + quarantined tasks", value=s["dlq"])
        yield GaugeMetricFamily("bossman_incidents_active", "Unresolved incidents", value=s["incidents_active"])
        c = CounterMetricFamily("bossman_recovery_attempts", "Recovery attempts", labels=["strategy", "outcome"])
        for (strategy, outcome), n in s["attempts"].items():
            c.add_metric([strategy, outcome], n)
        yield c
        yield CounterMetricFamily("bossman_events_published", "Events published", value=s["events_published"])
        g = GaugeMetricFamily("bossman_agent_heartbeat_age_seconds", "Seconds since last heartbeat",
                              labels=["agent_id", "name", "namespace"])
        for aid, name, ns, age in s["heartbeat_ages"]:
            g.add_metric([aid, name, ns], age)
        yield g
        g = GaugeMetricFamily("bossman_agent_tokens_per_minute", "Token burn rate", labels=["agent_id", "name"])
        for aid, name, tpm in s["token_rates"]:
            g.add_metric([aid, name], tpm)
        yield g
        g = GaugeMetricFamily("bossman_agent_context_utilisation", "Context window used", labels=["agent_id", "name"])
        for aid, name, ctx in s["context"]:
            g.add_metric([aid, name], ctx)
        yield g
        yield GaugeMetricFamily("bossman_lock_leases", "Active lock leases", value=s["leases"])
        g = GaugeMetricFamily("bossman_limit_in_use", "Permits in use per limit", labels=["limit"])
        for name, n in s["limits"].items():
            g.add_metric([name], n)
        yield g
        yield GaugeMetricFamily("bossman_circuits_open", "Open shared circuits", value=s["circuits_open"])
        yield GaugeMetricFamily("bossman_approvals_pending", "Approvals awaiting humans", value=s["approvals_pending"])
        g = GaugeMetricFamily("bossman_managed_processes", "Managed processes by state", labels=["state"])
        for st, n in s["managed"].items():
            g.add_metric([st], n)
        yield g
        yield GaugeMetricFamily("bossman_is_leader", "1 if this node runs supervision duties", value=s["is_leader"])
        yield GaugeMetricFamily("bossman_directives_issued", "Directives issued since start",
                                value=s["directives_issued"])


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=False)
        self.collector = RuntimeCollector()
        self.registry.register(self.collector)

    async def render(self, rt: Any) -> bytes:
        self.collector.snapshot = await self._sample(rt)
        return await asyncio.to_thread(generate_latest, self.registry)

    @staticmethod
    async def _sample(rt: Any) -> dict[str, Any]:
        from bossman_server.api import views

        agents = await rt.registry.all()
        by_status: dict[tuple[str, str], int] = {}
        for a in agents:
            k = (a.info.namespace, a.status.value)
            by_status[k] = by_status.get(k, 0) + 1
        stats = await rt.tasks.stats()
        attempts: dict[tuple[str, str], int] = {}
        for e in rt.engine.ledger.entries:
            k = (e.strategy.value, e.outcome.value)
            attempts[k] = attempts.get(k, 0) + 1
        incidents = views.incident_list(rt)
        lock_snap = await rt.locks.snapshot()
        limits = await rt.limits.snapshot()
        circuits = await rt.circuits.snapshot()
        managed: dict[str, int] = {}
        for m in rt.managed.status():
            managed[m["state"]] = managed.get(m["state"], 0) + 1
        return {
            "agents": by_status,
            "tasks": {k: v for k, v in stats.items() if k != "DLQ_depth"},
            "dlq": stats.get("DLQ_depth", 0),
            "incidents_active": sum(1 for i in incidents if i["state"] in ("active", "escalated", "failed")),
            "attempts": attempts,
            "events_published": rt.bus.stats["published"],
            "heartbeat_ages": [(a.agent_id, a.info.name, a.info.namespace, round(a.heartbeat_age_seconds, 1))
                               for a in agents if a.is_alive],
            "token_rates": [(a.agent_id, a.info.name, round(a.tokens_per_minute, 1)) for a in agents if a.is_alive],
            "context": [(a.agent_id, a.info.name, a.resource_usage.context_utilisation) for a in agents if a.is_alive],
            "leases": len(lock_snap["leases"]),
            "limits": {l["name"]: l["in_use"] for l in limits},
            "circuits_open": sum(1 for c in circuits if c["state"] == "OPEN"),
            "approvals_pending": await rt.approvals.pending_count(),
            "managed": managed,
            "is_leader": 1 if rt.is_leader else 0,
            "directives_issued": rt.directives.issued_total,
        }
