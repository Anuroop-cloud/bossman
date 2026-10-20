"""
control.tracing
───────────────
Causal trace linkage (zylos.md §Part7): "A single tool failure at step 2 that
silently corrupts steps 3–10 is invisible in LLM-first observability tools
that log independent events without causal links."

  * Every task carries a W3C trace id. Child tasks (parent_task_id) and
    tasks submitted with a `traceparent` join the same trace, so a whole
    multi-agent pipeline is one trace.
  * Every event BOSSman publishes for a task carries that trace id;
    `/api/traces/{trace_id}` reassembles the pipeline (tasks + events).
  * Optional OTLP export (OTEL_EXPORTER_OTLP_ENDPOINT + the `otel` extra):
    one span per task attempt and per recovery incident, using the same
    trace ids — so Langfuse, LangSmith, Jaeger, Honeycomb or Datadog show
    the agents' own spans and BOSSman's spans in one tree.
"""

from __future__ import annotations

import logging
import os
import re
import secrets
from datetime import datetime
from typing import Any

from bossman_server.contracts.events import BossmanEvent, EventType
from bossman_server.core.event_bus import EventBus

log = logging.getLogger(__name__)

TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


def parse_traceparent(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    m = TRACEPARENT.match(value.strip().lower())
    if not m or m.group(1) == "0" * 32:
        return None
    return m.group(1), m.group(2)


def new_span_id() -> str:
    return secrets.token_hex(8)


def format_traceparent(trace_id: str, span_id: str | None = None, sampled: bool = True) -> str:
    return f"00-{trace_id}-{span_id or new_span_id()}-{'01' if sampled else '00'}"


class OtelExporter:
    """Turns task / recovery lifecycle events into OpenTelemetry spans."""

    def __init__(self, bus: EventBus, *, span_exporter: Any | None = None, service_name: str = "bossman") -> None:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

        if span_exporter is None:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            span_exporter = OTLPSpanExporter()
            processor = BatchSpanProcessor(span_exporter)
        else:
            processor = SimpleSpanProcessor(span_exporter)
        self.provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        self.provider.add_span_processor(processor)
        self.tracer = self.provider.get_tracer("bossman")
        self._open: dict[str, Any] = {}
        bus.subscribe(self._on_event)

    @classmethod
    def from_env(cls, bus: EventBus) -> OtelExporter | None:
        if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") and not os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
            return None
        try:
            return cls(bus, service_name=os.getenv("OTEL_SERVICE_NAME", "bossman"))
        except ImportError:
            log.warning("OTEL endpoint configured but opentelemetry-sdk is not installed "
                        "(pip install 'bossman-server[otel]')")
            return None

    def _parent_ctx(self, trace_id: str | None):  # noqa: ANN202
        from opentelemetry import trace
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

        if not trace_id or len(trace_id) != 32:
            return None
        ctx = SpanContext(trace_id=int(trace_id, 16), span_id=int(new_span_id(), 16), is_remote=True,
                          trace_flags=TraceFlags(TraceFlags.SAMPLED))
        return trace.set_span_in_context(NonRecordingSpan(ctx))

    @staticmethod
    def _ns(dt: datetime) -> int:
        return int(dt.timestamp() * 1e9)

    async def _on_event(self, e: BossmanEvent) -> None:
        try:
            self._handle(e)
        except Exception:  # noqa: BLE001 - telemetry never breaks the bus
            log.debug("otel export failed", exc_info=True)

    def _handle(self, e: BossmanEvent) -> None:
        from opentelemetry.trace import Status, StatusCode

        et = e.event_type
        if et == EventType.TASK_STARTED and e.task_id:
            span = self.tracer.start_span(
                f"task {e.task_id}", context=self._parent_ctx(e.trace_id), start_time=self._ns(e.occurred_at),
                attributes={"bossman.task_id": e.task_id, "bossman.agent_id": e.agent_id or "",
                            "bossman.namespace": e.namespace or ""})
            old = self._open.pop(f"task:{e.task_id}", None)
            if old:
                old.end()
            self._open[f"task:{e.task_id}"] = span
        elif et in (EventType.TASK_COMPLETED, EventType.TASK_FAILED, EventType.TASK_CANCELLED,
                    EventType.TASK_QUEUED_DLQ) and e.task_id:
            span = self._open.pop(f"task:{e.task_id}", None)
            if span is not None:
                if et != EventType.TASK_COMPLETED:
                    span.set_status(Status(StatusCode.ERROR, e.message[:200]))
                    span.set_attribute("bossman.error", str(e.payload.get("error", ""))[:300])
                span.end(end_time=self._ns(e.occurred_at))
        elif et == EventType.TASK_PROGRESS and e.task_id:
            span = self._open.get(f"task:{e.task_id}")
            if span is not None:
                span.add_event("progress", {"step": str(e.payload.get("step", ""))[:200]},
                               timestamp=self._ns(e.occurred_at))
        elif et == EventType.RECOVERY_STARTED:
            iid = e.payload.get("incident_id")
            if iid:
                self._open[f"inc:{iid}"] = self.tracer.start_span(
                    f"recovery {e.payload.get('strategy')}", start_time=self._ns(e.occurred_at),
                    attributes={"bossman.incident_id": iid, "bossman.agent_id": e.agent_id or "",
                                "bossman.failure_type": e.payload.get("failure_type", "")})
        elif et in (EventType.EVALUATION_PASSED, EventType.EVALUATION_FAILED):
            span = self._open.pop(f"inc:{e.payload.get('incident_id')}", None)
            if span is not None:
                if et == EventType.EVALUATION_FAILED:
                    span.set_status(Status(StatusCode.ERROR, "recovery not verified"))
                span.end(end_time=self._ns(e.occurred_at))

    def shutdown(self) -> None:
        for span in self._open.values():
            span.end()
        self._open.clear()
        self.provider.shutdown()
