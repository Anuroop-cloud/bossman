import type { Severity, Tone } from "../types";

export function fmtDuration(totalSeconds: number | null | undefined): string {
  if (totalSeconds == null || !Number.isFinite(totalSeconds)) return "—";
  const s = Math.max(0, Math.floor(totalSeconds));
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d) return `${d}d ${h}h`;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleTimeString([], { hour12: false });
}

export function fmtDateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return `${d.toLocaleDateString([], { month: "short", day: "numeric" })} ${d.toLocaleTimeString([], { hour12: false })}`;
}

export function relTime(iso: string | null | undefined, now = Date.now()): string {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return "—";
  const s = Math.round((now - t) / 1000);
  if (s < 5) return "just now";
  return `${fmtDuration(s)} ago`;
}

export function pct(x: number | null | undefined, digits = 0): string {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(digits)}%`;
}

export function num(x: number | null | undefined): string {
  if (x == null) return "—";
  return x.toLocaleString();
}

export function agentTone(status: string): Tone {
  switch (status) {
    case "RUNNING":
      return "ok";
    case "IDLE":
    case "INITIALIZING":
      return "muted";
    case "RECOVERING":
      return "warn";
    case "STUCK":
      return "warn";
    case "FAILED":
      return "bad";
    case "TERMINATED":
      return "muted";
    default:
      return "muted";
  }
}

export function taskTone(status: string): Tone {
  switch (status) {
    case "RUNNING":
      return "ok";
    case "COMPLETED":
      return "info";
    case "FAILED":
    case "DEAD_LETTER":
      return "bad";
    case "PENDING":
    case "ASSIGNED":
      return "muted";
    default:
      return "muted";
  }
}

export function severityTone(s: Severity): Tone {
  return s === "CRITICAL" || s === "ERROR" ? "bad" : s === "WARNING" ? "warn" : s === "DEBUG" ? "muted" : "info";
}

export function incidentTone(state: string): Tone {
  return state === "resolved" ? "ok" : state === "active" ? "warn" : "bad";
}

export function healthTone(s: "PASS" | "WARN" | "FAIL"): Tone {
  return s === "PASS" ? "ok" : s === "WARN" ? "warn" : "bad";
}

/** Use for utilisation bars: green → amber → red. */
export function loadTone(fraction: number): Tone {
  return fraction >= 0.9 ? "bad" : fraction >= 0.75 ? "warn" : "ok";
}

export function shortId(id: string | null | undefined, n = 8): string {
  if (!id) return "—";
  return id.length > n + 4 ? `${id.slice(0, n)}…` : id;
}

export function bucketByMinute(isoTimes: (string | null)[], buckets = 12, bucketMinutes = 5, now = Date.now()) {
  const size = bucketMinutes * 60_000;
  const out = Array.from({ length: buckets }, (_, i) => ({
    start: now - (buckets - i) * size,
    count: 0,
  }));
  for (const iso of isoTimes) {
    if (!iso) continue;
    const t = new Date(iso).getTime();
    const idx = Math.floor((t - (now - buckets * size)) / size);
    if (idx >= 0 && idx < buckets) out[idx].count++;
  }
  return out;
}

export function countBy<T>(items: T[], key: (t: T) => string): Record<string, number> {
  const out: Record<string, number> = {};
  for (const it of items) {
    const k = key(it);
    out[k] = (out[k] ?? 0) + 1;
  }
  return out;
}
