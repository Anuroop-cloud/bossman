import { useState } from "react";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";
import type { CircuitView, LimitView, ManagedProc, SupervisorView } from "../types";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";
import { pct } from "../lib/format";

const procTone = (s: string) =>
  s === "running" ? "ok" : s === "crashloop" ? "bad" : s === "backoff" || s === "starting" || s === "stopping" ? "warn" : "muted";

export function Operations() {
  const { info } = useSession();
  const act = useAction();
  const managed = useQuery<{ managed: ManagedProc[]; active: boolean }>("/api/managed");
  const sups = useQuery<{ supervisors: SupervisorView[] }>("/api/supervisors");
  const limits = useQuery<{ limits: LimitView[]; role_limits: Record<string, number> }>("/api/limits");
  const circuits = useQuery<{ shared: CircuitView[] }>("/api/circuits");
  const [logs, setLogs] = useState<{ id: string; lines: string[] } | null>(null);

  const mAction = (id: string, action: string) =>
    act(`${action} requested`, () => api.post(`/api/managed/${id}/${action}`, { reason: "dashboard" })).then(managed.refetch);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Operations</h1>
          <div className="page-sub">Managed agent processes, supervision trees, fleet-wide limits and circuit breakers.</div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">
          <h2>Managed agents</h2>
          <div className="dim">{managed.data?.active ? "this node is running them (leader)" : "processes run on the leader node"}</div>
        </div>
        <div className="panel-body flush">
          <table className="dt">
            <thead><tr><th>Name</th><th>Runtime</th><th>State</th><th>PID</th><th>Restarts</th><th>Policy</th><th /></tr></thead>
            <tbody>
              {managed.data?.managed.map((m) => (
                <tr key={m.managed_id}>
                  <td><div>{m.name}</div><div className="cell-sub">{m.managed_id} · {m.role}@{m.namespace}</div></td>
                  <td className="mono">{m.runtime === "docker" ? `docker ${m.image}` : m.command.join(" ").slice(0, 60)}</td>
                  <td><span className={clsx("badge", procTone(m.state))}>{m.state}</span>{m.exit_code != null && <div className="cell-sub">exit {m.exit_code}</div>}</td>
                  <td className="num">{m.pid ?? "—"}</td>
                  <td className="num">{m.restarts_in_window}/{m.max_restarts} ({m.total_restarts} total)</td>
                  <td>{m.restart_policy}</td>
                  <td>
                    <div className="toolbar">
                      <button className="btn sm" disabled={!info?.can_operate} onClick={() => mAction(m.managed_id, "restart")}>Restart</button>
                      {m.desired_state === "running"
                        ? <button className="btn sm danger" disabled={!info?.can_operate} onClick={() => mAction(m.managed_id, "stop")}>Stop</button>
                        : <button className="btn sm" disabled={!info?.can_operate} onClick={() => mAction(m.managed_id, "start")}>Start</button>}
                      <button className="btn sm" disabled={!info?.can_operate} onClick={async () => {
                        const r = await api.get<{ lines: string[] }>(`/api/managed/${m.managed_id}/logs`);
                        setLogs({ id: m.name, lines: r.lines });
                      }}>Logs</button>
                    </div>
                  </td>
                </tr>
              ))}
              {!managed.data?.managed.length && (
                <tr><td colSpan={7} className="dim">No managed agents. Create one with <code>bossman managed create -f spec.yaml</code>.</td></tr>
              )}
            </tbody>
          </table>
        </div>
        {logs && (
          <div className="panel-body">
            <div className="toolbar"><strong>{logs.id}</strong><button className="btn sm" onClick={() => setLogs(null)}>Close</button></div>
            <pre className="json">{logs.lines.join("\n") || "(no output)"}</pre>
          </div>
        )}
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Supervision trees</h2></div>
          <div className="panel-body flush">
            <table className="dt">
              <thead><tr><th>Supervisor</th><th>Strategy</th><th>Budget</th><th>Parent</th><th>Members</th></tr></thead>
              <tbody>
                {sups.data?.supervisors.map((s) => (
                  <tr key={s.name}>
                    <td className="mono">{s.name}</td><td>{s.strategy}</td>
                    <td className="num">{s.budget_remaining}/{s.max_restarts} per {s.window_s}s</td>
                    <td className="mono">{s.parent ?? "—"}</td><td className="num">{s.members.length}</td>
                  </tr>
                ))}
                {!sups.data?.supervisors.length && <tr><td colSpan={5} className="dim">No supervisors configured.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Fleet-wide limits</h2></div>
          <div className="panel-body flush">
            <table className="dt">
              <thead><tr><th>Limit</th><th>Concurrency</th><th>Rate</th><th>Utilisation</th></tr></thead>
              <tbody>
                {limits.data?.limits.map((l) => (
                  <tr key={l.name}>
                    <td className="mono">{l.name}</td>
                    <td className="num">{l.in_use}/{l.max_concurrent ?? "∞"}</td>
                    <td className="num">{l.rate_per_s ? `${l.rate_per_s}/s (burst ${l.burst}, ${l.tokens_available} left)` : "—"}</td>
                    <td>{l.utilisation != null ? pct(l.utilisation) : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {limits.data && Object.keys(limits.data.role_limits).length > 0 && (
            <div className="panel-body dim mono">spawn caps: {Object.entries(limits.data.role_limits).map(([k, v]) => `${k}≤${v}`).join(" · ")}</div>
          )}
        </div>
      </div>

      <div className="panel">
        <div className="panel-head"><h2>Shared circuit breakers</h2></div>
        <div className="panel-body flush">
          <table className="dt">
            <thead><tr><th>Dependency</th><th>State</th><th>Failures</th><th>Retry in</th><th>Reason</th><th /></tr></thead>
            <tbody>
              {circuits.data?.shared.map((c) => (
                <tr key={c.key}>
                  <td className="mono">{c.key}</td>
                  <td><span className={clsx("badge", c.state === "CLOSED" ? "ok" : c.state === "OPEN" ? "bad" : "warn")}>{c.state}</span></td>
                  <td className="num">{c.failures}</td>
                  <td className="num">{c.retry_after_s ? `${c.retry_after_s}s` : "—"}</td>
                  <td className="dim">{c.reason ?? c.last_error ?? "—"}</td>
                  <td>
                    {c.state === "CLOSED"
                      ? <button className="btn sm danger" disabled={!info?.can_operate} onClick={() =>
                          act("Circuit opened", () => api.post(`/api/circuits/${c.key}/open`, { reason: "dashboard" })).then(circuits.refetch)}>Open</button>
                      : <button className="btn sm" disabled={!info?.can_operate} onClick={() =>
                          act("Circuit closed", () => api.post(`/api/circuits/${c.key}/close`)).then(circuits.refetch)}>Close</button>}
                  </td>
                </tr>
              ))}
              {!circuits.data?.shared.length && <tr><td colSpan={6} className="dim">No circuits yet — they appear when agents call through <code>agent.guarded()</code>.</td></tr>}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
