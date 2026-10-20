import { Link, useParams } from "react-router-dom";
import { useQuery } from "../lib/useQuery";
import type { AgentDetailView, BossEvent } from "../types";
import { agentTone, healthTone, num, pct, relTime, fmtDuration, fmtTime, severityTone, taskTone } from "../lib/format";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";
import clsx from "clsx";

const DIRECTIVES: { type: string; label: string; danger?: boolean }[] = [
  { type: "PAUSE", label: "Pause" },
  { type: "RESUME", label: "Resume" },
  { type: "COMPACT_CONTEXT", label: "Compact context" },
  { type: "FALLBACK_MODEL", label: "Fallback model" },
  { type: "RESTORE_MODEL", label: "Restore model" },
  { type: "ABORT_TASK", label: "Abort task", danger: true },
  { type: "SHUTDOWN", label: "Shut down", danger: true },
];

export function AgentDetail() {
  const { id } = useParams();
  const { data, loading, error, refetch } = useQuery<AgentDetailView>(`/api/agents/${id}`);
  const { info } = useSession();
  const act = useAction();
  const canOperate = Boolean(info?.can_operate);

  if (loading && !data) {
    return <div className="page"><div className="state"><div className="spinner" /><div>Loading agent…</div></div></div>;
  }
  if (error || !data) {
    return <div className="page"><div className="banner bad"><strong>Failed to load agent:</strong> {error?.message}</div></div>;
  }
  const agent = data.agent;
  const cfg = data.configuration;
  const send = (type: string) =>
    act(`${type} sent`, () => api.post(`/api/agents/${agent.agent_id}/directives`, { type, reason: "dashboard" }))
      .then(refetch);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{agent.name}</h1>
          <div className="page-sub mono">{agent.agent_id} · {cfg.namespace} · {agent.role} · epoch {cfg.epoch}</div>
        </div>
        <div className="head-right">
          <span className={clsx("badge", agentTone(agent.status))}>{agent.status}</span>
          {agent.is_deadlock_suspect && <span className="badge bad">Deadlock suspect</span>}
          <span className="badge muted">{cfg.transport}</span>
        </div>
      </div>

      {canOperate && (
        <div className="panel">
          <div className="panel-head"><h2>Control</h2><div className="dim">directives are delivered on the next heartbeat or immediately to long-polling agents</div></div>
          <div className="panel-body toolbar">
            {DIRECTIVES.map((d) => (
              <button key={d.type} className={clsx("btn sm", d.danger && "danger")} onClick={() => send(d.type)}>{d.label}</button>
            ))}
            <button className="btn sm" onClick={() => act("Restart requested", () =>
              api.post(`/api/agents/${agent.agent_id}/restart`, { reason: "dashboard" })).then(refetch)}>Restart</button>
            <button className="btn sm danger" onClick={() => act("Locks released", () =>
              api.post(`/api/agents/${agent.agent_id}/release-resources`, { reason: "dashboard" })).then(refetch)}>Release locks</button>
          </div>
        </div>
      )}

      <div className="grid c4">
        <div className="panel metric">
          <div className="label">Health</div>
          <div className="value">{data.health.overall_status}</div>
          <div className="sub"><span className={clsx("dot", agent.is_healthy ? "ok" : "warn")} /> heartbeat {agent.heartbeat_age_s}s ago</div>
        </div>
        <div className="panel metric">
          <div className="label">Tasks</div>
          <div className="value">{num(agent.total_tasks_completed)}</div>
          <div className="sub"><span className="dot bad" /> {num(agent.total_tasks_failed)} failed · {agent.consecutive_failures} in a row</div>
        </div>
        <div className="panel metric">
          <div className="label">Context</div>
          <div className="value">{pct(agent.context_utilisation)}</div>
          <div className="sub">{num(agent.tokens_used)} tokens · {pct(agent.token_fraction)} of budget</div>
        </div>
        <div className="panel metric">
          <div className="label">Uptime</div>
          <div className="value">{fmtDuration(agent.uptime_s)}</div>
          <div className="sub">restarts {data.restart_count_in_window}/{data.restart_budget} in window</div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Health levels</h2></div>
          <div className="panel-body">
            <dl className="kv">
              {Object.entries(data.health.levels).map(([level, r]) => (
                <div key={level} style={{ display: "contents" }}>
                  <dt>{level}</dt>
                  <dd><span className={clsx("badge", healthTone(r.status))}>{r.status}</span> <span className="dim">{r.detail}</span></dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Configuration</h2></div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Tier</dt><dd>{cfg.permission_tier}</dd>
              <dt>Capabilities</dt><dd className="mono">{cfg.capabilities.length ? cfg.capabilities.join(", ") : "— (unrestricted)"}</dd>
              <dt>Supervisor</dt><dd>{cfg.supervisor ?? "—"}</dd>
              <dt>Managed</dt><dd>{cfg.managed_name ?? "—"}</dd>
              <dt>Singleton key</dt><dd className="mono">{cfg.singleton_key ?? "—"}</dd>
              <dt>SDK</dt><dd className="mono">{cfg.sdk ?? "in-process"}</dd>
              <dt>Resources held</dt><dd>{agent.resources_held.join(", ") || "none"}</dd>
              <dt>Waiting for</dt><dd>{agent.resources_waiting.join(", ") || "none"}</dd>
            </dl>
          </div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Directives</h2></div>
          <div className="panel-body flush">
            <table className="dt">
              <thead><tr><th>Type</th><th>Status</th><th>Issued by</th><th>When</th></tr></thead>
              <tbody>
                {[...data.directives.outstanding, ...data.directives.recent].map((d) => (
                  <tr key={d.directive_id}>
                    <td className="mono">{d.type}</td>
                    <td><span className={clsx("badge", d.status === "ACKED" ? "ok" : d.status === "FAILED" || d.status === "EXPIRED" ? "bad" : "warn")}>{d.status}</span></td>
                    <td className="dim">{d.issued_by}</td>
                    <td>{relTime(d.created_at)}</td>
                  </tr>
                ))}
                {!data.directives.outstanding.length && !data.directives.recent.length && (
                  <tr><td colSpan={4} className="dim">No directives yet.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Probes &amp; flags</h2></div>
          <div className="panel-body">
            <dl className="kv">
              {Object.entries(data.probes).map(([name, p]) => (
                <div key={name} style={{ display: "contents" }}>
                  <dt>probe:{name}</dt>
                  <dd><span className={clsx("badge", p.passed ? "ok" : "bad")}>{p.passed ? "PASS" : "FAIL"}</span> <span className="dim">{p.detail}</span></dd>
                </div>
              ))}
              {Object.entries(data.flags).map(([k, v]) => (
                <div key={k} style={{ display: "contents" }}>
                  <dt>{k}</dt><dd><span className={clsx("badge", v ? "ok" : "bad")}>{v ? "on" : "off"}</span></dd>
                </div>
              ))}
              {!Object.keys(data.probes).length && !Object.keys(data.flags).length && <><dt>—</dt><dd className="dim">No probes or flag rules.</dd></>}
            </dl>
          </div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Recent tasks</h2></div>
          <div className="panel-body flush">
            <table className="dt">
              <thead><tr><th>Task</th><th>Status</th><th>Attempts</th></tr></thead>
              <tbody>
                {data.tasks.slice(-15).reverse().map((t) => (
                  <tr key={t.task_id}>
                    <td><Link className="link" to={`/tasks/${t.task_id}`}>{t.title}</Link></td>
                    <td><span className={clsx("badge", taskTone(t.status))}>{t.status}</span></td>
                    <td className="num">{t.attempt_count}/{t.max_attempts}</td>
                  </tr>
                ))}
                {!data.tasks.length && <tr><td colSpan={3} className="dim">No tasks yet.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Events</h2></div>
          <div className="events">
            {data.events.slice(0, 40).map((e: BossEvent) => (
              <div key={e.seq} className="ev">
                <time>{fmtTime(e.occurred_at)}</time>
                <span className={clsx("dot", severityTone(e.severity))} />
                <div><div className="ev-msg">{e.message}</div><div className="ev-meta">{e.event_type}</div></div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {agent.last_error && (
        <div className="panel">
          <div className="panel-head"><h2 className="tone-bad">Last error</h2><div className="dim">{relTime(agent.last_error_at)}</div></div>
          <div className="panel-body"><pre className="json">{agent.last_error}</pre></div>
        </div>
      )}
    </div>
  );
}
