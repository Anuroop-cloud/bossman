import { Link, useParams } from "react-router-dom";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";
import type { TaskDetailView } from "../types";
import { fmtDateTime, fmtTime, relTime, severityTone, taskTone } from "../lib/format";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";

export function TaskDetail() {
  const { id } = useParams();
  const { data, loading, error, refetch } = useQuery<TaskDetailView>(`/api/tasks/${id}`);
  const { info } = useSession();
  const act = useAction();
  if (loading && !data) return <div className="page"><div className="state"><div className="spinner" /></div></div>;
  if (error || !data) return <div className="page"><div className="banner bad">Failed to load task: {error?.message}</div></div>;
  const t = data.task;
  const inDlq = t.status === "DEAD_LETTER" || t.status === "QUARANTINED";
  const live = ["PENDING", "BLOCKED", "ASSIGNED", "RUNNING"].includes(t.status);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{t.title}</h1>
          <div className="page-sub mono">{t.task_id} · {t.namespace} · priority {t.priority}</div>
        </div>
        <div className="head-right toolbar">
          <span className={clsx("badge", taskTone(t.status))}>{t.status}</span>
          {t.trace_id && <Link className="btn sm" to={`/traces/${t.trace_id}`}>View trace</Link>}
          {info?.can_operate && inDlq && (
            <button className="btn sm primary" onClick={() =>
              act("Task replayed", () => api.post(`/api/tasks/${t.task_id}/replay`, { reason: "dashboard" })).then(refetch)}>Replay</button>
          )}
          {info?.is_admin && inDlq && (
            <button className="btn sm danger" onClick={() =>
              act("Task discarded", () => api.post(`/api/tasks/${t.task_id}/discard`, { reason: "dashboard" })).then(refetch)}>Discard</button>
          )}
          {info?.can_operate && live && (
            <button className="btn sm danger" onClick={() =>
              act("Task cancelled", () => api.post(`/api/tasks/${t.task_id}/cancel`, { reason: "dashboard" })).then(refetch)}>Cancel</button>
          )}
        </div>
      </div>

      {(t.dlq_reason || t.quarantine_reason) && (
        <div className="banner bad">{t.quarantine_reason ? `Quarantined: ${t.quarantine_reason}` : `Dead-lettered: ${t.dlq_reason}`}</div>
      )}

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Details</h2></div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Assigned to</dt><dd>{t.assigned_to ? <Link className="link mono" to={`/agents/${t.assigned_to}`}>{t.assigned_to}</Link> : "—"}</dd>
              <dt>Role</dt><dd>{t.required_role ?? "any"}</dd>
              <dt>Attempts</dt><dd>{t.attempt_count} / {t.max_attempts}</dd>
              <dt>Created</dt><dd>{fmtDateTime(t.created_at)}</dd>
              <dt>Retry after</dt><dd>{t.not_before ? fmtDateTime(t.not_before) : "—"}</dd>
              <dt>Lease expires</dt><dd>{t.lease_expires_at ? relTime(t.lease_expires_at) : "—"}</dd>
              <dt>Depends on</dt><dd className="mono">{t.depends_on.length ? t.depends_on.map((d) => <Link key={d} className="link" to={`/tasks/${d}`}>{d} </Link>) : "—"}</dd>
              <dt>Parent</dt><dd>{t.parent_task_id ? <Link className="link mono" to={`/tasks/${t.parent_task_id}`}>{t.parent_task_id}</Link> : "—"}</dd>
              <dt>Last error</dt><dd className="tone-bad">{t.last_error ?? "—"}</dd>
            </dl>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Attempts</h2></div>
          <div className="panel-body flush">
            <table className="dt">
              <thead><tr><th>#</th><th>Agent</th><th>Outcome</th><th>Started</th></tr></thead>
              <tbody>
                {t.attempts.map((a, i) => (
                  <tr key={i}>
                    <td className="num">{a.attempt}</td>
                    <td className="mono">{a.agent_id}</td>
                    <td><span className={clsx("badge", a.outcome === "completed" ? "ok" : a.outcome === "running" ? "info" : "bad")}>{a.outcome}</span>
                      {a.error && <div className="cell-sub">{a.error}</div>}</td>
                    <td>{fmtTime(a.started_at)}</td>
                  </tr>
                ))}
                {!t.attempts.length && <tr><td colSpan={4} className="dim">Not attempted yet.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Input</h2></div>
          <div className="panel-body"><pre className="json">{JSON.stringify(t.input_data, null, 2)}</pre></div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Output</h2></div>
          <div className="panel-body"><pre className="json">{JSON.stringify(t.output_data, null, 2)}</pre></div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Checkpoints</h2></div>
          <div className="panel-body">
            {data.checkpoints.length ? (
              <div className="timeline">
                {data.checkpoints.map((c) => (
                  <div key={c.seq} className="tl-item">
                    <span className="dot info" />
                    <div><strong>#{c.seq} {c.phase}</strong> <span className="dim mono">{c.agent_id}</span>
                      <pre className="json">{JSON.stringify(c.state, null, 2)}</pre></div>
                  </div>
                ))}
              </div>
            ) : <div className="dim">No checkpoints recorded.</div>}
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Events</h2></div>
          <div className="events">
            {data.events.map((e) => (
              <div key={e.seq} className="ev">
                <time>{fmtTime(e.occurred_at)}</time>
                <span className={clsx("dot", severityTone(e.severity))} />
                <div><div className="ev-msg">{e.message}</div><div className="ev-meta">{e.event_type}</div></div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
