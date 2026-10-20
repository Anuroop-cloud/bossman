import { Link, useParams } from "react-router-dom";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";
import type { TraceView } from "../types";
import { fmtTime, severityTone, taskTone } from "../lib/format";

/** One multi-agent pipeline as a causal tree: tasks (parent / depends_on) + every event in order. */
export function Trace() {
  const { id } = useParams();
  const { data, error } = useQuery<TraceView>(`/api/traces/${id}`);
  if (error) return <div className="page"><div className="banner bad">{error.message}</div></div>;
  if (!data) return <div className="page"><div className="state"><div className="spinner" /></div></div>;

  const children = new Map<string | null, typeof data.tasks>();
  for (const t of data.tasks) {
    const k = t.parent_task_id && data.tasks.some((x) => x.task_id === t.parent_task_id) ? t.parent_task_id : null;
    children.set(k, [...(children.get(k) ?? []), t]);
  }
  const render = (parent: string | null, depth: number): JSX.Element[] =>
    (children.get(parent) ?? []).flatMap((t) => [
      <tr key={t.task_id}>
        <td style={{ paddingLeft: 14 + depth * 22 }}>
          <Link className="link" to={`/tasks/${t.task_id}`}>{t.title}</Link>
          <div className="cell-sub">{t.task_id}{t.depends_on.length ? ` · after ${t.depends_on.join(", ")}` : ""}</div>
        </td>
        <td><span className={clsx("badge", taskTone(t.status))}>{t.status}</span></td>
        <td className="mono dim">{t.assigned_to ?? "—"}</td>
        <td className="num">{t.attempt_count}/{t.max_attempts}</td>
      </tr>,
      ...render(t.task_id, depth + 1),
    ]);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Trace</h1>
          <div className="page-sub mono">{data.trace_id} · {data.tasks.length} task(s) · {data.events.length} event(s)</div>
        </div>
      </div>
      <div className="panel">
        <div className="panel-head"><h2>Tasks</h2></div>
        <div className="panel-body flush">
          <table className="dt">
            <thead><tr><th>Task</th><th>Status</th><th>Agent</th><th>Attempts</th></tr></thead>
            <tbody>{render(null, 0)}</tbody>
          </table>
        </div>
      </div>
      <div className="panel">
        <div className="panel-head"><h2>Timeline</h2></div>
        <div className="events" style={{ maxHeight: 640 }}>
          {data.events.map((e) => (
            <div key={e.event_id} className="ev">
              <time>{fmtTime(e.occurred_at)}</time>
              <span className={clsx("dot", severityTone(e.severity))} />
              <div>
                <div className="ev-msg">{e.message}</div>
                <div className="ev-meta">{e.event_type}{e.agent_id ? ` · ${e.agent_id}` : ""}{e.task_id ? ` · ${e.task_id}` : ""}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
