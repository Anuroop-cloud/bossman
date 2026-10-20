import { useState } from "react";
import { useNavigate } from "react-router-dom";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";
import type { Task } from "../types";
import { taskTone, relTime } from "../lib/format";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";

interface TasksView {
  tasks: Task[];
  dlq: Task[];
  stats: Record<string, number>;
}

export function Tasks() {
  const { data, loading, error, refetch } = useQuery<TasksView>("/api/tasks");
  const [tab, setTab] = useState<"all" | "dlq">("all");
  const [filter, setFilter] = useState("");
  const nav = useNavigate();
  const { info } = useSession();
  const act = useAction();

  if (loading && !data) {
    return <div className="page"><div className="state"><div className="spinner" /><div>Loading tasks…</div></div></div>;
  }
  if (error && !data) {
    return <div className="page"><div className="banner bad"><strong>Failed to load tasks:</strong> {error.message}</div></div>;
  }
  const rows = (tab === "all" ? data?.tasks : data?.dlq) ?? [];
  const shown = rows.filter((t) => !filter || `${t.title} ${t.status} ${t.task_id} ${t.assigned_to ?? ""}`
    .toLowerCase().includes(filter.toLowerCase()));
  const s = data?.stats ?? {};

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Tasks</h1>
          <div className="page-sub">
            running {s.running ?? 0} · queued {s.queued ?? 0} · blocked {s.blocked ?? 0} · completed {s.completed ?? 0} ·
            dead-letter {s.dlq ?? 0}
          </div>
        </div>
        <div className="head-right toolbar">
          <input type="search" placeholder="Filter…" value={filter} onChange={(e) => setFilter(e.target.value)} />
        </div>
      </div>

      <div className="tabs">
        <button className={clsx("tab", tab === "all" && "on")} onClick={() => setTab("all")}>All tasks</button>
        <button className={clsx("tab", tab === "dlq" && "on")} onClick={() => setTab("dlq")}>
          Dead letter / quarantine ({data?.dlq.length ?? 0})
        </button>
      </div>

      <div className="panel">
        <div className="panel-body flush">
          <div className="table-wrap">
            <table className="dt">
              <thead>
                <tr>
                  <th>Task</th><th>Status</th><th>Progress</th><th>Attempts</th><th>Assigned to</th><th>Created</th>
                  {tab === "dlq" && <th>Actions</th>}
                </tr>
              </thead>
              <tbody>
                {shown.map((t) => (
                  <tr key={t.task_id} className="clickable" onClick={() => nav(`/tasks/${t.task_id}`)}>
                    <td>
                      <div>{t.title}</div>
                      <div className="cell-sub">{t.task_id} {t.dlq_reason ? `· ${t.dlq_reason}` : ""}</div>
                    </td>
                    <td><span className={clsx("badge", taskTone(t.status))}>{t.status}</span></td>
                    <td>
                      {t.progress_pct != null ? (
                        <div className="bar-row" style={{ width: 90 }}>
                          <div className="bar info"><i style={{ width: `${t.progress_pct}%` }} /></div>
                          <div className="bar-meta"><span>{t.current_step ?? `${t.progress_pct}%`}</span></div>
                        </div>
                      ) : "—"}
                    </td>
                    <td className="num">{t.attempt_count}/{t.max_attempts}</td>
                    <td className="mono dim">{t.assigned_to ?? "—"}</td>
                    <td>{relTime(t.created_at)}</td>
                    {tab === "dlq" && (
                      <td onClick={(e) => e.stopPropagation()}>
                        <div className="toolbar">
                          <button className="btn sm" disabled={!info?.can_operate} onClick={() =>
                            act("Task replayed", () => api.post(`/api/tasks/${t.task_id}/replay`, { reason: "dashboard" })).then(refetch)}>
                            Replay
                          </button>
                          <button className="btn sm danger" disabled={!info?.is_admin} onClick={() =>
                            act("Task discarded", () => api.post(`/api/tasks/${t.task_id}/discard`, { reason: "dashboard" })).then(refetch)}>
                            Discard
                          </button>
                        </div>
                      </td>
                    )}
                  </tr>
                ))}
                {!shown.length && (
                  <tr><td colSpan={7} className="dim" style={{ textAlign: "center" }}>No tasks.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
