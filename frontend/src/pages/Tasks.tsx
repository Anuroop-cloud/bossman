import { useQuery } from "../lib/useQuery";
import type { Task } from "../types";
import { taskTone, relTime, pct } from "../lib/format";
import clsx from "clsx";

export function Tasks() {
  const { data, loading, error } = useQuery<{ tasks: Task[] }>("/api/tasks");

  if (loading && !data) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading tasks...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load tasks:</strong> {error.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Tasks</h1>
          <div className="page-sub">Global task queue and execution status</div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-body flush">
          <div className="table-wrap">
            <table className="dt">
              <thead>
                <tr>
                  <th>Task ID</th>
                  <th>Title</th>
                  <th>Status</th>
                  <th>Progress</th>
                  <th>Assigned To</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {data?.tasks.map(t => (
                  <tr key={t.task_id}>
                    <td className="mono">{t.task_id.slice(0, 8)}</td>
                    <td>
                      <div>{t.title}</div>
                      <div className="cell-sub">{t.description || "—"}</div>
                    </td>
                    <td>
                      <span className={clsx("badge", taskTone(t.status))}>
                        {t.status}
                      </span>
                    </td>
                    <td>
                      {t.progress_pct != null ? (
                         <div className="bar-row" style={{ width: 80 }}>
                           <div className="bar info">
                             <i style={{ width: `${t.progress_pct * 100}%` }} />
                           </div>
                           <div className="bar-meta">
                             <span>{pct(t.progress_pct)}</span>
                           </div>
                         </div>
                      ) : "—"}
                    </td>
                    <td className="mono dim">{t.assigned_to ? t.assigned_to.slice(0, 8) : "—"}</td>
                    <td>{relTime(t.created_at)}</td>
                  </tr>
                ))}
                {!data?.tasks.length && (
                  <tr>
                    <td colSpan={6} className="dim" style={{ textAlign: "center" }}>No tasks found.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
