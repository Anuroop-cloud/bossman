import { useQuery } from "../lib/useQuery";
import type { Agent } from "../types";
import { agentTone, pct, num, relTime } from "../lib/format";
import clsx from "clsx";
import { Link } from "react-router-dom";

export function Agents() {
  const { data, loading, error } = useQuery<{ agents: Agent[] }>("/api/agents");

  if (loading && !data) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading agents...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load agents:</strong> {error.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Agents</h1>
          <div className="page-sub">Active autonomous agents and supervisors</div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-body flush">
          <div className="table-wrap">
            <table className="dt">
              <thead>
                <tr>
                  <th>Agent ID</th>
                  <th>Name / Role</th>
                  <th>Status</th>
                  <th>Context Util.</th>
                  <th>Tokens Used</th>
                  <th>Task</th>
                  <th>Uptime</th>
                </tr>
              </thead>
              <tbody>
                {data?.agents.map(a => (
                  <tr key={a.agent_id} className="clickable">
                    <td className="mono">
                      <Link to={`/agents/${a.agent_id}`} style={{ display: 'block', textDecoration: 'underline', color: 'var(--info)' }}>
                        {a.agent_id.slice(0, 8)}
                      </Link>
                    </td>
                    <td>
                      <div>{a.name}</div>
                      <div className="cell-sub">{a.role}</div>
                    </td>
                    <td>
                      <span className={clsx("badge", agentTone(a.status))}>
                        {a.status}
                      </span>
                    </td>
                    <td>
                      <div className="bar-row" style={{ width: 80 }}>
                        <div className={clsx("bar", a.context_utilisation > 0.8 ? "warn" : "ok")}>
                          <i style={{ width: `${a.context_utilisation * 100}%` }} />
                        </div>
                        <div className="bar-meta">
                          <span>{pct(a.context_utilisation)}</span>
                        </div>
                      </div>
                    </td>
                    <td className="num">{num(a.tokens_used)}</td>
                    <td className="mono dim">{a.current_task_id ? a.current_task_id.slice(0, 8) : "—"}</td>
                    <td>{relTime(a.started_at)}</td>
                  </tr>
                ))}
                {!data?.agents.length && (
                  <tr>
                    <td colSpan={7} className="dim" style={{ textAlign: "center" }}>No agents running.</td>
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
