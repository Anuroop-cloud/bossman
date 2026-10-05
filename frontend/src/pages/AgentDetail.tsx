import { useParams } from "react-router-dom";
import { useQuery } from "../lib/useQuery";
import type { Agent } from "../types";
import { agentTone, num, pct, relTime, fmtDuration } from "../lib/format";
import clsx from "clsx";

export function AgentDetail() {
  const { id } = useParams();
  const { data: agent, loading, error } = useQuery<Agent>(`/api/agents/${id}`);

  if (loading && !agent) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading agent...</div>
        </div>
      </div>
    );
  }

  if (error || !agent) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load agent:</strong> {error?.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{agent.name}</h1>
          <div className="page-sub mono">{agent.agent_id}</div>
        </div>
        <div className="head-right">
          <div className={clsx("badge", agentTone(agent.status))}>
            {agent.status}
          </div>
          {agent.is_deadlock_suspect && (
            <div className="badge bad">Deadlock Suspect</div>
          )}
        </div>
      </div>

      <div className="grid c4">
        <div className="panel metric">
          <div className="label">Status</div>
          <div className="value">{agent.is_alive ? "Alive" : "Dead"}</div>
          <div className="sub">
            <div><span className={clsx("dot", agent.is_healthy ? "ok" : "warn")} /> {agent.is_healthy ? "Healthy" : "Unhealthy"}</div>
          </div>
        </div>
        <div className="panel metric">
          <div className="label">Tasks Completed</div>
          <div className="value">{num(agent.total_tasks_completed)}</div>
          <div className="sub">
            <div><span className="dot bad" /> {num(agent.total_tasks_failed)} Failed</div>
          </div>
        </div>
        <div className="panel metric">
          <div className="label">Context Utilisation</div>
          <div className="value">{pct(agent.context_utilisation)}</div>
          <div className="sub">
            <div className="bar-row" style={{ width: 80 }}>
              <div className={clsx("bar", agent.context_utilisation > 0.8 ? "warn" : "info")}>
                <i style={{ width: `${agent.context_utilisation * 100}%` }} />
              </div>
            </div>
          </div>
        </div>
        <div className="panel metric">
          <div className="label">Uptime</div>
          <div className="value">{fmtDuration(agent.uptime_s)}</div>
          <div className="sub">
            <div>Started {relTime(agent.started_at)}</div>
          </div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head">
            <h2>Current Task</h2>
          </div>
          <div className="panel-body">
            {agent.current_task_id ? (
              <dl className="kv">
                <dt>Task ID</dt>
                <dd className="mono">{agent.current_task_id}</dd>
                <dt>Step</dt>
                <dd>{agent.current_step || "—"}</dd>
                <dt>Resources Held</dt>
                <dd>{agent.resources_held.length > 0 ? agent.resources_held.join(", ") : "None"}</dd>
                <dt>Waiting For</dt>
                <dd>{agent.resources_waiting.length > 0 ? agent.resources_waiting.join(", ") : "None"}</dd>
              </dl>
            ) : (
              <div className="dim">No active task.</div>
            )}
          </div>
        </div>

        <div className="panel">
          <div className="panel-head">
            <h2>Configuration</h2>
          </div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Role</dt><dd>{agent.role}</dd>
              <dt>Permission Tier</dt><dd>{agent.permission_tier}</dd>
              <dt>Tokens Used</dt><dd>{num(agent.tokens_used)} / {num(agent.tokens_budget)} ({pct(agent.token_fraction)})</dd>
              <dt>API Calls</dt><dd>{num(agent.api_calls_total)} total ({num(agent.api_calls_per_minute)}/min)</dd>
            </dl>
          </div>
        </div>
      </div>
      
      {agent.last_error && (
        <div className="panel">
          <div className="panel-head">
            <h2 className="tone-bad">Last Error</h2>
            <div className="dim">{relTime(agent.last_error_at)}</div>
          </div>
          <div className="panel-body">
            <pre className="json">{agent.last_error}</pre>
          </div>
        </div>
      )}
    </div>
  );
}
