import { useParams, Link } from "react-router-dom";
import { useQuery } from "../lib/useQuery";
import type { IncidentDetail as IncidentDetailType } from "../types";
import { incidentTone, relTime, healthTone } from "../lib/format";
import clsx from "clsx";

export function IncidentDetail() {
  const { id } = useParams();
  const { data: incident, loading, error } = useQuery<IncidentDetailType>(`/api/incidents/${id}`);

  if (loading && !incident) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading incident...</div>
        </div>
      </div>
    );
  }

  if (error || !incident) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load incident:</strong> {error?.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{incident.failure_type}</h1>
          <div className="page-sub mono">{incident.incident_id}</div>
        </div>
        <div className="head-right">
          <div className={clsx("badge", incidentTone(incident.state))}>
            {incident.state}
          </div>
          {incident.is_critical && (
            <div className="badge bad">Critical</div>
          )}
        </div>
      </div>

      <div className="panel">
        <div className="panel-body">
          <dl className="kv">
            <dt>Agent</dt>
            <dd>
              {incident.agent_id ? (
                <Link to={`/agents/${incident.agent_id}`} className="link mono">{incident.agent_id}</Link>
              ) : "—"}
            </dd>
            <dt>Detected</dt>
            <dd>{relTime(incident.detected_at)}</dd>
            <dt>Detail</dt>
            <dd>{incident.detail}</dd>
            <dt>Stage</dt>
            <dd>{incident.stage}</dd>
          </dl>
        </div>
      </div>

      {incident.diagnosis && (
        <div className="panel">
          <div className="panel-head">
            <h2>Diagnosis</h2>
          </div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Root Cause</dt><dd>{incident.diagnosis.root_cause}</dd>
              <dt>Rule Matched</dt><dd>{incident.diagnosis.rule_matched}</dd>
              <dt>Confidence</dt><dd>{incident.diagnosis.confidence * 100}%</dd>
              <dt>Strategy</dt><dd>{incident.diagnosis.recommended_strategy}</dd>
            </dl>
          </div>
        </div>
      )}

      {incident.attempts.length > 0 && (
        <div className="panel">
          <div className="panel-head">
            <h2>Recovery Attempts</h2>
          </div>
          <div className="panel-body flush">
            <table className="dt">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Strategy</th>
                  <th>Outcome</th>
                  <th>Duration</th>
                </tr>
              </thead>
              <tbody>
                {incident.attempts.map(att => (
                  <tr key={att.attempt_id}>
                    <td>{att.attempt_number}</td>
                    <td>{att.strategy}</td>
                    <td>
                      <span className={clsx("badge", att.outcome === "SUCCESS" ? "ok" : "bad")}>
                        {att.outcome}
                      </span>
                    </td>
                    <td>{att.duration_s ? `${att.duration_s.toFixed(1)}s` : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      
      {incident.evaluation && (
        <div className="panel">
          <div className="panel-head">
            <h2>Evaluation</h2>
          </div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Passed</dt>
              <dd>
                <span className={clsx("badge", incident.evaluation.passed ? "ok" : "bad")}>
                  {incident.evaluation.passed ? "PASS" : "FAIL"}
                </span>
              </dd>
              <dt>Outcome</dt><dd>{incident.evaluation.outcome}</dd>
              <dt>Notes</dt><dd>{incident.evaluation.notes}</dd>
            </dl>
            
            {incident.evaluation.health && (
              <div style={{ marginTop: 16 }}>
                <h3 style={{ marginBottom: 8 }}>Health Report</h3>
                <div className="grid c4">
                  {Object.entries(incident.evaluation.health.levels).map(([level, data]) => (
                    <div key={level} className="metric" style={{ border: '1px solid var(--border)', borderRadius: '6px' }}>
                      <div className="label">{level.replace("L", "Level ")}</div>
                      <div className={clsx("value", `tone-${healthTone(data.status)}`)}>{data.status}</div>
                      <div className="sub">{data.detail}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
