import { useQuery } from "../lib/useQuery";
import type { Incident } from "../types";
import { incidentTone, relTime } from "../lib/format";
import clsx from "clsx";
import { Link } from "react-router-dom";

export function Incidents() {
  const { data, loading, error } = useQuery<{ incidents: Incident[] }>("/api/incidents");

  if (loading && !data) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading incidents...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load incidents:</strong> {error.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Incidents</h1>
          <div className="page-sub">Failure tracking and automated recovery</div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-body flush">
          <div className="table-wrap">
            <table className="dt">
              <thead>
                <tr>
                  <th>Incident ID</th>
                  <th>Agent</th>
                  <th>Type</th>
                  <th>State</th>
                  <th>Stage</th>
                  <th>Detected</th>
                </tr>
              </thead>
              <tbody>
                {data?.incidents.map(inc => (
                  <tr key={inc.incident_id} className="clickable">
                    <td className="mono">
                      <Link to={`/incidents/${inc.incident_id}`} style={{ display: 'block', textDecoration: 'underline', color: 'var(--info)' }}>
                        {inc.incident_id.slice(0, 8)}
                      </Link>
                    </td>
                    <td className="mono dim">{inc.agent_id ? inc.agent_id.slice(0, 8) : "—"}</td>
                    <td>
                      <div>{inc.failure_type}</div>
                      <div className="cell-sub">{inc.detail}</div>
                    </td>
                    <td>
                      <span className={clsx("badge", incidentTone(inc.state))}>
                        {inc.state}
                      </span>
                    </td>
                    <td>
                      <span className="dim mono" style={{ fontSize: '11px' }}>{inc.stage}</span>
                    </td>
                    <td>{relTime(inc.detected_at)}</td>
                  </tr>
                ))}
                {!data?.incidents.length && (
                  <tr>
                    <td colSpan={6} className="dim" style={{ textAlign: "center" }}>No incidents found.</td>
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
