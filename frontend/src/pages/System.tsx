import { useParams } from "react-router-dom";
import { useQuery } from "../lib/useQuery";
import type { SystemView } from "../types";

export function System() {
  const { data, loading, error } = useQuery<SystemView>("/api/system");

  if (loading && !data) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading system info...</div>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load system info:</strong> {error?.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>System Configuration</h1>
          <div className="page-sub">Environment, runtime, and global settings</div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head">
            <h2>Environment</h2>
          </div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Version</dt><dd>{data.version}</dd>
              <dt>Environment</dt><dd>{data.environment}</dd>
              <dt>Runtime Status</dt><dd>{data.runtime.status}</dd>
              <dt>Python</dt><dd>{data.runtime.python}</dd>
            </dl>
          </div>
        </div>

        <div className="panel">
          <div className="panel-head">
            <h2>LLM Settings</h2>
          </div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Provider</dt><dd>{data.llm.provider}</dd>
              <dt>Configured</dt><dd>{data.llm.configured ? "Yes" : "No"}</dd>
              <dt>Model</dt><dd>{data.llm.model || "—"}</dd>
              <dt>Diagnostician</dt><dd>{data.llm.diagnostician}</dd>
            </dl>
          </div>
        </div>
      </div>
    </div>
  );
}
