import { useQuery } from "../lib/useQuery";
import type { Json } from "../types";
import { fmtDuration } from "../lib/format";

interface SystemFull {
  version: string;
  environment: string;
  runtime: { status: string; uptime_s: number; python: string; components: Record<string, Record<string, Json>> };
  limits: Record<string, number>;
  llm: { provider: string; configured: boolean; enabled?: boolean; model: string | null; diagnostician: string;
         quality_judge?: boolean; note: string };
  database: { configured: boolean; connected: boolean | null; dialect?: string | null; events_persisted?: number; note: string };
  cluster?: { mode: string; node_id: string; is_leader: boolean; leader_id: string | null; persistence: string };
  security: Record<string, Json>;
  governance?: { approvals_pending: number; flags: number; supervisors: number; role_limits: Record<string, number> };
  heartbeat?: { interval_s: number; timeout_s: number };
}

const yes = (b: unknown) => (b ? "Yes" : "No");

export function System() {
  const { data, loading, error } = useQuery<SystemFull>("/api/system");
  if (loading && !data) return <div className="page"><div className="state"><div className="spinner" /><div>Loading system info…</div></div></div>;
  if (error || !data) return <div className="page"><div className="banner bad"><strong>Failed to load system info:</strong> {error?.message}</div></div>;
  const c = data.cluster;

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>System</h1>
          <div className="page-sub">BOSSman {data.version} · {data.environment} · up {fmtDuration(data.runtime.uptime_s)}</div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head"><h2>Cluster</h2></div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Mode</dt><dd>{c?.mode ?? "standalone"}</dd>
              <dt>This node</dt><dd className="mono">{c?.node_id}</dd>
              <dt>Leader</dt><dd>{c?.is_leader ? "this node" : c?.leader_id ?? "—"}</dd>
              <dt>Persistence</dt><dd>{c?.persistence ?? "memory"}</dd>
              <dt>Heartbeat</dt><dd>every {data.heartbeat?.interval_s}s · timeout {data.heartbeat?.timeout_s}s</dd>
              <dt>Python</dt><dd>{data.runtime.python}</dd>
            </dl>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Database</h2></div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Configured</dt><dd>{yes(data.database.configured)}</dd>
              <dt>Connected</dt><dd>{data.database.connected == null ? "—" : yes(data.database.connected)}</dd>
              <dt>Dialect</dt><dd>{data.database.dialect ?? "—"}</dd>
              <dt>Events persisted</dt><dd>{data.database.events_persisted ?? 0}</dd>
              <dt>Note</dt><dd className="dim">{data.database.note}</dd>
            </dl>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>Security &amp; governance</h2></div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Operator actions</dt><dd>{yes(data.security.operator_actions_enabled)}</dd>
              <dt>Public reads</dt><dd>{yes(data.security.public_reads)}</dd>
              <dt>API keys</dt><dd>{String(data.security.api_keys ?? 0)}</dd>
              <dt>Stored secrets</dt><dd>{String(data.security.secrets ?? 0)}</dd>
              <dt>Policy revision</dt><dd>{String(data.security.policy_revision ?? 0)}</dd>
              <dt>Pending approvals</dt><dd>{data.governance?.approvals_pending ?? 0}</dd>
              <dt>Flag rules</dt><dd>{data.governance?.flags ?? 0}</dd>
            </dl>
          </div>
        </div>
        <div className="panel">
          <div className="panel-head"><h2>LLM</h2></div>
          <div className="panel-body">
            <dl className="kv">
              <dt>Provider</dt><dd>{data.llm.provider}</dd>
              <dt>Credential</dt><dd>{yes(data.llm.configured)}</dd>
              <dt>Enabled</dt><dd>{yes(data.llm.enabled)}</dd>
              <dt>Model</dt><dd className="mono">{data.llm.model ?? "—"}</dd>
              <dt>Diagnostician</dt><dd>{data.llm.diagnostician}</dd>
              <dt>Quality judge</dt><dd>{yes(data.llm.quality_judge)}</dd>
            </dl>
          </div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head"><h2>Components</h2></div>
        <div className="panel-body"><pre className="json">{JSON.stringify(data.runtime.components, null, 2)}</pre></div>
      </div>
    </div>
  );
}
