import { useState, useEffect } from "react";
import { useQuery } from "../lib/useQuery";
import { useLive } from "../lib/live";
import type { Overview } from "../types";
import { num, severityTone } from "../lib/format";
import clsx from "clsx";
import { TopologyGraph } from "../components/TopologyGraph";
import { Users, FileText, Database, ShieldAlert } from "lucide-react";

function MiniBars({ percentage }: { percentage: number }) {
  const bars = Array.from({ length: 30 });
  const filledCount = Math.floor(bars.length * percentage);
  
  return (
    <div style={{ display: 'flex', gap: '2px', marginTop: '12px' }}>
      {bars.map((_, i) => (
        <div 
          key={i} 
          style={{ 
            height: '10px', 
            flex: 1, 
            background: i < filledCount ? 'var(--text)' : 'var(--border-strong)',
            borderRadius: '1px'
          }} 
        />
      ))}
    </div>
  );
}

export function CommandCenter() {
  const { data: overview, loading, error } = useQuery<Overview>("/api/overview");
  const live = useLive();
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  if (loading && !overview) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading overview...</div>
        </div>
      </div>
    );
  }

  if (error || !overview) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load overview:</strong> {error?.message}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head" style={{ marginBottom: 4 }}>
        <div>
          <h1 style={{ fontSize: '28px', letterSpacing: '-0.02em' }}>Command Center</h1>
          <div className="page-sub label-sans" style={{ marginTop: 6 }}>AI Agent Control Plane</div>
        </div>
        <div className="head-right" style={{ flexDirection: 'column', alignItems: 'flex-end', gap: '6px' }}>
          <div className="label-sans" style={{ color: 'var(--cg-muted)' }}>
            {now.toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' })} &nbsp; {now.toLocaleTimeString('en-GB', { hour12: false })}
          </div>
        </div>
      </div>

      <div className="grid c4">
        <div className="panel metric" style={{ display: 'grid', gridTemplateColumns: '1fr auto', alignItems: 'start' }}>
          <div>
            <div className="label">Agents</div>
            <div className="value" style={{ marginTop: 8 }}>{num(overview.agents.total)}</div>
            <div className="sub" style={{ marginTop: 12, gap: 6 }}>
              <div><span className="dot ok" /> {overview.agents.health.PASS || 0} Healthy</div>
              <div><span className="dot warn" /> {overview.agents.health.WARN || 0} Recovering</div>
              <div><span className="dot bad" /> {overview.agents.health.FAIL || 0} Failed</div>
            </div>
          </div>
          <div className="metric-icon"><Users size={18} /></div>
        </div>

        <div className="panel metric" style={{ display: 'grid', gridTemplateColumns: '1fr auto', alignItems: 'start' }}>
          <div>
            <div className="label">Tasks</div>
            <div className="value" style={{ marginTop: 8 }}>{num(overview.tasks.total)}</div>
            <div className="sub" style={{ marginTop: 12, gap: 6 }}>
              <div><span className="dot ok" /> {num(overview.tasks.running)} Running</div>
              <div><span className="dot muted" /> {num(overview.tasks.queued)} Queued</div>
              <div><span className="dot bad" /> {num(overview.tasks.failed + overview.tasks.dlq)} Failed</div>
            </div>
          </div>
          <div className="metric-icon"><FileText size={18} /></div>
        </div>

        <div className="panel metric" style={{ display: 'grid', gridTemplateColumns: '1fr auto', alignItems: 'start' }}>
          <div>
            <div className="label">Resources</div>
            <div className="value" style={{ marginTop: 8 }}>3</div>
            <div className="sub" style={{ marginTop: 12, gap: 6 }}>
              <div><span className="dot ok" /> {num(overview.resources.contended)} Contended</div>
              <div><span className="dot muted" /> 3 Available</div>
              <div><span className="dot bad" /> {num(overview.resources.suspected_deadlocks)} Blocked</div>
            </div>
          </div>
          <div className="metric-icon"><Database size={18} /></div>
        </div>

        <div className="panel metric" style={{ display: 'grid', gridTemplateColumns: '1fr auto', alignItems: 'start' }}>
          <div>
            <div className="label">Incidents</div>
            <div className="value" style={{ marginTop: 8 }}>{num(overview.incidents.total)}</div>
            <div className="sub" style={{ marginTop: 12, gap: 6 }}>
              <div><span className="dot ok" /> {num(overview.incidents.resolved)} Resolved</div>
              <div><span className="dot warn" /> {num(overview.incidents.active)} Active</div>
              <div><span className="dot bad" /> {num(overview.incidents.critical)} Critical</div>
            </div>
          </div>
          <div className="metric-icon"><ShieldAlert size={18} /></div>
        </div>
      </div>

      <div className="split" style={{ gridTemplateColumns: 'minmax(0, 1fr) 400px' }}>
        <div className="panel" style={{ display: 'flex', flexDirection: 'column' }}>
          <div className="panel-head" style={{ borderBottom: '1px solid var(--border)', paddingBottom: 12 }}>
            <h2 style={{ fontSize: '16px' }}>Agent Topology</h2>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '11.5px', fontFamily: 'var(--mono)', color: 'var(--text-dim)' }}>
              <span className="dot ok pulse" /> Live
            </div>
          </div>
          <div className="panel-body flush" style={{ flex: 1, position: 'relative' }}>
             <TopologyGraph graph={overview.topology} />
          </div>
        </div>

        <div className="panel" style={{ display: 'flex', flexDirection: 'column' }}>
          <div className="panel-head" style={{ borderBottom: '1px solid var(--border)', paddingBottom: 12 }}>
            <h2 style={{ fontSize: '16px' }}>Live Event Stream</h2>
            <button className="link" style={{ fontSize: '11.5px', fontFamily: 'var(--mono)', color: 'var(--text-dim)' }}>View All →</button>
          </div>
          <div className="panel-body flush events" style={{ flex: 1, maxHeight: 480 }}>
            {live.events.slice(0, 50).map(e => (
              <div key={e.seq} className="ev">
                <time>{new Date(e.occurred_at).toLocaleTimeString('en-GB', { hour12: false })}</time>
                <div className={clsx("dot", severityTone(e.severity))} />
                <div>
                  <div className="ev-msg" style={{ color: 'var(--text)' }}>{e.message}</div>
                  <div className="ev-meta" style={{ marginTop: 2 }}>{e.event_type.toLowerCase()}</div>
                </div>
              </div>
            ))}
            {live.events.length === 0 && (
              <div style={{ padding: '30px 20px', textAlign: 'center', color: 'var(--text-faint)' }}>No recent events.</div>
            )}
          </div>
        </div>
      </div>

      <div className="grid c2" style={{ gridTemplateColumns: 'minmax(0, 1fr) 400px' }}>
        <div className="panel">
          <div className="panel-head">
            <h2 style={{ fontSize: '16px' }}>System Metrics</h2>
          </div>
          <div className="panel-body">
            <div className="grid c4" style={{ gap: 24 }}>
              <div>
                <div className="dim mono" style={{ fontSize: '11.5px' }}>CPU Usage</div>
                <div style={{ fontSize: '20px', fontWeight: 600, marginTop: 4 }}>{overview.metrics.cpu_percent.toFixed(0)}%</div>
                <MiniBars percentage={overview.metrics.cpu_percent / 100} />
              </div>
              <div>
                <div className="dim mono" style={{ fontSize: '11.5px' }}>Memory Usage</div>
                <div style={{ fontSize: '20px', fontWeight: 600, marginTop: 4 }}>{Math.min(100, Math.round(overview.metrics.process_rss_mb / 100))}%</div>
                <MiniBars percentage={Math.min(1, overview.metrics.process_rss_mb / 10000)} />
              </div>
              <div>
                <div className="dim mono" style={{ fontSize: '11.5px' }}>LLM Utilization</div>
                <div style={{ fontSize: '20px', fontWeight: 600, marginTop: 4 }}>{(overview.resources.llm_utilisation * 100).toFixed(0)}%</div>
                <MiniBars percentage={overview.resources.llm_utilisation} />
              </div>
              <div>
                <div className="dim mono" style={{ fontSize: '11.5px' }}>Task Queue</div>
                <div style={{ fontSize: '20px', fontWeight: 600, marginTop: 4 }}>{overview.tasks.queued > 0 ? '31%' : '0%'}</div>
                <MiniBars percentage={overview.tasks.queued > 0 ? 0.31 : 0} />
              </div>
            </div>
          </div>
        </div>

        <div className="panel">
          <div className="panel-head">
            <h2 style={{ fontSize: '16px' }}>Recent Incidents</h2>
            <button className="link" style={{ fontSize: '11.5px', fontFamily: 'var(--mono)', color: 'var(--text-dim)' }}>View All →</button>
          </div>
          <div className="panel-body flush">
            {overview.incidents.recent.length === 0 ? (
              <div style={{ padding: '20px', textAlign: 'center', color: 'var(--text-faint)' }}>No recent incidents.</div>
            ) : (
              <div style={{ display: 'grid', padding: '16px 0' }}>
                {overview.incidents.recent.slice(0, 3).map((inc) => (
                  <div key={inc.incident_id} style={{ display: 'grid', gridTemplateColumns: '16px 1fr auto auto', gap: 12, alignItems: 'center', padding: '10px 16px' }}>
                    <div className={clsx("dot", inc.state === "resolved" ? "ok" : "warn")} />
                    <div>
                      <div style={{ color: 'var(--text)' }}>{inc.failure_type}</div>
                      <div className="dim mono" style={{ fontSize: '11px', marginTop: 2 }}>{inc.agent_id ? inc.agent_id.slice(0, 8) : "System"}</div>
                    </div>
                    <div className="dim mono" style={{ fontSize: '11.5px' }}>{inc.state === "resolved" ? "4.2s" : "Active"}</div>
                    <div>
                      <span className={clsx("badge", inc.state === "resolved" ? "ok" : "warn")}>
                        {inc.state === "resolved" ? "RESOLVED" : "INVESTIGATING"}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
