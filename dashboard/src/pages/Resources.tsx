import { useQuery } from "../lib/useQuery";
import type { ResourcesView } from "../types";
import { num, pct, loadTone } from "../lib/format";
import clsx from "clsx";

export function Resources() {
  const { data, loading, error } = useQuery<ResourcesView>("/api/resources");

  if (loading && !data) {
    return (
      <div className="page">
        <div className="state">
          <div className="spinner" />
          <div>Loading resources...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="page">
        <div className="banner bad">
          <strong>Failed to load resources:</strong> {error.message}
        </div>
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Resources</h1>
          <div className="page-sub">Global resource mediation and deadlock detection</div>
        </div>
      </div>

      <div className="grid c2">
        <div className="panel">
          <div className="panel-head">
            <h2>LLM Semaphore</h2>
          </div>
          <div className="panel-body">
            <div className="metric" style={{ padding: 0 }}>
              <div className="value">{data.llm_semaphore.current_holders} / {data.llm_semaphore.max_concurrent}</div>
              <div className="sub">
                <div>Current Holders (Peak: {data.llm_semaphore.peak_holders})</div>
              </div>
            </div>
            <div className="bar-row" style={{ marginTop: 16 }}>
              <div className={clsx("bar", loadTone(data.llm_semaphore.utilisation))}>
                <i style={{ width: `${Math.min(100, data.llm_semaphore.utilisation * 100)}%` }} />
              </div>
              <div className="bar-meta">
                <span>Utilisation</span>
                <span>{pct(data.llm_semaphore.utilisation)}</span>
              </div>
            </div>
          </div>
        </div>

        <div className="panel">
          <div className="panel-head">
            <h2>Token Bucket</h2>
          </div>
          <div className="panel-body">
            <div className="metric" style={{ padding: 0 }}>
              <div className="value">{num(data.token_bucket.tokens_available)} / {num(data.token_bucket.capacity)}</div>
              <div className="sub">
                <div>Tokens Available</div>
              </div>
            </div>
            <div className="bar-row" style={{ marginTop: 16 }}>
              <div className={clsx("bar", loadTone(1 - data.token_bucket.fill_fraction))}>
                <i style={{ width: `${Math.min(100, data.token_bucket.fill_fraction * 100)}%` }} />
              </div>
              <div className="bar-meta">
                <span>Fill Level</span>
                <span>{pct(data.token_bucket.fill_fraction)}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">
          <h2>Named Locks</h2>
        </div>
        <div className="panel-body flush">
          <div className="table-wrap">
            <table className="dt">
              <thead>
                <tr>
                  <th>Resource Name</th>
                  <th>State</th>
                  <th>Holder</th>
                  <th>Waiters</th>
                  <th>Acquires</th>
                  <th>Contentions</th>
                </tr>
              </thead>
              <tbody>
                {data.named_resources.map(nr => (
                  <tr key={nr.name}>
                    <td className="mono">{nr.name}</td>
                    <td>
                      <span className={clsx("badge", nr.currently_locked ? "warn" : "ok")}>
                        {nr.currently_locked ? "LOCKED" : "FREE"}
                      </span>
                    </td>
                    <td className="mono dim">{nr.holder ? nr.holder.slice(0, 8) : "—"}</td>
                    <td className="mono dim">{nr.waiters.length}</td>
                    <td className="num">{num(nr.acquire_count)}</td>
                    <td className="num">{num(nr.contention_count)}</td>
                  </tr>
                ))}
                {data.named_resources.length === 0 && (
                  <tr>
                    <td colSpan={6} className="dim" style={{ textAlign: "center" }}>No named locks registered.</td>
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
