import { Fragment, useState } from "react";
import { Link } from "react-router-dom";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";
import type { Approval } from "../types";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";

const ago = (ts: number) => `${Math.max(0, Math.round((Date.now() / 1000 - ts) / 60))}m ago`;
const tone = (s: Approval["status"]) => (s === "approved" ? "ok" : s === "pending" ? "warn" : "bad");

export function Approvals() {
  const [scope, setScope] = useState<"pending" | "all">("pending");
  const { data, refetch, error } = useQuery<{ approvals: Approval[] }>(
    scope === "pending" ? "/api/approvals?status=pending" : "/api/approvals");
  const { info } = useSession();
  const act = useAction();
  const [reason, setReason] = useState<Record<string, string>>({});

  const decide = (a: Approval, approve: boolean) =>
    act(approve ? "Approved" : "Denied", () =>
      api.post(`/api/approvals/${a.approval_id}/${approve ? "approve" : "deny"}`, { reason: reason[a.approval_id] ?? "" }),
    ).then(refetch);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Approvals</h1>
          <div className="page-sub">Irreversible or production actions wait here for humans. Each approver counts once.</div>
        </div>
        <div className="head-right chips">
          <button className={clsx("chip", scope === "pending" && "on")} onClick={() => setScope("pending")}>Pending</button>
          <button className={clsx("chip", scope === "all" && "on")} onClick={() => setScope("all")}>All</button>
        </div>
      </div>
      {!info?.can_operate && (
        <div className="banner warn">Sign in with an operator or admin token (sidebar) to approve or deny.</div>
      )}
      {error && <div className="banner bad">{error.message}</div>}
      {data?.approvals.length === 0 && <div className="state"><h3>Nothing waiting for a human.</h3></div>}
      <div className="grid c2">
        {data?.approvals.map((a) => (
          <div key={a.approval_id} className="panel">
            <div className="panel-head">
              <h2>{a.agent_name} → {a.request.action}{a.request.tool ? ` via ${a.request.tool}` : ""}</h2>
              <span className={clsx("badge", tone(a.status))}>{a.status} · {a.approvals.length}/{a.required}</span>
            </div>
            <div className="panel-body" style={{ display: "grid", gap: 10 }}>
              <dl className="kv">
                <dt>Agent</dt><dd><Link className="link mono" to={`/agents/${a.agent_id}`}>{a.agent_id}</Link> ({a.namespace})</dd>
                <dt>Why gated</dt><dd>{a.decision.reason}</dd>
                <dt>Justification</dt><dd>{a.request.justification || <span className="dim">none given</span>}</dd>
                {a.request.resource && <><dt>Resource</dt><dd className="mono">{a.request.resource}</dd></>}
                {Object.keys(a.request.labels).length > 0 && <><dt>Labels</dt><dd className="mono">{JSON.stringify(a.request.labels)}</dd></>}
                {a.request.credentials.length > 0 && <><dt>Credentials</dt><dd className="mono">{a.request.credentials.join(", ")}</dd></>}
                <dt>Requested</dt><dd>{ago(a.created_at)}</dd>
                {a.approvals.map((v) => (
                  <Fragment key={`a-${v.by}`}><dt>approved</dt><dd>{v.by} {v.reason && `— ${v.reason}`}</dd></Fragment>
                ))}
                {a.denials.map((v) => (
                  <Fragment key={`d-${v.by}`}><dt>denied</dt><dd>{v.by} {v.reason && `— ${v.reason}`}</dd></Fragment>
                ))}
              </dl>
              {a.request.args && <pre className="json">{a.request.args}</pre>}
              {a.status === "pending" && (
                <div className="toolbar">
                  <input type="text" placeholder="reason (optional)" value={reason[a.approval_id] ?? ""}
                         onChange={(e) => setReason({ ...reason, [a.approval_id]: e.target.value })} style={{ flex: 1 }} />
                  <button className="btn primary" disabled={!info?.can_operate} onClick={() => decide(a, true)}>Approve</button>
                  <button className="btn danger" disabled={!info?.can_operate} onClick={() => decide(a, false)}>Deny</button>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
