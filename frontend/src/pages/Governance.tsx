import { useEffect, useState } from "react";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";
import type { ApiKeyRecord, FlagRule, SecretRecord } from "../types";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";

type Tab = "policy" | "flags" | "keys" | "secrets" | "audit";

export function Governance() {
  const [tab, setTab] = useState<Tab>("policy");
  const { info } = useSession();
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Governance</h1>
          <div className="page-sub">Infrastructure-enforced boundaries: policy, kill switches, credentials, keys and the audit trail.</div>
        </div>
      </div>
      <div className="tabs">
        {(["policy", "flags", "keys", "secrets", "audit"] as Tab[]).map((t) => (
          <button key={t} className={clsx("tab", tab === t && "on")} onClick={() => setTab(t)}>{t}</button>
        ))}
      </div>
      {tab === "policy" && <PolicyTab canEdit={Boolean(info?.is_admin)} />}
      {tab === "flags" && <FlagsTab canEdit={Boolean(info?.can_operate)} />}
      {tab === "keys" && (info?.is_admin ? <KeysTab /> : <AdminOnly />)}
      {tab === "secrets" && (info?.is_admin ? <SecretsTab /> : <AdminOnly />)}
      {tab === "audit" && (info?.can_operate ? <AuditTab /> : <div className="banner warn">Operator token required.</div>)}
    </div>
  );
}

const AdminOnly = () => <div className="banner warn">Admin token required (sign in from the sidebar).</div>;

function PolicyTab({ canEdit }: { canEdit: boolean }) {
  const { data, refetch } = useQuery<{ revision: number; yaml: string; stats: Record<string, number> }>("/api/policies");
  const [text, setText] = useState("");
  const act = useAction();
  useEffect(() => { if (data) setText(data.yaml); }, [data?.revision]); // eslint-disable-line react-hooks/exhaustive-deps
  return (
    <div className="panel">
      <div className="panel-head">
        <h2>Action policy · revision {data?.revision ?? "…"}</h2>
        <div className="dim mono">
          allow {data?.stats.allow ?? 0} · deny {data?.stats.deny ?? 0} · approval {data?.stats.require_approval ?? 0}
        </div>
      </div>
      <div className="panel-body" style={{ display: "grid", gap: 10 }}>
        <textarea value={text} onChange={(e) => setText(e.target.value)} readOnly={!canEdit} spellCheck={false}
                  style={{ width: "100%", minHeight: 420, font: "12px/1.5 var(--mono)", background: "var(--bg)",
                           color: "var(--text)", border: "1px solid var(--border-strong)", borderRadius: 6, padding: 12 }} />
        {canEdit && (
          <div className="toolbar">
            <button className="btn primary" onClick={() =>
              act("Policy applied", () => api.put("/api/policies", { yaml: text })).then(refetch)}>Apply</button>
            <button className="btn" onClick={() => data && setText(data.yaml)}>Reset</button>
          </div>
        )}
      </div>
    </div>
  );
}

function FlagsTab({ canEdit }: { canEdit: boolean }) {
  const { data, refetch } = useQuery<{ version: number; rules: FlagRule[] }>("/api/flags");
  const act = useAction();
  const [key, setKey] = useState("");
  const [scope, setScope] = useState({ namespace: "", role: "", agent_id: "" });
  const set = (k: string, enabled: boolean, s: Partial<FlagRule> = {}) =>
    act(`Flag ${k} ${enabled ? "on" : "off"}`, () => api.put(`/api/flags/${k}`, {
      enabled, namespace: s.namespace || undefined, role: s.role || undefined, agent_id: s.agent_id || undefined,
    })).then(refetch);
  return (
    <div className="panel">
      <div className="panel-head"><h2>Kill switches</h2><div className="dim">tool.&lt;name&gt; · action.&lt;name&gt; · model.&lt;name&gt; — unknown keys default to ON</div></div>
      <div className="panel-body flush">
        <table className="dt">
          <thead><tr><th>Key</th><th>Scope</th><th>State</th><th>Rollout</th><th>By</th><th /></tr></thead>
          <tbody>
            {data?.rules.map((r) => (
              <tr key={r.rule_id}>
                <td className="mono">{r.key}</td>
                <td className="mono dim">{[r.namespace, r.role, r.agent_id].filter(Boolean).join(" / ") || "global"}</td>
                <td><span className={clsx("badge", r.enabled ? "ok" : "bad")}>{r.enabled ? "ON" : "OFF"}</span></td>
                <td className="num">{r.rollout_percent}%</td>
                <td className="dim">{r.updated_by}</td>
                <td>
                  {canEdit && (
                    <div className="toolbar">
                      <button className="btn sm" onClick={() => set(r.key, !r.enabled, r)}>{r.enabled ? "Turn off" : "Turn on"}</button>
                      <button className="btn sm danger" onClick={() =>
                        act("Rule removed", () => api.delete(`/api/flags/rules/${encodeURIComponent(r.rule_id)}`)).then(refetch)}>Remove</button>
                    </div>
                  )}
                </td>
              </tr>
            ))}
            {!data?.rules.length && <tr><td colSpan={6} className="dim">No flag rules — everything is enabled.</td></tr>}
          </tbody>
        </table>
      </div>
      {canEdit && (
        <div className="panel-body toolbar">
          <input type="text" placeholder="tool.web_search" value={key} onChange={(e) => setKey(e.target.value)} />
          <input type="text" placeholder="namespace (optional)" value={scope.namespace}
                 onChange={(e) => setScope({ ...scope, namespace: e.target.value })} />
          <input type="text" placeholder="role (optional)" value={scope.role}
                 onChange={(e) => setScope({ ...scope, role: e.target.value })} />
          <button className="btn danger" disabled={!key} onClick={() => set(key, false, scope)}>Disable</button>
          <button className="btn" disabled={!key} onClick={() => set(key, true, scope)}>Enable</button>
        </div>
      )}
    </div>
  );
}

function KeysTab() {
  const { data, refetch } = useQuery<{ keys: ApiKeyRecord[] }>("/api/keys");
  const act = useAction();
  const [form, setForm] = useState({ name: "", role: "agent", namespaces: "default", max_tier: "WRITE" });
  const [created, setCreated] = useState<string | null>(null);
  return (
    <div className="panel">
      <div className="panel-head"><h2>API keys</h2><div className="dim">only a hash is stored; a new key is shown once</div></div>
      {created && (
        <div className="panel-body">
          <div className="banner warn" style={{ display: "grid", gap: 6 }}>
            <strong>Copy this key now — it will not be shown again:</strong>
            <code className="mono" style={{ userSelect: "all", overflowWrap: "anywhere" }}>{created}</code>
            <button className="btn sm" onClick={() => setCreated(null)}>Done</button>
          </div>
        </div>
      )}
      <div className="panel-body flush">
        <table className="dt">
          <thead><tr><th>Name</th><th>Role</th><th>Namespaces</th><th>Max tier</th><th>Prefix</th><th>Status</th><th /></tr></thead>
          <tbody>
            {data?.keys.map((k) => (
              <tr key={k.key_id}>
                <td>{k.name}</td><td>{k.role}</td><td className="mono">{k.namespaces.join(", ")}</td>
                <td>{k.max_tier}</td><td className="mono dim">{k.prefix}…</td>
                <td><span className={clsx("badge", k.revoked ? "bad" : "ok")}>{k.revoked ? "revoked" : "active"}</span></td>
                <td>{!k.revoked && <button className="btn sm danger" onClick={() =>
                  act("Key revoked", () => api.post(`/api/keys/${k.key_id}/revoke`)).then(refetch)}>Revoke</button>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="panel-body toolbar">
        <input type="text" placeholder="name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
        <select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
          {["agent", "viewer", "operator", "admin"].map((r) => <option key={r}>{r}</option>)}
        </select>
        <input type="text" placeholder="namespaces (comma)" value={form.namespaces}
               onChange={(e) => setForm({ ...form, namespaces: e.target.value })} />
        <select value={form.max_tier} onChange={(e) => setForm({ ...form, max_tier: e.target.value })}>
          {["READ_ONLY", "WRITE", "PRIVILEGED", "DESTRUCTIVE"].map((t) => <option key={t}>{t}</option>)}
        </select>
        <button className="btn primary" disabled={!form.name} onClick={async () => {
          const res = await act("Key created", () => api.post<{ key: string }>("/api/keys", {
            name: form.name, role: form.role, max_tier: form.max_tier,
            namespaces: form.namespaces.split(",").map((s) => s.trim()).filter(Boolean),
          }));
          if (res) { setCreated(res.key); refetch(); }
        }}>Create key</button>
      </div>
    </div>
  );
}

function SecretsTab() {
  const { data, refetch } = useQuery<{ secrets: SecretRecord[] }>("/api/secrets");
  const act = useAction();
  const [f, setF] = useState({ name: "", value: "", hosts: "", tools: "", release: "proxy", min_tier: "WRITE" });
  return (
    <div className="panel">
      <div className="panel-head"><h2>Credentials</h2><div className="dim">encrypted at rest; released only after allow/approval; proxy mode never hands the value to the agent</div></div>
      <div className="panel-body flush">
        <table className="dt">
          <thead><tr><th>Name</th><th>Release</th><th>Hosts</th><th>Tools</th><th>Min tier</th><th /></tr></thead>
          <tbody>
            {data?.secrets.map((s) => (
              <tr key={s.name}>
                <td className="mono">{s.name}</td><td>{s.release}</td>
                <td className="mono">{s.allowed_hosts.join(", ") || "—"}</td>
                <td className="mono">{s.allowed_tools.join(", ") || "any"}</td><td>{s.min_tier}</td>
                <td><button className="btn sm danger" onClick={() =>
                  act("Secret deleted", () => api.delete(`/api/secrets/${s.name}`)).then(refetch)}>Delete</button></td>
              </tr>
            ))}
            {!data?.secrets.length && <tr><td colSpan={6} className="dim">No credentials stored.</td></tr>}
          </tbody>
        </table>
      </div>
      <div className="panel-body toolbar">
        <input type="text" placeholder="name" value={f.name} onChange={(e) => setF({ ...f, name: e.target.value })} />
        <input type="password" placeholder="value" value={f.value} autoComplete="off"
               onChange={(e) => setF({ ...f, value: e.target.value })} />
        <input type="text" placeholder="allowed hosts (comma)" value={f.hosts} onChange={(e) => setF({ ...f, hosts: e.target.value })} />
        <input type="text" placeholder="allowed tools (comma)" value={f.tools} onChange={(e) => setF({ ...f, tools: e.target.value })} />
        <select value={f.release} onChange={(e) => setF({ ...f, release: e.target.value })}>
          <option value="proxy">proxy only</option><option value="direct">direct</option>
        </select>
        <select value={f.min_tier} onChange={(e) => setF({ ...f, min_tier: e.target.value })}>
          {["READ_ONLY", "WRITE", "PRIVILEGED", "DESTRUCTIVE"].map((t) => <option key={t}>{t}</option>)}
        </select>
        <button className="btn primary" disabled={!f.name || !f.value} onClick={async () => {
          const split = (s: string) => s.split(",").map((x) => x.trim()).filter(Boolean);
          const ok = await act("Secret saved", () => api.put(`/api/secrets/${f.name}`, {
            value: f.value, allowed_hosts: split(f.hosts), allowed_tools: split(f.tools),
            release: f.release, min_tier: f.min_tier }));
          if (ok) { setF({ ...f, value: "" }); refetch(); }
        }}>Save</button>
      </div>
    </div>
  );
}

function AuditTab() {
  const { data } = useQuery<{ entries: { audit_id: string; kind: string; actor: string; agent_id: string | null;
    at: number; data: Record<string, unknown> }[] }>("/api/audit?limit=300");
  return (
    <div className="panel">
      <div className="panel-head"><h2>Audit trail</h2></div>
      <div className="panel-body flush">
        <table className="dt">
          <thead><tr><th>When</th><th>Kind</th><th>Actor</th><th>Details</th></tr></thead>
          <tbody>
            {data?.entries.map((e) => (
              <tr key={e.audit_id}>
                <td className="mono">{new Date(e.at * 1000).toLocaleString()}</td>
                <td className="mono">{e.kind}</td>
                <td>{e.actor}</td>
                <td className="mono cell-sub" style={{ maxWidth: 520, overflowWrap: "anywhere" }}>{JSON.stringify(e.data).slice(0, 300)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
