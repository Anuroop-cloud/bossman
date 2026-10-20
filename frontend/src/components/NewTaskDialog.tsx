import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../lib/api";
import { useAction, useSession } from "../lib/session";

/** Submit work to the fleet (operator). Mirrors `bossman task submit`. */
export function NewTaskDialog({ onClose }: { onClose: () => void }) {
  const { info } = useSession();
  const act = useAction();
  const nav = useNavigate();
  const [f, setF] = useState({ title: "", role: "", input: "{}", priority: "NORMAL", attempts: 3, namespace: "default" });
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    let input: unknown;
    try { input = JSON.parse(f.input || "{}"); } catch { setErr("Input must be valid JSON."); return; }
    const t = await act("Task submitted", () => api.post<{ task_id: string }>("/api/tasks", {
      title: f.title, required_role: f.role || undefined, input, priority: f.priority,
      max_attempts: f.attempts, namespace: f.namespace || "default",
    }));
    if (t) { onClose(); nav(`/tasks/${t.task_id}`); }
  };

  return (
    <div className="overlay center" onClick={onClose}>
      <div className="dialog" onClick={(e) => e.stopPropagation()} role="dialog" aria-label="New task">
        <h2>New task</h2>
        {!info?.can_operate && <div className="banner warn">Sign in with an operator token to submit tasks.</div>}
        <input type="text" placeholder="What should the fleet do?" value={f.title} autoFocus
               onChange={(e) => setF({ ...f, title: e.target.value })} />
        <div className="toolbar">
          <input type="text" placeholder="role (any)" value={f.role} onChange={(e) => setF({ ...f, role: e.target.value })} style={{ flex: 1 }} />
          <input type="text" placeholder="namespace" value={f.namespace} onChange={(e) => setF({ ...f, namespace: e.target.value })} style={{ flex: 1 }} />
        </div>
        <div className="toolbar">
          <select value={f.priority} onChange={(e) => setF({ ...f, priority: e.target.value })}>
            {["CRITICAL", "HIGH", "NORMAL", "LOW", "BACKGROUND"].map((p) => <option key={p}>{p}</option>)}
          </select>
          <label className="dim">max attempts
            <input type="text" value={f.attempts} style={{ width: 56, marginLeft: 8 }}
                   onChange={(e) => setF({ ...f, attempts: Math.max(1, Number(e.target.value) || 1) })} />
          </label>
        </div>
        <textarea rows={5} value={f.input} onChange={(e) => setF({ ...f, input: e.target.value })}
                  style={{ font: "12.5px/1.5 var(--mono)" }} aria-label="input JSON" />
        {err && <div className="tone-bad">{err}</div>}
        <div className="dialog-actions">
          <button className="btn" onClick={onClose}>Cancel</button>
          <button className="btn accent" disabled={!f.title.trim() || !info?.can_operate} onClick={submit}>Submit</button>
        </div>
      </div>
    </div>
  );
}
