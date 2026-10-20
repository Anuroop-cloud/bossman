import { useState } from "react";
import { session } from "../lib/api";
import { useSession } from "../lib/session";
import { KeyRound, LogOut } from "lucide-react";

/** Sidebar control: paste an operator/admin/viewer token (kept in memory only). */
export function TokenLogin() {
  const { info } = useSession();
  const [value, setValue] = useState("");
  if (session.token) {
    return (
      <div style={{ display: "grid", gap: 6 }}>
        <div className="dim" style={{ fontSize: 12 }}>
          Signed in as <strong>{info?.role ?? "…"}</strong>{info?.name ? ` (${info.name})` : ""}
        </div>
        <button className="btn sm" onClick={() => session.set(null)}><LogOut size={12} /> Sign out</button>
      </div>
    );
  }
  return (
    <form
      onSubmit={(e) => { e.preventDefault(); session.set(value); setValue(""); }}
      style={{ display: "grid", gap: 6 }}
    >
      <label className="dim" style={{ fontSize: 12 }} htmlFor="tok">
        <KeyRound size={12} /> Token (operator / admin)
      </label>
      <input id="tok" type="password" value={value} onChange={(e) => setValue(e.target.value)}
             placeholder="paste token" autoComplete="off" />
      <button className="btn sm" type="submit" disabled={!value.trim()}>Sign in</button>
    </form>
  );
}
