// Session state (role / permissions) + a tiny toast + action helper.
// The token lives in memory only (lib/api.ts) — never in storage or URLs.
import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";
import { api, ApiError, session } from "./api";
import type { SessionInfo } from "../types";

interface Toast { id: number; tone: "ok" | "bad"; text: string }
interface Ctx {
  info: SessionInfo | null;
  refresh: () => void;
  toast: (tone: "ok" | "bad", text: string) => void;
}
const SessionCtx = createContext<Ctx>({ info: null, refresh: () => undefined, toast: () => undefined });

export function SessionProvider({ children }: { children: ReactNode }) {
  const [info, setInfo] = useState<SessionInfo | null>(null);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const refresh = useCallback(() => {
    api.get<SessionInfo>("/api/session").then(setInfo).catch(() => setInfo(null));
  }, []);
  useEffect(() => {
    refresh();
    const unsub = session.subscribe(refresh);
    return () => { unsub(); };
  }, [refresh]);
  const toast = useCallback((tone: "ok" | "bad", text: string) => {
    const id = Date.now() + Math.random();
    setToasts((t) => [...t, { id, tone, text }]);
    window.setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 5000);
  }, []);
  return (
    <SessionCtx.Provider value={{ info, refresh, toast }}>
      {children}
      <div className="toasts" aria-live="polite">
        {toasts.map((t) => <div key={t.id} className={`toast ${t.tone}`}>{t.text}</div>)}
      </div>
    </SessionCtx.Provider>
  );
}

export const useSession = () => useContext(SessionCtx);

/** Run a mutating call with toasts; returns the result (or null on failure). */
export function useAction() {
  const { toast } = useSession();
  return useCallback(async <T,>(label: string, fn: () => Promise<T>): Promise<T | null> => {
    try {
      const out = await fn();
      toast("ok", label);
      return out;
    } catch (e) {
      const msg = e instanceof ApiError ? e.message : "Unexpected error.";
      toast("bad", `${label} failed: ${msg}`);
      return null;
    }
  }, [toast]);
}
