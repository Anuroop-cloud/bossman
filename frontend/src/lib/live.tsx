import { createContext, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { api } from "./api";
import type { BossEvent } from "../types";

export type LiveStatus = "connecting" | "open" | "reconnecting" | "stale";

interface LiveCtx {
  status: LiveStatus;
  events: BossEvent[]; // newest first
  /** bumps when a state-changing event arrives (heartbeats excluded) */
  revision: number;
  /** bumps after every (re)connect so views re-sync via REST */
  resyncs: number;
  lastMessageAt: number | null;
  lastEventAt: number | null;
  retryInMs: number | null;
}

const Ctx = createContext<LiveCtx>({
  status: "connecting", events: [], revision: 0, resyncs: 0, lastMessageAt: null, lastEventAt: null, retryInMs: null,
});

const MAX_EVENTS = 300;
const FLUSH_MS = 400;

function wsUrl(): string {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws`;
}

export function mergeEvents(prev: BossEvent[], incoming: BossEvent[]): BossEvent[] {
  const seen = new Set(prev.map((e) => e.seq));
  const add = incoming.filter((e) => !seen.has(e.seq));
  if (!add.length) return prev;
  return [...add, ...prev].sort((a, b) => b.seq - a.seq).slice(0, MAX_EVENTS);
}

export function LiveProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<LiveCtx>({
    status: "connecting", events: [], revision: 0, resyncs: 0, lastMessageAt: null, lastEventAt: null, retryInMs: null,
  });
  const buffer = useRef<BossEvent[]>([]);
  const lastMsg = useRef<number | null>(null);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let closed = false;
    let attempt = 0;
    let retryTimer: number | undefined;
    let keepalive = 5;

    const backfill = () =>
      api
        .get<{ events: BossEvent[] }>("/api/events?limit=100")
        .then((r) => setState((s) => ({ ...s, events: mergeEvents(s.events, r.events) })))
        .catch(() => undefined);

    const connect = () => {
      if (closed) return;
      setState((s) => ({ ...s, status: attempt === 0 ? "connecting" : "reconnecting", retryInMs: null }));
      try {
        ws = new WebSocket(wsUrl());
      } catch {
        scheduleRetry();
        return;
      }
      ws.onopen = () => {
        attempt = 0;
      };
      ws.onmessage = (m) => {
        lastMsg.current = Date.now();
        let msg: { type: string; event?: BossEvent; keepalive_s?: number };
        try {
          msg = JSON.parse(m.data);
        } catch {
          return; // ignore malformed frames
        }
        if (msg.type === "hello") {
          keepalive = msg.keepalive_s ?? 5;
          setState((s) => ({ ...s, status: "open", resyncs: s.resyncs + 1, lastMessageAt: Date.now(), retryInMs: null }));
          void backfill();
        } else if (msg.type === "event" && msg.event) {
          buffer.current.push(msg.event);
        } else if (msg.type === "tick") {
          setState((s) => (s.status === "open" ? { ...s, lastMessageAt: Date.now() } : { ...s, status: "open", lastMessageAt: Date.now() }));
        }
      };
      ws.onclose = () => {
        ws = null;
        if (!closed) scheduleRetry();
      };
      ws.onerror = () => ws?.close();
    };

    const scheduleRetry = () => {
      attempt += 1;
      const delay = Math.min(15_000, 1000 * 2 ** Math.min(attempt, 4)) * (0.75 + Math.random() * 0.5);
      setState((s) => ({ ...s, status: "reconnecting", retryInMs: delay }));
      retryTimer = window.setTimeout(connect, delay);
    };

    // Batch high-rate event flow into a few renders per second.
    const flush = window.setInterval(() => {
      if (!buffer.current.length) return;
      const batch = buffer.current.splice(0);
      const meaningful = batch.some((e) => e.event_type !== "AGENT_HEARTBEAT");
      setState((s) => ({
        ...s,
        events: mergeEvents(s.events, batch),
        revision: meaningful ? s.revision + 1 : s.revision,
        lastEventAt: Date.now(),
      }));
    }, FLUSH_MS);

    // Server sends a tick every `keepalive` seconds; silence ⇒ half-open socket.
    const watchdog = window.setInterval(() => {
      if (ws && lastMsg.current && Date.now() - lastMsg.current > keepalive * 3000) {
        setState((s) => ({ ...s, status: "stale" }));
        ws.close();
      }
    }, 1000);

    connect();
    return () => {
      closed = true;
      window.clearTimeout(retryTimer);
      window.clearInterval(flush);
      window.clearInterval(watchdog);
      ws?.close();
    };
  }, []);

  const value = useMemo(() => state, [state]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export const useLive = () => useContext(Ctx);
