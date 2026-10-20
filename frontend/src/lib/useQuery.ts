import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError } from "./api";
import { useLive } from "./live";

export interface QueryState<T> {
  data: T | null;
  error: ApiError | null;
  loading: boolean;
  updatedAt: number | null;
  /** data on screen is older than the live feed or the last refresh failed */
  stale: boolean;
  refetch: () => void;
}

const MIN_GAP_MS = 2500; // throttle live-triggered refetches
const SLOW_REFRESH_MS = 20_000; // covers heartbeat-only fields (tokens, context)
const OFFLINE_POLL_MS = 15_000; // only while the WebSocket is down

/**
 * Fetch `path`; keep it fresh via the WebSocket (no polling while connected,
 * except one slow refresh). Previous data is kept on error so the UI can show
 * "stale" instead of blanking.
 */
export function useQuery<T>(path: string | null): QueryState<T> {
  const live = useLive();
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(path !== null);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const last = useRef(0);
  const timer = useRef<number | undefined>(undefined);
  const ctl = useRef<AbortController | null>(null);

  const run = useCallback(() => {
    if (path === null) return;
    ctl.current?.abort();
    const c = new AbortController();
    ctl.current = c;
    last.current = Date.now();
    api
      .get<T>(path, c.signal)
      .then((d) => {
        setData(d);
        setError(null);
        setUpdatedAt(Date.now());
      })
      .catch((e) => {
        if ((e as Error).name === "AbortError") return;
        setError(e instanceof ApiError ? e : new ApiError(0, "unknown", "Unexpected error."));
      })
      .finally(() => {
        if (ctl.current === c) setLoading(false);
      });
  }, [path]);

  // Reset when the path changes.
  useEffect(() => {
    setData(null);
    setError(null);
    setUpdatedAt(null);
    setLoading(path !== null);
    run();
    return () => ctl.current?.abort();
  }, [path, run]);

  // Live-driven refresh (throttled, trailing edge).
  useEffect(() => {
    if (path === null || live.revision === 0) return;
    const wait = Math.max(0, MIN_GAP_MS - (Date.now() - last.current));
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(run, wait);
    return () => window.clearTimeout(timer.current);
  }, [live.revision, path, run]);

  // Re-sync after (re)connect.
  useEffect(() => {
    if (live.resyncs > 1) run();
  }, [live.resyncs, run]);

  // Slow refresh while live; slightly faster fallback poll while offline.
  useEffect(() => {
    if (path === null) return;
    const ms = live.status === "open" ? SLOW_REFRESH_MS : OFFLINE_POLL_MS;
    const id = window.setInterval(run, ms);
    return () => window.clearInterval(id);
  }, [live.status, path, run]);

  const stale = data !== null && (error !== null || live.status !== "open");
  return { data, error, loading, updatedAt, stale, refetch: run };
}
