/** Client-side resilience: transient classification + full-jitter backoff. */

const TRANSIENT = new Set([408, 409, 425, 429, 500, 502, 503, 504, 529]);
const NON_TRANSIENT = new Set([400, 401, 403, 404, 405, 410, 413, 422]);

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export function fullJitter(attempt: number, baseS = 1, capS = 60): number {
  return Math.random() * Math.min(capS, baseS * 2 ** Math.max(0, attempt));
}

export function statusOf(err: unknown): number | undefined {
  const e = err as any;
  const s = e?.status ?? e?.statusCode ?? e?.response?.status;
  return typeof s === "number" ? s : undefined;
}

export function errorClassOf(err: unknown): string {
  const e = err as any;
  const name = String(e?.name ?? "error").toLowerCase();
  if (name.includes("abort") || name.includes("timeout")) return "timeout";
  if (name.includes("fetch") || e?.code === "ECONNREFUSED" || e?.code === "ECONNRESET") return "connection";
  if (name.includes("ratelimit")) return "rate_limit";
  if (name.includes("auth") || name.includes("permission")) return "credential";
  if (name.includes("typeerror") || name.includes("validation")) return "validation";
  return name.slice(0, 64);
}

export function isTransient(err: unknown): boolean {
  const s = statusOf(err);
  if (s !== undefined) {
    if (TRANSIENT.has(s) || s >= 500) return true;
    if (NON_TRANSIENT.has(s)) return false;
  }
  const t = (err as any)?.transient;
  if (typeof t === "boolean") return t;
  return ["timeout", "connection", "rate_limit"].includes(errorClassOf(err));
}

export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  opts: { maxRetries?: number; baseS?: number; capS?: number; retryIf?: (e: unknown) => boolean } = {},
): Promise<T> {
  const { maxRetries = 5, baseS = 1, capS = 60, retryIf = isTransient } = opts;
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (attempt >= maxRetries || !retryIf(e)) throw e;
      const ra = (e as any)?.retryAfterS;
      await sleep(1000 * (typeof ra === "number" ? ra : fullJitter(attempt, baseS, capS)));
    }
  }
}
