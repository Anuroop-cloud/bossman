// API client. Credentials: the operator token is held in memory only — never
// localStorage/sessionStorage, never the URL, never bundled. The backend is the
// authorisation boundary; the UI only forwards the token and displays results.

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
  get unavailable() {
    return this.status === 0 || this.status >= 502;
  }
  get denied() {
    return this.status === 401 || this.status === 403;
  }
  get notFound() {
    return this.status === 404;
  }
}

let token: string | null = null;
const listeners = new Set<() => void>();

export const session = {
  get token() {
    return token;
  },
  set(t: string | null) {
    token = t && t.trim() ? t.trim() : null;
    listeners.forEach((l) => l());
  },
  subscribe(l: () => void) {
    listeners.add(l);
    return () => listeners.delete(l);
  },
};

async function request<T>(method: "GET" | "POST" | "PUT" | "DELETE", path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (token) headers["X-BOSSman-Token"] = token;
  let res: Response;
  try {
    res = await fetch(path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
      credentials: "omit",
      cache: "no-store",
    });
  } catch (e) {
    if ((e as Error).name === "AbortError") throw e;
    throw new ApiError(0, "unreachable", "Backend unreachable.");
  }
  if (!res.ok) {
    let code = "error";
    let message = `Request failed (${res.status}).`;
    try {
      const j = await res.json();
      code = j?.error?.code ?? code;
      message = j?.error?.message ?? message;
    } catch {
      /* non-JSON error body: the dev proxy could not reach the API */
      if (res.status >= 500) {
        throw new ApiError(0, "unreachable", "BOSSman API unreachable — is `bossmand serve` running on :7070?");
      }
    }
    throw new ApiError(res.status, code, message);
  }
  return (await res.json()) as T;
}

export const api = {
  get: <T,>(path: string, signal?: AbortSignal) => request<T>("GET", path, undefined, signal),
  post: <T,>(path: string, body: unknown = {}) => request<T>("POST", path, body),
  put: <T,>(path: string, body: unknown = {}) => request<T>("PUT", path, body),
  delete: <T,>(path: string) => request<T>("DELETE", path),
};
