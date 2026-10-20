import { fromResponse, Unavailable } from "./errors.js";
import { fullJitter, sleep } from "./resilience.js";
import { Agent, type AgentOptions } from "./agent.js";

export const VERSION = "0.2.0";

export interface ClientOptions {
  url?: string;
  apiKey?: string;
  namespace?: string;
  timeoutMs?: number;
  maxRetries?: number;
  fetch?: typeof fetch;
}

export interface RequestOptions {
  json?: unknown;
  params?: Record<string, string | number | boolean | undefined | null>;
  timeoutMs?: number;
  retries?: number;
}

export interface SubmitOptions {
  input?: Record<string, unknown>;
  description?: string;
  requiredRole?: string;
  priority?: "CRITICAL" | "HIGH" | "NORMAL" | "LOW" | "BACKGROUND";
  maxAttempts?: number;
  idempotencyKey?: string;
  dependsOn?: string[];
  outputSchema?: Record<string, unknown>;
  parentTaskId?: string;
  traceparent?: string;
  deadline?: string;
  labels?: Record<string, string>;
  namespace?: string;
}

/** The BOSSman client. Reads BOSSMAN_URL / BOSSMAN_API_KEY / BOSSMAN_NAMESPACE from the environment. */
export class Bossman {
  readonly url: string;
  readonly apiKey?: string;
  readonly namespace?: string;
  private timeoutMs: number;
  private maxRetries: number;
  private fetchImpl: typeof fetch;

  constructor(opts: ClientOptions = {}) {
    const env = typeof process !== "undefined" ? process.env : ({} as Record<string, string | undefined>);
    this.url = (opts.url ?? env.BOSSMAN_URL ?? "http://127.0.0.1:7070").replace(/\/+$/, "");
    this.apiKey = opts.apiKey ?? env.BOSSMAN_API_KEY ?? env.BOSSMAN_TOKEN;
    this.namespace = opts.namespace ?? env.BOSSMAN_NAMESPACE;
    this.timeoutMs = opts.timeoutMs ?? 30_000;
    this.maxRetries = opts.maxRetries ?? 3;
    this.fetchImpl = opts.fetch ?? fetch;
  }

  async request<T = any>(method: string, path: string, opts: RequestOptions = {}): Promise<T> {
    const attempts = opts.retries ?? this.maxRetries;
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(opts.params ?? {})) if (v !== undefined && v !== null) qs.set(k, String(v));
    const url = `${this.url}${path}${qs.toString() ? `?${qs}` : ""}`;
    const headers: Record<string, string> = {
      Accept: "application/json",
      "User-Agent": `bossman-sdk-typescript/${VERSION}`,
    };
    if (this.apiKey) headers.Authorization = `Bearer ${this.apiKey}`;
    if (opts.json !== undefined) headers["Content-Type"] = "application/json";
    for (let attempt = 0; ; attempt++) {
      let res: Response;
      try {
        res = await this.fetchImpl(url, {
          method,
          headers,
          body: opts.json === undefined ? undefined : JSON.stringify(opts.json),
          signal: AbortSignal.timeout(opts.timeoutMs ?? this.timeoutMs),
        });
      } catch (e) {
        if (attempt >= attempts) throw new Unavailable(`BOSSman unreachable at ${this.url}: ${(e as Error).message}`);
        await sleep(1000 * fullJitter(attempt, 0.5, 10));
        continue;
      }
      if ([502, 503, 504].includes(res.status) && attempt < attempts) {
        await sleep(1000 * fullJitter(attempt, 0.5, 10));
        continue;
      }
      if (res.status === 204) return null as T;
      let body: any;
      try {
        body = await res.json();
      } catch {
        body = { error: { code: "bad_response", message: `HTTP ${res.status}` } };
      }
      if (res.status >= 400) throw fromResponse(res.status, body);
      return body as T;
    }
  }

  get = <T = any>(path: string, o: RequestOptions = {}) => this.request<T>("GET", path, o);
  post = <T = any>(path: string, json: unknown = {}, o: RequestOptions = {}) =>
    this.request<T>("POST", path, { ...o, json });
  put = <T = any>(path: string, json: unknown = {}, o: RequestOptions = {}) =>
    this.request<T>("PUT", path, { ...o, json });
  delete = <T = any>(path: string, o: RequestOptions = {}) => this.request<T>("DELETE", path, o);

  /** Create (but do not start) a supervised agent session. Call `await agent.start()`. */
  agent(opts: AgentOptions): Agent {
    return new Agent(this, opts);
  }

  async submit(title: string, o: SubmitOptions = {}): Promise<any> {
    const body: Record<string, unknown> = {
      title,
      description: o.description ?? "",
      input: o.input ?? {},
      required_role: o.requiredRole,
      priority: o.priority ?? "NORMAL",
      max_attempts: o.maxAttempts ?? 3,
      idempotency_key: o.idempotencyKey,
      depends_on: o.dependsOn ?? [],
      output_schema: o.outputSchema,
      parent_task_id: o.parentTaskId,
      traceparent: o.traceparent,
      deadline: o.deadline,
      labels: o.labels ?? {},
      namespace: o.namespace ?? this.namespace,
    };
    for (const k of Object.keys(body)) if (body[k] === undefined) delete body[k];
    return this.post("/v1/tasks", body);
  }

  task(taskId: string): Promise<any> {
    return this.get(`/v1/tasks/${taskId}`);
  }

  /** Wait until a task is terminal. */
  async wait(taskId: string, timeoutS = 300): Promise<any> {
    const deadline = Date.now() + timeoutS * 1000;
    for (;;) {
      const remaining = (deadline - Date.now()) / 1000;
      const t = await this.get(`/v1/tasks/${taskId}`, {
        params: { wait: Math.max(0, Math.min(remaining, 25)) },
        timeoutMs: 40_000,
      });
      if (t.is_terminal || remaining <= 0) return t;
    }
  }
}
