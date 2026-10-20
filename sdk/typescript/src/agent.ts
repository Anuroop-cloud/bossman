import type { Bossman } from "./client.js";
import { VERSION } from "./client.js";
import * as errors from "./errors.js";
import { errorClassOf, fullJitter, isTransient, sleep, statusOf } from "./resilience.js";
import { Task } from "./task.js";

export interface AgentOptions {
  name?: string;
  role?: string;
  namespace?: string;
  tier?: "READ_ONLY" | "WRITE" | "PRIVILEGED" | "DESTRUCTIVE";
  capabilities?: string[];
  labels?: Record<string, string>;
  metadata?: Record<string, unknown>;
  singletonKey?: string;
  takeover?: boolean;
  supervisor?: string;
  startOrder?: number;
  managedId?: string;
  heartbeatIntervalS?: number;
  primaryModel?: string;
  fallbackModel?: string;
  tokensBudget?: number;
  claimLeaseS?: number;
}

export interface Directive {
  directive_id: string;
  type: string;
  params: Record<string, unknown>;
  reason: string;
}

export type TaskHandler = (task: Task) => Promise<unknown> | unknown;
type Hook = (d: Directive) => unknown;

export class Authorization {
  decision: string;
  grantId?: string;
  approvalId?: string;
  credentials: Record<string, { mode: "proxy" | "direct"; value?: string; hosts?: string[] }>;
  constructor(public data: any) {
    this.decision = data.decision ?? "allow";
    this.grantId = data.grant_id ?? undefined;
    this.approvalId = data.approval_id ?? undefined;
    this.credentials = data.credentials ?? {};
  }
  secret(name: string): string {
    const c = this.credentials[name];
    if (!c || c.mode !== "direct") throw new errors.Forbidden(`credential '${name}' is proxy-only`);
    return c.value!;
  }
}

/** One supervised agent process. */
export class Agent {
  readonly name: string;
  readonly role: string;
  namespace?: string;
  agentId?: string;
  epoch?: number;
  flags: Record<string, boolean> = {};
  model?: string;
  degradationLevel = 0;
  compactRequested = false;
  rollbackRequested = false;
  fenced = false;
  stopping = false;
  paused = false;

  private opts: AgentOptions;
  private heartbeatIntervalS: number;
  private flagsVersion = 0;
  private tokensUsed = 0;
  private context = 0;
  private apiCalls = 0;
  private step?: string;
  private status = "IDLE";
  private reportedDegradation = 0;
  private current?: Task;
  private seen = new Set<string>();
  private acks: Array<{ directive_id: string; ok: boolean; result?: unknown }> = [];
  private hooks = new Map<string, Hook[]>();
  private loops: Promise<void>[] = [];
  private resumeWaiters: Array<() => void> = [];
  private abortControllers = new Set<AbortController>();

  constructor(readonly client: Bossman, opts: AgentOptions = {}) {
    const env = typeof process !== "undefined" ? process.env : ({} as Record<string, string | undefined>);
    this.opts = { ...opts };
    this.name = opts.name ?? env.BOSSMAN_AGENT_NAME ?? `agent-${Math.random().toString(16).slice(2, 8)}`;
    this.role = opts.role ?? env.BOSSMAN_ROLE ?? "worker";
    this.namespace = opts.namespace ?? client.namespace;
    this.opts.managedId = opts.managedId ?? env.BOSSMAN_MANAGED_ID;
    this.opts.supervisor = opts.supervisor ?? env.BOSSMAN_SUPERVISOR;
    if (env.POD_NAME && env.POD_NAMESPACE) {
      this.opts.labels = { "k8s.pod": env.POD_NAME, "k8s.namespace": env.POD_NAMESPACE, ...(opts.labels ?? {}) };
    }
    this.heartbeatIntervalS = opts.heartbeatIntervalS ?? 10;
    this.model = opts.primaryModel;
  }

  // ── lifecycle ──────────────────────────────────────────────────────────────

  private async register(): Promise<void> {
    const o = this.opts;
    const body: Record<string, unknown> = {
      name: this.name,
      role: this.role,
      namespace: this.namespace,
      permission_tier: o.tier ?? "READ_ONLY",
      capabilities: o.capabilities ?? [],
      labels: o.labels ?? {},
      metadata: o.metadata ?? {},
      singleton_key: o.singletonKey,
      takeover: o.takeover ?? false,
      supervisor: o.supervisor,
      start_order: o.startOrder ?? 0,
      managed_id: o.managedId,
      sdk: `typescript/${VERSION}`,
      tokens_budget: o.tokensBudget,
    };
    for (const k of Object.keys(body)) if (body[k] === undefined) delete body[k];
    const data = await this.client.post("/v1/agents", body);
    this.agentId = data.agent_id;
    this.epoch = data.epoch;
    this.namespace = data.namespace;
    if (o.heartbeatIntervalS === undefined) this.heartbeatIntervalS = data.heartbeat_interval_s ?? 10;
    this.flags = data.flags ?? {};
    this.flagsVersion = data.flags_version ?? 0;
    this.fenced = false;
  }

  async start(): Promise<this> {
    await this.register();
    this.loops = [this.heartbeatLoop(), this.directiveLoop()];
    return this;
  }

  async stop(reason = "shutdown"): Promise<void> {
    this.stopping = true;
    for (const c of this.abortControllers) c.abort();
    this.resume();
    if (this.agentId && !this.fenced) {
      try {
        await this.client.delete(`/v1/agents/${this.agentId}`, { params: { reason, epoch: this.epoch }, retries: 1 });
      } catch {
        /* best effort */
      }
    }
  }

  // ── telemetry ──────────────────────────────────────────────────────────────

  report(r: { tokensUsed?: number; tokens?: number; context?: number; step?: string; apiCalls?: number;
    degradationLevel?: number; model?: string }): void {
    if (r.tokensUsed !== undefined) this.tokensUsed = r.tokensUsed;
    if (r.tokens) this.tokensUsed += r.tokens;
    if (r.context !== undefined) this.context = Math.max(0, Math.min(1, r.context));
    if (r.step !== undefined) this.step = r.step.slice(0, 200);
    this.apiCalls += r.apiCalls ?? 0;
    if (r.degradationLevel !== undefined) this.reportedDegradation = r.degradationLevel;
    if (r.model !== undefined) this.model = r.model;
  }

  async heartbeat(): Promise<any> {
    const acks = this.acks;
    this.acks = [];
    const body = {
      epoch: this.epoch,
      status: this.status,
      current_task_id: this.current?.taskId ?? null,
      current_step: this.step ?? null,
      tokens_used: this.tokensUsed,
      context_utilisation: this.context,
      api_calls_since_last_heartbeat: this.apiCalls,
      degradation_level: Math.max(this.degradationLevel, this.reportedDegradation),
      model: this.model ?? null,
      paused: this.paused,
      acked_directives: acks,
      flags_version: this.flagsVersion,
    };
    this.apiCalls = 0;
    let ack: any;
    try {
      ack = await this.client.post(`/v1/agents/${this.agentId}/heartbeat`, body, { retries: 1 });
    } catch (e) {
      this.acks = [...acks, ...this.acks];
      throw e;
    }
    if (ack.flags) {
      this.flags = ack.flags;
      this.flagsVersion = ack.flags_version ?? this.flagsVersion;
    }
    for (const d of ack.directives ?? []) await this.handleDirective(d);
    return ack;
  }

  private async heartbeatLoop(): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      await sleep(1000 * (failures === 0 ? this.heartbeatIntervalS : Math.min(this.heartbeatIntervalS, fullJitter(failures, 0.5, 10))));
      if (this.stopping) return;
      try {
        await this.heartbeat();
        failures = 0;
      } catch (e) {
        if (e instanceof errors.Fenced) return this.onFenced();
        if (e instanceof errors.NotFound) {
          this.opts.takeover = true;
          try {
            await this.register();
          } catch {
            /* retry next beat */
          }
          continue;
        }
        failures++;
      }
    }
  }

  private async directiveLoop(): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      try {
        const res = await this.client.get(`/v1/agents/${this.agentId}/directives`, {
          params: { wait: 25, epoch: this.epoch },
          timeoutMs: 40_000,
          retries: 0,
        });
        for (const d of res.directives ?? []) await this.handleDirective(d);
        failures = 0;
      } catch (e) {
        if (e instanceof errors.Fenced) return this.onFenced();
        failures++;
        await sleep(1000 * fullJitter(failures, 0.5, 15));
      }
    }
  }

  private onFenced(): void {
    this.fenced = true;
    this.stopping = true;
    for (const c of this.abortControllers) c.abort();
  }

  // ── directives ─────────────────────────────────────────────────────────────

  on(type: string, hook: Hook): this {
    const list = this.hooks.get(type.toUpperCase()) ?? [];
    list.push(hook);
    this.hooks.set(type.toUpperCase(), list);
    return this;
  }

  private resume(): void {
    this.paused = false;
    for (const w of this.resumeWaiters.splice(0)) w();
  }

  /** Blocks while BOSSman has the agent paused — call at safe points. */
  async waitIfPaused(): Promise<void> {
    if (!this.paused) return;
    this.step = "paused";
    await new Promise<void>((r) => this.resumeWaiters.push(r));
  }

  private async handleDirective(d: Directive): Promise<void> {
    if (this.seen.has(d.directive_id)) return;
    this.seen.add(d.directive_id);
    let ok = true;
    let result: Record<string, unknown> = { applied: d.type };
    try {
      switch (d.type) {
        case "PAUSE": this.paused = true; break;
        case "RESUME": this.resume(); break;
        case "ABORT_TASK": for (const c of this.abortControllers) c.abort(); break;
        case "COMPACT_CONTEXT": this.compactRequested = true; break;
        case "ROLLBACK_CHECKPOINT": this.rollbackRequested = true; break;
        case "FALLBACK_MODEL":
          this.degradationLevel = Math.max(1, this.degradationLevel);
          if (this.opts.fallbackModel) this.model = this.opts.fallbackModel;
          result.model = this.model;
          break;
        case "RESTORE_MODEL": this.degradationLevel = 0; this.model = this.opts.primaryModel; break;
        case "SET_DEGRADATION": this.degradationLevel = Number(d.params.level ?? 0); break;
        case "RESTART":
          for (const c of this.abortControllers) c.abort();
          this.context = 0; this.step = undefined; this.resume();
          break;
        case "SHUTDOWN": this.stopping = true; for (const c of this.abortControllers) c.abort(); break;
        case "REFRESH_FLAGS": this.flagsVersion = -1; break;
      }
      for (const hook of this.hooks.get(d.type) ?? []) {
        const out = await hook(d);
        if (out && typeof out === "object") result = { ...result, ...(out as object) };
      }
    } catch (e) {
      ok = false;
      result = { error: String((e as Error).message ?? e).slice(0, 300) };
    }
    try {
      await this.client.post(`/v1/agents/${this.agentId}/directives/${d.directive_id}/ack`, { ok, result }, { retries: 2 });
    } catch {
      this.acks.push({ directive_id: d.directive_id, ok, result });
    }
  }

  flag(key: string, dflt = true): boolean {
    return this.flags[key] ?? dflt;
  }

  // ── tasks ──────────────────────────────────────────────────────────────────

  async claim(waitS = 20, leaseS = this.opts.claimLeaseS ?? 60): Promise<Task | null> {
    await this.waitIfPaused();
    const data = await this.client.post(
      `/v1/agents/${this.agentId}/tasks/claim`,
      { wait_s: waitS, lease_s: leaseS },
      { params: { epoch: this.epoch }, timeoutMs: (waitS + 15) * 1000, retries: 1 },
    );
    return data ? new Task(this, data, leaseS) : null;
  }

  /**
   * Claim and run tasks until stopped. The handler receives the Task and an
   * AbortSignal (fired on ABORT_TASK / RESTART / SHUTDOWN / upstream cancel).
   */
  async serve(handler: (task: Task, signal: AbortSignal) => Promise<unknown> | unknown,
              opts: { waitS?: number; stopWhenIdle?: boolean } = {}): Promise<void> {
    while (!this.stopping) {
      let task: Task | null;
      try {
        task = await this.claim(opts.waitS ?? 20);
      } catch (e) {
        if (e instanceof errors.Fenced) break;
        await sleep(1000 * fullJitter(2, 0.5, 5));
        continue;
      }
      if (!task) {
        if (opts.stopWhenIdle) break;
        continue;
      }
      await this.run(task, handler);
    }
  }

  private async run(task: Task, handler: (task: Task, signal: AbortSignal) => unknown): Promise<void> {
    const ctl = new AbortController();
    this.abortControllers.add(ctl);
    task.abortController = ctl;
    this.current = task;
    this.status = "RUNNING";
    this.step = "started";
    const keepAlive = task.keepAlive();
    try {
      const out = await Promise.race([
        Promise.resolve(handler(task, ctl.signal)),
        new Promise((_, rej) => ctl.signal.addEventListener("abort", () => rej(new Error("aborted")), { once: true })),
      ]);
      if (!task.settled) {
        const output = out && typeof out === "object" ? (out as Record<string, unknown>) : out === undefined ? {} : { result: out };
        await task.complete(output);
      }
    } catch (e) {
      if (!task.settled && !(e instanceof errors.OutputInvalid) && !(e instanceof errors.LeaseConflict)) {
        const aborted = ctl.signal.aborted;
        try {
          await task.fail(aborted ? "aborted by BOSSman directive or shutdown" : `${(e as Error).name}: ${(e as Error).message}`, {
            errorClass: aborted ? "aborted" : errorClassOf(e),
            statusCode: statusOf(e),
            retryable: aborted ? true : isTransient(e),
          });
        } catch {
          /* lease already gone */
        }
      }
    } finally {
      task.settled = true;
      await keepAlive.catch(() => undefined);
      this.abortControllers.delete(ctl);
      this.current = undefined;
      this.status = "IDLE";
      this.step = undefined;
    }
  }

  // ── resources ──────────────────────────────────────────────────────────────

  /** Exclusive leased lock; renewed while `fn` runs, always released. */
  async withLock<T>(resources: string[], fn: () => Promise<T>, o: { ttlS?: number; waitS?: number } = {}): Promise<T> {
    const ttlS = o.ttlS ?? 30;
    const waitS = o.waitS ?? 10;
    const lease = await this.client.post("/v1/locks/acquire",
      { agent_id: this.agentId, resources, ttl_s: ttlS, wait_s: waitS, task_id: this.current?.taskId },
      { timeoutMs: (waitS + 15) * 1000, retries: 0 });
    const timer = setInterval(() => {
      this.client.post(`/v1/locks/${lease.lease_id}/renew`, { ttl_s: ttlS }, { retries: 1 }).catch(() => undefined);
    }, Math.max(500, (ttlS * 1000) / 3));
    try {
      return await fn();
    } finally {
      clearInterval(timer);
      await this.client.post(`/v1/locks/${lease.lease_id}/release`, {}, { retries: 2 }).catch(() => undefined);
    }
  }

  /** Fleet-wide limit (default "llm"): concurrency + rate gate. */
  async withLimit<T>(fn: () => Promise<T>, o: { name?: string; cost?: number; waitS?: number } = {}): Promise<T> {
    const waitS = o.waitS ?? 30;
    const permit = await this.client.post(`/v1/limits/${o.name ?? "llm"}/acquire`,
      { agent_id: this.agentId, cost: o.cost ?? 1, wait_s: waitS },
      { timeoutMs: (waitS + 15) * 1000, retries: 0 });
    try {
      return await fn();
    } finally {
      await this.client.post(`/v1/limits/permits/${permit.permit_id}/release`, {}, { retries: 2 }).catch(() => undefined);
    }
  }

  /** Call a dependency through the shared circuit breaker. */
  async guarded<T>(dependency: string, fn: () => Promise<T>, fallback?: () => Promise<T> | T): Promise<T> {
    const allow = await this.client.post(`/v1/circuits/${dependency}/allow`);
    if (!allow.allowed) {
      if (fallback) return await fallback();
      throw new errors.CircuitOpen(dependency, allow.retry_after_s ?? 0);
    }
    const started = Date.now();
    try {
      const out = await fn();
      await this.reportOutcome(dependency, true, undefined, started);
      return out;
    } catch (e) {
      await this.reportOutcome(dependency, false, e, started);
      throw e;
    }
  }

  private async reportOutcome(dep: string, ok: boolean, e: unknown, started: number): Promise<void> {
    await this.client.post(`/v1/circuits/${dep}/report`, { ok, error: e ? String(e).slice(0, 300) : null, agent_id: this.agentId },
      { retries: 1 }).catch(() => undefined);
    await this.event("tool_call", dep, {
      ok, duration_ms: Date.now() - started,
      status_code: e ? statusOf(e) : undefined, error_class: e ? errorClassOf(e) : undefined,
      transient: e ? isTransient(e) : undefined, error: e ? String(e).slice(0, 500) : undefined,
    });
  }

  // ── governance ─────────────────────────────────────────────────────────────

  async authorize(action: string, o: { tool?: string; resource?: string; irreversible?: boolean; args?: unknown;
    justification?: string; labels?: Record<string, string>; credentials?: string[]; waitForApprovalS?: number } = {}): Promise<Authorization> {
    const decision = await this.client.post(`/v1/agents/${this.agentId}/authorize`, {
      action, tool: o.tool, resource: o.resource, irreversible: o.irreversible, args: o.args,
      justification: o.justification, labels: o.labels ?? {}, credentials: o.credentials ?? [],
      task_id: this.current?.taskId,
    }, { params: { epoch: this.epoch } });
    if (decision.decision === "allow") return new Authorization(decision);
    if (decision.decision === "deny") throw new errors.PolicyDenied(`${action} denied: ${decision.reason}`, decision);
    const deadline = Date.now() + 1000 * (o.waitForApprovalS ?? 600);
    for (;;) {
      const remaining = (deadline - Date.now()) / 1000;
      if (remaining <= 0) throw new errors.ApprovalTimeout(`approval ${decision.approval_id} not decided in time`);
      const a = await this.client.get(`/v1/approvals/${decision.approval_id}`, {
        params: { wait: Math.min(25, remaining) }, timeoutMs: 40_000 });
      if (a.status === "approved") return new Authorization({ ...decision, ...a, decision: "allow" });
      if (a.status === "denied" || a.status === "expired") {
        throw new errors.PolicyDenied(`${action} ${a.status} by humans`, { ...decision, status: a.status });
      }
    }
  }

  proxy(auth: Authorization, r: { credential: string; method?: string; url: string; headers?: Record<string, string>;
    body?: unknown; timeoutS?: number }): Promise<any> {
    if (!auth.grantId) throw new errors.Forbidden("authorization has no grant");
    return this.client.post(`/v1/agents/${this.agentId}/proxy`, {
      grant_id: auth.grantId, credential: r.credential, method: r.method ?? "GET", url: r.url,
      headers: r.headers ?? {}, body: r.body, timeout_s: r.timeoutS ?? 30,
    }, { timeoutMs: ((r.timeoutS ?? 30) + 15) * 1000, retries: 0 });
  }

  /** Run a side effect at most once per key across the fleet. */
  async once<T>(key: string, fn: () => Promise<T>, ttlS = 300): Promise<T> {
    for (;;) {
      const s = await this.client.post(`/v1/idempotency/${encodeURIComponent(key)}/begin`, { agent_id: this.agentId, ttl_s: ttlS });
      if (s.state === "completed") return s.result as T;
      if (s.state === "new") break;
      await sleep(1000 * Math.min(2, Number(s.retry_after_s ?? 1)));
    }
    try {
      const result = await fn();
      await this.client.post(`/v1/idempotency/${encodeURIComponent(key)}/complete`, { agent_id: this.agentId, result });
      return result;
    } catch (e) {
      await this.client.post(`/v1/idempotency/${encodeURIComponent(key)}/abort`, { agent_id: this.agentId }).catch(() => undefined);
      throw e;
    }
  }

  // ── quality / events ───────────────────────────────────────────────────────

  reportQuality(score: number, o: { taskId?: string; evaluator?: string; note?: string } = {}): Promise<any> {
    return this.client.post(`/v1/agents/${this.agentId}/quality`, {
      score, task_id: o.taskId, evaluator: o.evaluator ?? "agent", note: o.note ?? "" });
  }

  async event(type: "tool_call" | "llm_call" | "log" | "custom", name = "", fields: Record<string, unknown> = {}): Promise<void> {
    if (type === "tool_call" || type === "llm_call") this.apiCalls++;
    const ev: Record<string, unknown> = { type, name, task_id: this.current?.taskId, trace_id: this.current?.traceId, ...fields };
    for (const k of Object.keys(ev)) if (ev[k] === undefined) delete ev[k];
    await this.client.post(`/v1/agents/${this.agentId}/events`, { events: [ev] }, { retries: 1 }).catch(() => undefined);
  }

  log(message: string, data: Record<string, unknown> = {}): Promise<void> {
    return this.event("log", "", { message: message.slice(0, 1000), data });
  }

  get currentTask(): Task | undefined {
    return this.current;
  }
}
