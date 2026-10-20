import type { Agent } from "./agent.js";
import { LeaseConflict } from "./errors.js";
import { sleep } from "./resilience.js";

/** A leased unit of work held by an Agent. */
export class Task {
  readonly taskId: string;
  readonly title: string;
  readonly description: string;
  readonly input: Record<string, any>;
  readonly leaseId?: string;
  readonly traceId?: string;
  readonly traceparent?: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  lastCheckpoint: any;
  settled = false;
  cancelRequested = false;
  abortController?: AbortController;

  constructor(private agent: Agent, readonly data: any, readonly leaseS = 60) {
    this.taskId = data.task_id;
    this.title = data.title;
    this.description = data.description ?? "";
    this.input = data.input_data ?? {};
    this.leaseId = data.lease_id ?? undefined;
    this.traceId = data.trace_id ?? undefined;
    this.traceparent = data.traceparent ?? undefined;
    this.attempt = data.attempt_count ?? 1;
    this.maxAttempts = data.max_attempts ?? 1;
    this.lastCheckpoint = data.checkpoint ?? null;
  }

  get resumed(): boolean {
    return this.lastCheckpoint !== null;
  }

  async progress(step: string): Promise<void> {
    this.agent.report({ step });
    await this.renew(step);
  }

  private async renew(step?: string): Promise<void> {
    const res = await this.agent.client.post(`/v1/tasks/${this.taskId}/heartbeat`,
      { lease_id: this.leaseId, lease_s: this.leaseS, step }, { retries: 1 });
    if (res.cancel_requested) this.cancelRequested = true;
  }

  /** @internal renews the lease until settled; aborts the handler if the task was cancelled upstream. */
  async keepAlive(): Promise<void> {
    while (!this.settled) {
      await sleep(Math.max(1000, (this.leaseS * 1000) / 3));
      if (this.settled) return;
      try {
        await this.renew();
      } catch (e) {
        if (e instanceof LeaseConflict) return;
        continue;
      }
      if (this.cancelRequested) {
        this.abortController?.abort();
        return;
      }
    }
  }

  async checkpoint(phase: string, state: Record<string, unknown> = {}): Promise<any> {
    this.lastCheckpoint = await this.agent.client.put(`/v1/tasks/${this.taskId}/checkpoint`,
      { lease_id: this.leaseId, phase, state });
    return this.lastCheckpoint;
  }

  async loadCheckpoint(beforeSeq?: number): Promise<any> {
    const r = await this.agent.client.get(`/v1/tasks/${this.taskId}/checkpoint`, { params: { before_seq: beforeSeq } });
    return r.checkpoint;
  }

  async complete(output: Record<string, unknown> = {}): Promise<any> {
    this.settled = true;
    return this.agent.client.post(`/v1/tasks/${this.taskId}/complete`, { lease_id: this.leaseId, output }, { retries: 2 });
  }

  async fail(error: string, o: { errorClass?: string; statusCode?: number; retryable?: boolean } = {}): Promise<any> {
    this.settled = true;
    return this.agent.client.post(`/v1/tasks/${this.taskId}/fail`, {
      lease_id: this.leaseId, error: error.slice(0, 4000), error_class: o.errorClass,
      status_code: o.statusCode, retryable: o.retryable,
    }, { retries: 2 });
  }

  /** Submit a child task in the same trace. */
  spawn(title: string, o: Record<string, any> = {}): Promise<any> {
    return this.agent.client.submit(title, { parentTaskId: this.taskId, traceparent: this.traceparent,
      namespace: this.agent.namespace, ...o });
  }
}
