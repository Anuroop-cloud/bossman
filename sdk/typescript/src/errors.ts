/** Typed errors mirroring the server envelope `{"error": {"code", "message"}}`. */

export class BossmanError extends Error {
  constructor(
    message: string,
    public status = 0,
    public code = "error",
    public body: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = new.target.name;
  }
  get transient(): boolean {
    return this.status === 0 || this.status === 429 || this.status >= 500;
  }
}

export class Unavailable extends BossmanError {}
export class Unauthorized extends BossmanError {}
export class Forbidden extends BossmanError {}
export class NotFound extends BossmanError {}
export class Conflict extends BossmanError {}
export class Fenced extends Conflict {}
export class LeaseConflict extends Conflict {}
export class Deadlock extends Conflict {}
export class DuplicateAgent extends Conflict {}
export class LockTimeout extends BossmanError {}
export class RateLimited extends BossmanError {
  get retryAfterS(): number {
    return Number(this.body.retry_after_s ?? 1);
  }
}
export class OutputInvalid extends BossmanError {}
export class PolicyDenied extends BossmanError {
  constructor(message: string, public decision: Record<string, unknown>) {
    super(message, 403, "policy_denied", decision);
  }
}
export class ApprovalTimeout extends BossmanError {}
export class CircuitOpen extends BossmanError {
  constructor(public key: string, public retryAfterS = 0) {
    super(`circuit '${key}' is open`, 503, "circuit_open");
  }
}

const BY_CODE: Record<string, typeof BossmanError> = {
  fenced: Fenced,
  lease_conflict: LeaseConflict,
  lease_lost: LeaseConflict,
  deadlock: Deadlock,
  lock_timeout: LockTimeout,
  rate_limited: RateLimited,
  spawn_limit: RateLimited,
  duplicate_agent: DuplicateAgent,
  output_invalid: OutputInvalid,
};
const BY_STATUS: Record<number, typeof BossmanError> = {
  401: Unauthorized,
  403: Forbidden,
  404: NotFound,
  409: Conflict,
  423: LockTimeout,
  429: RateLimited,
};

export function fromResponse(status: number, body: any): BossmanError {
  const err = (body && body.error) || {};
  const code: string = err.code ?? "error";
  const message: string = err.message ?? `HTTP ${status}`;
  const Cls = BY_CODE[code] ?? BY_STATUS[status] ?? (status >= 500 ? Unavailable : BossmanError);
  return new Cls(message, status, code, err);
}
