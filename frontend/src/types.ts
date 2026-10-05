// Shapes mirror the BOSSman API (api/views.py). Kept intentionally loose where
// the backend passes through structured telemetry.

export type Json = unknown;
export type Severity = "DEBUG" | "INFO" | "WARNING" | "ERROR" | "CRITICAL";
export type Tone = "ok" | "warn" | "bad" | "info" | "muted";

export interface Agent {
  agent_id: string;
  name: string;
  role: string;
  permission_tier: "READ_ONLY" | "WRITE" | "PRIVILEGED" | "DESTRUCTIVE";
  status: string;
  is_healthy: boolean;
  is_alive: boolean;
  current_task_id: string | null;
  current_step: string | null;
  heartbeat_age_s: number;
  uptime_s: number;
  started_at: string;
  last_activity: string;
  consecutive_failures: number;
  total_tasks_completed: number;
  total_tasks_failed: number;
  tokens_used: number;
  tokens_budget: number;
  token_fraction: number;
  context_utilisation: number;
  api_calls_total: number;
  api_calls_per_minute: number;
  resources_held: string[];
  resources_waiting: string[];
  is_deadlock_suspect: boolean;
  last_error: string | null;
  last_error_at: string | null;
}

export interface HealthLevel {
  status: "PASS" | "WARN" | "FAIL";
  detail: string;
  metrics: Record<string, Json>;
}
export interface HealthReport {
  agent_id: string;
  overall_status: "PASS" | "WARN" | "FAIL";
  checked_at: string;
  levels: Record<"L1_LIVENESS" | "L2_PROGRESS" | "L3_RESOURCES" | "L4_QUALITY", HealthLevel>;
}

export interface Task {
  task_id: string;
  title: string;
  description: string;
  priority: string;
  priority_value: number;
  status: string;
  assigned_to: string | null;
  required_role: string | null;
  attempt_count: number;
  max_attempts: number;
  last_error: string | null;
  created_at: string;
  assigned_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  deadline: string | null;
  dlq_reason: string | null;
  dlq_at: string | null;
  current_step: string | null;
  progress_pct: number | null;
  progress_basis: string;
  input_data?: Json;
  output_data?: Json;
}

export interface BossEvent {
  seq: number;
  event_id: string;
  event_type: string;
  severity: Severity;
  occurred_at: string;
  agent_id: string | null;
  task_id: string | null;
  resource_name: string | null;
  message: string;
  payload: Record<string, Json>;
}

export interface Stage {
  stage: "DETECTED" | "DIAGNOSED" | "RECOVERY" | "EVALUATION" | "RESOLVED";
  status: "done" | "active" | "pending" | "failed";
  at: string | null;
}

export interface Incident {
  incident_id: string;
  agent_id: string | null;
  failure_type: string;
  detail: string;
  state: "active" | "resolved" | "failed" | "escalated";
  severity: Severity;
  is_critical: boolean;
  stage: Stage["stage"];
  stages: Stage[];
  detected_at: string | null;
  updated_at: string | null;
  diagnosis_summary: string | null;
  confidence: number | null;
  recovery_strategy: string | null;
  attempt_count: number;
}

export interface Attempt {
  attempt_id: string;
  attempt_number: number;
  strategy: string;
  outcome: string;
  failure_type: string;
  failure_detail: string;
  recovery_started_at: string | null;
  recovery_completed_at: string | null;
  duration_s: number | null;
  verified_by_evaluator: boolean;
  evaluator_notes: string | null;
  recovery_notes: string | null;
  telemetry_snapshot?: Json;
}

export interface IncidentDetail extends Incident {
  diagnosis: {
    root_cause: string;
    confidence: number;
    rule_matched: string;
    needs_llm_review: boolean;
    recommended_strategy: string;
    evidence: Record<string, Json>;
  } | null;
  attempts: Attempt[];
  evaluation: {
    passed: boolean;
    at: string;
    notes: string | null;
    outcome: string | null;
    health: HealthReport | null;
  } | null;
  timeline: BossEvent[];
  resource_impact: { held: string[]; waiting: string[]; recent_resource_events: BossEvent[] };
  raw: { failure: Json; events: Json[] };
}

export interface GraphNode {
  id: string;
  kind: "supervisor" | "agent" | "task" | "resource";
  ref: string;
  label: string;
  status?: string;
  role?: string;
  locked?: boolean;
  waiters?: number;
  contention_count?: number;
  suspected_deadlock?: boolean;
}
export interface GraphEdge {
  id: string;
  kind: "supervises" | "works_on" | "holds" | "waits";
  source: string;
  target: string;
  sources: string[];
  suspected_deadlock: boolean;
}
export interface Graph {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface Overview {
  status: "HEALTHY" | "DEGRADED" | "CRITICAL";
  server_time: string;
  agents: { total: number; by_status: Record<string, number>; health: Record<"PASS" | "WARN" | "FAIL", number> };
  tasks: { total: number; running: number; queued: number; failed: number; dlq: number; completed: number };
  resources: { llm_utilisation: number; token_bucket_fill: number; contended: number; suspected_deadlocks: number };
  incidents: { total: number; active: number; critical: number; resolved: number; recent: Incident[] };
  metrics: {
    events_per_minute: number;
    events_published: number;
    cpu_percent: number;
    process_rss_mb: number;
    ws_clients: number;
    uptime_s: number;
  };
  topology: Graph;
  last_event_seq: number;
}

export interface NamedResource {
  name: string;
  holder: string | null;
  waiters: string[];
  currently_locked: boolean;
  acquire_count: number;
  contention_count: number;
  total_hold_seconds: number;
}
export interface ResourcesView {
  llm_semaphore: {
    name: string;
    max_concurrent: number;
    current_holders: number;
    peak_holders: number;
    total_acquired: number;
    total_contentions: number;
    contention_rate: number;
    utilisation: number;
    available: number;
  };
  token_bucket: {
    name: string;
    rate_per_second: number;
    capacity: number;
    tokens_available: number;
    fill_fraction: number;
    total_consumed: number;
    total_calls: number;
    total_wait_seconds: number;
    starvation_events: number;
    utilisation: number;
  };
  named_resources: NamedResource[];
  graph: Graph & {
    wait_for: { waiter: string; holder: string }[];
    cycles: string[][];
  };
  recent_deadlock_events: BossEvent[];
  capabilities: { semaphore_holder_ids: boolean; release_resource: boolean; database_pool: boolean };
}

export interface SystemView {
  version: string;
  environment: string;
  runtime: { status: string; uptime_s: number; python: string; components: Record<string, Record<string, Json>> };
  limits: Record<string, number>;
  llm: { provider: string; configured: boolean; model: string | null; diagnostician: string; note: string };
  database: { configured: boolean; connected: boolean | null; note: string };
  event_bus: Record<string, number>;
  websocket: { connected_clients: number; keepalive_interval_s: number };
  security: { operator_actions_enabled: boolean };
}

export interface SessionInfo {
  tier: string;
  actions_enabled: boolean;
  can_replay_task: boolean;
  can_discard_task: boolean;
  unsupported_actions: string[];
}
