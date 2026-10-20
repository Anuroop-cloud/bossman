export { Bossman, VERSION } from "./client.js";
export type { ClientOptions, RequestOptions, SubmitOptions } from "./client.js";
export { Agent, Authorization } from "./agent.js";
export type { AgentOptions, Directive, TaskHandler } from "./agent.js";
export { Task } from "./task.js";
export * as errors from "./errors.js";
export { fullJitter, isTransient, retryWithBackoff, errorClassOf, statusOf } from "./resilience.js";
