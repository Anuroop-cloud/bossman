// End-to-end test against a running BOSSman (skipped unless BOSSMAN_URL is set).
//   BOSSMAN_URL=http://127.0.0.1:7070 BOSSMAN_API_KEY=<agent key> BOSSMAN_TOKEN_OPERATOR=<operator token> npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { Bossman, errors } from "../dist/index.js";

const live = Boolean(process.env.BOSSMAN_URL && process.env.BOSSMAN_API_KEY);

test("serve, checkpoint, child task, trace", { skip: !live }, async () => {
  const bm = new Bossman();
  const agent = await bm.agent({ name: `ts-e2e-${Date.now()}`, role: "ts-e2e", heartbeatIntervalS: 0.5 }).start();
  try {
    const root = await bm.submit("ts root", { requiredRole: "ts-e2e", input: { n: 2 } });
    const served = agent.serve(async (task) => {
      await task.progress("working");
      await task.checkpoint("half", { n: task.input.n });
      const child = await task.spawn("ts child", { requiredRole: "nobody" });
      return { doubled: task.input.n * 2, child: child.task_id };
    });
    const done = await bm.wait(root.task_id, 20);
    assert.equal(done.status, "COMPLETED");
    assert.equal(done.output_data.doubled, 4);
    const child = await bm.task(done.output_data.child);
    assert.equal(child.trace_id, root.trace_id);
    assert.equal(child.parent_task_id, root.task_id);
    agent.stopping = true;
    await Promise.race([served, new Promise((r) => setTimeout(r, 25_000))]);
  } finally {
    await agent.stop();
  }
});

test("locks, limits, idempotency, policy", { skip: !live }, async () => {
  const bm = new Bossman();
  const agent = await bm.agent({ name: `ts-gov-${Date.now()}`, role: "ts-gov", tier: "READ_ONLY" }).start();
  try {
    const held = await agent.withLock(["ts-resource"], async () => "inside");
    assert.equal(held, "inside");
    assert.equal(await agent.withLimit(async () => 7), 7);
    let effects = 0;
    const key = `ts-once-${Date.now()}`;
    const a = await agent.once(key, async () => ++effects);
    const b = await agent.once(key, async () => ++effects);
    assert.equal(a, 1);
    assert.equal(b, 1);
    await assert.rejects(agent.authorize("delete", { tool: "db", justification: "x" }), errors.PolicyDenied);
    const ok = await agent.authorize("read");
    assert.equal(ok.decision, "allow");
  } finally {
    await agent.stop();
  }
});

test("unit: error mapping and jitter", () => {
  assert.ok(errors.fromResponse(409, { error: { code: "fenced", message: "x" } }) instanceof errors.Fenced);
  assert.ok(errors.fromResponse(429, { error: { code: "rate_limited", message: "x" } }) instanceof errors.RateLimited);
  assert.ok(errors.fromResponse(503, {}) instanceof errors.Unavailable);
});
