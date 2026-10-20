# @bossman/sdk (TypeScript / Node ≥ 18)

Zero-dependency client for the BOSSman agent protocol.

```bash
npm install @bossman/sdk        # or: npm install ./sdk/typescript
export BOSSMAN_URL=http://localhost:7070
export BOSSMAN_API_KEY=bm_...
```

```ts
import { Bossman } from "@bossman/sdk";

const bm = new Bossman();
const agent = await bm.agent({ name: "writer-1", role: "writer", capabilities: ["cms"] }).start();

await agent.serve(async (task, signal) => {
  const draft = await agent.withLimit(() => llm(task.input.brief, { signal }));   // fleet-wide LLM gate
  agent.report({ tokens: 900, context: 0.2 });
  await task.checkpoint("drafted", { draft });
  const auth = await agent.authorize("create", { tool: "cms", credentials: ["cms"] });
  await agent.proxy(auth, { credential: "cms", method: "POST", url: "https://cms.example.com/posts", body: { draft } });
  return { published: true };
});
```

`signal` aborts on `ABORT_TASK`, `RESTART`, `SHUTDOWN` or an upstream cancel.
Locks: `agent.withLock(["kb"], fn)`; circuits: `agent.guarded("openai", fn, fallback)`;
exactly-once: `agent.once(key, fn)`; quality: `agent.reportQuality(0.9)`;
directive hooks: `agent.on("COMPACT_CONTEXT", fn)`.

Tests: `npm test` (unit tests always; end-to-end tests when `BOSSMAN_URL` and
`BOSSMAN_API_KEY` point at a running server).
