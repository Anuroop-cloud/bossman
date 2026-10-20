// A Node agent: npm install ../../sdk/typescript && node agent.mjs
// Env: BOSSMAN_URL, BOSSMAN_API_KEY
import { Bossman } from "@bossman/sdk";

const bm = new Bossman();
const agent = await bm.agent({ name: `ts-writer-${process.pid}`, role: "writer" }).start();
agent.on("COMPACT_CONTEXT", () => console.log("BOSSman asked me to compact my context"));

process.on("SIGINT", async () => {
  await agent.stop("ctrl-c");
  process.exit(0);
});

console.log(`registered ${agent.agentId}; serving tasks for role "writer"`);
await agent.serve(async (task, signal) => {
  await task.progress("drafting");
  const text = await agent.withLimit(async () => {
    await new Promise((r, rej) => {
      const t = setTimeout(r, 500);
      signal.addEventListener("abort", () => { clearTimeout(t); rej(new Error("aborted")); });
    });
    return `draft for: ${task.input.topic ?? task.title}`;
  });
  agent.report({ tokens: 300, context: 0.1 });
  await task.checkpoint("drafted", { length: text.length });
  return { text };
});
