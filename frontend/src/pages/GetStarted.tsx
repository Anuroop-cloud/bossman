import { useState } from "react";
import clsx from "clsx";
import { useQuery } from "../lib/useQuery";

const origin = typeof window !== "undefined" ? window.location.origin : "http://localhost:7070";

const SNIPPETS: Record<string, { label: string; code: string }> = {
  python: {
    label: "Python",
    code: `pip install -e sdk/python      # bossman-sdk + bossman CLI
export BOSSMAN_URL=${origin}
export BOSSMAN_API_KEY=<agent key>

# agent.py
import asyncio
from bossman import Bossman

async def handle(task):
    async with agent.limit("llm"):            # fleet-wide LLM gate
        answer = await my_llm(task.input["question"])
    agent.report(tokens=1200, context=0.3)    # telemetry → watchdog
    await task.checkpoint("answered")
    return {"answer": answer}

async def main():
    global agent
    async with Bossman() as bm, bm.agent(name="researcher-1", role="researcher") as agent:
        await agent.serve(handle)

asyncio.run(main())`,
  },
  typescript: {
    label: "TypeScript",
    code: `npm install ./sdk/typescript   # @bossman/sdk

import { Bossman } from "@bossman/sdk";
const bm = new Bossman({ url: "${origin}", apiKey: process.env.BOSSMAN_API_KEY });
const agent = await bm.agent({ name: "writer-1", role: "writer" }).start();

await agent.serve(async (task, signal) => {
  const text = await agent.withLimit(() => llm(task.input.brief, { signal }));
  await task.checkpoint("drafted", { length: text.length });
  return { text };
});`,
  },
  langgraph: {
    label: "LangGraph",
    code: `from bossman.integrations.langgraph import run_graph

async def handle(task):
    # every node → reported step + checkpoint; pauses honoured between nodes
    state = await run_graph(agent, app, {"question": task.input["question"]},
                            config={"configurable": {"thread_id": task.task_id}})
    return {"answer": state["answer"]}`,
  },
  http: {
    label: "HTTP",
    code: `# any language — register, heartbeat, claim, complete
curl -s -X POST ${origin}/v1/agents -H "Authorization: Bearer $KEY" \\
  -H 'content-type: application/json' -d '{"name":"bot-1","role":"worker"}'
curl -s -X POST ${origin}/v1/agents/$AGENT_ID/tasks/claim -H "Authorization: Bearer $KEY" \\
  -H 'content-type: application/json' -d '{"wait_s":20}'
curl -s -X POST ${origin}/v1/tasks/$TASK_ID/complete -H "Authorization: Bearer $KEY" \\
  -H 'content-type: application/json' -d '{"lease_id":"'$LEASE'","output":{"ok":true}}'
# full spec: ${origin}/api/docs`,
  },
  managed: {
    label: "Managed",
    code: `# let BOSSman launch + restart the process for you
cat > spec.yaml <<'EOF'
name: batch-worker
command: ["python", "examples/managed/worker.py"]
role: batch
restart_policy: on-failure
max_restarts: 5
EOF
bossman managed create -f spec.yaml
bossman managed            # state · pid · restarts`,
  },
};

const DEPLOY = `docker compose up -d --wait                 # API + dashboard + Redis + Postgres
docker compose --profile ha up -d --wait    # 2 nodes, leader election
helm install bossman deploy/helm/bossman    # Kubernetes`;

function Code({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="copy-wrap">
      <pre className="code">{code}</pre>
      <button className="btn sm" onClick={() => {
        navigator.clipboard?.writeText(code).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500); });
      }}>{copied ? "Copied" : "Copy"}</button>
    </div>
  );
}

export function GetStarted() {
  const [tab, setTab] = useState("python");
  const { data } = useQuery<{ agents: { total: number } }>("/api/overview");
  const connected = (data?.agents.total ?? 0) > 0;
  return (
    <div className="page">
      <div className="hero">
        <h1>Run your agents under supervision</h1>
        <div className="dim" style={{ fontSize: 15, maxWidth: 760 }}>
          BOSSman is to agents what Docker is to processes: connect any agent, and it gets heartbeats, leased work,
          automatic recovery, fleet-wide limits, policy and human approvals — with this dashboard on top.
        </div>
        <div className="toolbar">
          <span className={clsx("badge", connected ? "ok" : "warn")}>
            {connected ? `${data?.agents.total} agent(s) connected` : "No agents connected yet"}
          </span>
          <span className="badge info">{origin}</span>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head"><h2>Connect an agent</h2></div>
        <div className="panel-body steps">
          <div className="step"><div><strong>Create an agent key</strong>
            <div className="dim">Governance → Keys (admin), or <code>bossman keys create --role agent --namespace default</code>.
              For local trials, use <code>BOSSMAN_AGENT_TOKEN</code> from <code>.env</code>.</div></div></div>
          <div className="step"><div style={{ display: "grid", gap: 10, minWidth: 0 }}><strong>Add the SDK and serve tasks</strong>
            <div className="chips">
              {Object.entries(SNIPPETS).map(([k, s]) => (
                <button key={k} className={clsx("chip", tab === k && "on")} onClick={() => setTab(k)}>{s.label}</button>
              ))}
            </div>
            <Code code={SNIPPETS[tab].code} /></div></div>
          <div className="step"><div><strong>Send work</strong>
            <div className="dim">Use <em>New task</em> in the sidebar, <code>bossman task submit "title" --role researcher</code>,
              or <code>bm.submit(...)</code> from code. Watch it under Tasks and Incidents.</div></div></div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head"><h2>Deploy</h2></div>
        <div className="panel-body"><Code code={DEPLOY} /></div>
      </div>
    </div>
  );
}
