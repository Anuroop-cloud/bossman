# Examples

All examples use `BOSSMAN_URL` and `BOSSMAN_API_KEY` (an agent key) from the
environment and run without any LLM credentials (a deterministic stub stands
in; set `ANTHROPIC_API_KEY` to use Claude).

| Example | Shows |
|---|---|
| [`quickstart/`](quickstart) | A supervised research agent: leased tasks, fleet-wide LLM limit, telemetry, checkpoints, directives, a known-answer probe. `submit.py` sends work and waits for results. |
| [`pipeline/`](pipeline) | Planner → writers → reviewer with `depends_on`, `output_schema`, one trace for the whole job, and quarantine when an upstream step produces bad output (`PIPELINE_CHAOS=1`). |
| [`governance/`](governance) | Ask-before-acting: policy denies, two-person approval for production deploys, credentials injected by the gateway (the agent never holds the token). |
| [`managed/`](managed) | BOSSman launches and restarts the process itself (`bossman managed create -f spec.yaml`); crash-loop protection with `CRASH_EVERY`. |
| [`langgraph_agent/`](langgraph_agent) | A LangGraph graph run under supervision (`bossman.integrations.langgraph.run_graph`). |
| [`chaos/`](chaos) | Game day: deadlock refusal, context overflow, silent degradation and a hung agent — and BOSSman's response to each. |
| [`typescript/`](typescript) | A Node agent with `@bossman/sdk` (abortable work via `AbortSignal`). |
| [`kubernetes/`](kubernetes) | Helm values running BOSSman HA plus supervised agent Deployments. |
