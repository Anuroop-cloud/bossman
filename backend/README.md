# bossman-server

The BOSSman control plane: the daemon (`bossmand`) that supervises a fleet of
AI agents — heartbeats and watchdog, failure detection and diagnosis,
automated recovery, leased locks and fleet-wide rate limits, policy and human
approvals, managed agent processes, and the dashboard API.

Agents connect to it with the [`bossman-sdk`](../sdk/python) package (Python)
or [`@bossman/sdk`](../sdk/typescript) (TypeScript).

```bash
pip install -e ".[dev]"
bossmand serve            # http://127.0.0.1:7070
bossmand migrate          # apply database migrations (persistent mode)
bossmand keys create --role agent --namespace default
```

See the repository README for the full documentation.
