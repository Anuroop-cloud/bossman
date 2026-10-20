"""
Governance: an agent that must ask before acting.

Setup (admin):
    bossman secret set github --value "$GITHUB_TOKEN" --host api.github.com --tool github --min-tier WRITE
Run:
    python examples/governance/deployer.py
Then approve from the dashboard (Approvals) or:
    bossman approvals
    bossman approve <approval_id>          # production changes need TWO different operators

The agent never sees the GitHub token: BOSSman injects it into the proxied
request only after policy (and humans) allowed the action.
"""

import asyncio

from bossman import Bossman, errors


async def main():
    async with Bossman() as bm:
        async with bm.agent(name="release-bot", role="deployer", tier="PRIVILEGED",
                            capabilities=["github", "kubectl"]) as agent:
            # 1. read-only work is simply allowed
            await agent.authorize("read", tool="github")
            print("read: allowed")

            # 2. never-allowed actions are refused by policy, whatever the prompt said
            try:
                await agent.authorize("execute", tool="kubectl", args="kubectl exec db -- psql -c 'DROP DATABASE prod'")
            except errors.PolicyDenied as exc:
                print("drop database:", exc)

            # 3. production deploys need a justification and two human approvals
            print("deploy: waiting for two approvals (bossman approvals / dashboard)…")
            auth = await agent.authorize("deploy", tool="kubectl", labels={"env": "production"},
                                         justification="Release 1.4.2 — fixes checkout outage INC-221",
                                         wait_for_approval_s=900)
            print("deploy approved; grant", auth.grant_id)

            # 4. credentialed call through the gateway (token injected server-side)
            auth = await agent.authorize("create", tool="github", credentials=["github"])
            res = await agent.proxy(auth, credential="github", method="POST",
                                    url="https://api.github.com/repos/OWNER/REPO/issues",
                                    body={"title": "Release 1.4.2 deployed", "body": "via BOSSman"})
            print("github:", res["status"])


if __name__ == "__main__":
    asyncio.run(main())
