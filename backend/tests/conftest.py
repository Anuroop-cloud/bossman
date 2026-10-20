"""Shared test configuration."""

import os

# Never call a real LLM from the test suite, whatever the developer's shell has.
os.environ["BOSSMAN_LLM"] = "off"
# Tests drive their own clocks; keep the watchdog quick but deterministic.
os.environ.setdefault("HEARTBEAT_TIMEOUT_SECONDS", "30")
