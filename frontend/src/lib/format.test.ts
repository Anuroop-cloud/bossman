import { describe, expect, it } from "vitest";
import { fmtDuration, healthTone, pct, shortId, taskTone } from "./format";
import { mergeEvents } from "./live";
import type { BossEvent } from "../types";

const ev = (seq: number): BossEvent => ({
  seq, event_id: `e${seq}`, event_type: "TASK_CREATED", severity: "INFO", occurred_at: new Date().toISOString(),
  agent_id: null, task_id: null, resource_name: null, message: "", payload: {},
});

describe("format", () => {
  it("formats durations", () => {
    expect(fmtDuration(59)).toBe("59s");
    expect(fmtDuration(3600 + 120)).toBe("1h 2m");
    expect(fmtDuration(null)).toBe("—");
  });
  it("formats percentages and ids", () => {
    expect(pct(0.256)).toBe("26%");
    expect(shortId("agent-1234567890abc")).toBe("agent-12…");
  });
  it("maps tones", () => {
    expect(taskTone("DEAD_LETTER")).toBe("bad");
    expect(healthTone("WARN")).toBe("warn");
  });
});

describe("mergeEvents", () => {
  it("dedupes by seq and keeps newest first", () => {
    const merged = mergeEvents([ev(2), ev(1)], [ev(3), ev(2)]);
    expect(merged.map((e) => e.seq)).toEqual([3, 2, 1]);
  });
});
