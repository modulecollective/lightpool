import type {
  Change,
  Event,
  Header,
  Member,
  Process,
  Summary,
  Traffic,
} from "./trace.ts";
export type EventPreview = Event & {
  omittedChanges: number;
  omittedTraffic: number;
  categories: string[];
};
export type NodeView = { process: Process; summary?: Summary };
export type ReplayState = {
  time: string;
  position: number;
  observer: string;
  nodes: Map<string, NodeView>;
  members: Map<string, Member>;
  timers: Map<string, Change>;
  probes: Map<string, Change>;
  events: EventPreview[];
  trails: (Traffic & { time: string })[];
  sent: bigint;
  received: bigint;
  attempts: bigint;
  drops: bigint;
};
export function initialState(h: Header, observer: string): ReplayState {
  return {
    time: "0",
    position: 0,
    observer,
    nodes: new Map(h.initial.map((p) => [p.node, { process: { ...p } }])),
    members: new Map(),
    timers: new Map(),
    probes: new Map(),
    events: [],
    trails: [],
    sent: 0n,
    received: 0n,
    attempts: 0n,
    drops: 0n,
  };
}
export function apply(state: ReplayState, e: Event) {
  // Apply a complete event before publishing state to the main thread.
  for (const c of e.changes) {
    const node = state.nodes.get(c.node)!;
    if (c.kind === "process") {
      node.process = c.process!;
      if (c.phase === "start") {
        node.summary = undefined;
        if (c.node === state.observer) {
          state.members.clear();
          state.timers.clear();
          state.probes.clear();
        }
      }
    }
    if (c.kind === "summary") node.summary = c.summary;
    if (c.node !== state.observer) continue;
    switch (c.kind) {
      case "member.upsert":
        state.members.set(c.member!.name, c.member!);
        break;
      case "member.remove":
        state.members.delete(c.subject!);
        break;
      case "timer":
        if (c.phase === "arm") state.timers.set(c.id!, c);
        else state.timers.delete(c.id!);
        break;
      case "probe":
        if (c.phase === "finished") state.probes.delete(c.id!);
        else state.probes.set(c.id!, c);
        break;
    }
  }
  for (const t of e.traffic) {
    if (t.stage === "sent") state.sent += BigInt(t.bytes);
    if (t.stage === "received") state.received += BigInt(t.bytes);
    if (t.stage === "attempt") state.attempts++;
    if (t.stage === "drop") state.drops++;
    if (t.stage === "attempt" || t.stage === "drop" || t.stage === "handle")
      state.trails.push({ ...t, time: e.time });
  }
  state.trails = state.trails
    .filter((t) => BigInt(e.time) - BigInt(t.time) < 500000000n)
    .slice(-96);
  state.time = e.time;
  state.position++;
  state.events.push({
    ...e,
    changes: e.changes.slice(0, 24),
    traffic: e.traffic.slice(0, 24),
    omittedChanges: Math.max(0, e.changes.length - 24),
    omittedTraffic: Math.max(0, e.traffic.length - 24),
    categories: [...new Set(e.changes.map((c) => c.kind.split(".")[0]))],
  });
  if (state.events.length > 80) state.events.shift();
}
export function serializable(s: ReplayState) {
  return {
    ...s,
    nodes: [...s.nodes],
    members: [...s.members],
    timers: [...s.timers],
    probes: [...s.probes],
    sent: s.sent.toString(),
    received: s.received.toString(),
    attempts: s.attempts.toString(),
    drops: s.drops.toString(),
  };
}
export type View = ReturnType<typeof serializable>;
