export type Process = {
  node: string;
  lifetime: string;
  running: boolean;
  pausedUntil: string;
};
export type Member = {
  name: string;
  address: string;
  incarnation: string;
  state: "alive" | "suspect" | "dead" | "left";
  meta?: string;
  application?: string;
};
export type Summary = {
  health: string;
  backlog: string;
  pendingStreams: string;
  stopped: boolean;
  left: boolean;
  stats: Record<string, string>;
};
export type Change = {
  kind: string;
  node: string;
  subject?: string;
  member?: Member;
  process?: Process;
  summary?: Summary;
  id?: string;
  generation?: string;
  operation?: string;
  phase?: string;
  purpose?: string;
  outcome?: string;
  deadline?: string;
  helpers?: string[];
};
export type Traffic = {
  id: string;
  transport: string;
  stage: string;
  from: string;
  to: string;
  bytes: string;
  messages: string[];
  detail?: string;
  received?: string;
};
export type Header = {
  type: "header";
  format: string;
  version: number;
  manifest: Record<string, unknown>;
  nodes: { name: string; address: string }[];
  initial: Process[];
};
export type Event = {
  type: "event";
  seq: string;
  time: string;
  id: string;
  kind: string;
  actor?: string;
  lifetime: string;
  changes: Change[];
  traffic: Traffic[];
};
export type Totals = {
  sentBytes: string;
  receivedBytes: string;
  datagramDrops: string;
  detections: string;
};
export type End = {
  type: "end";
  seq: string;
  time: string;
  reason: string;
  complete: boolean;
  nodes: (Process & Totals & { summary?: Summary })[];
  totals: Totals;
};
export type RecordLine = Header | Event | End;
const maxLine = 64 * 1024 * 1024;
const changes = new Set([
  "process",
  "member.upsert",
  "member.remove",
  "summary",
  "probe",
  "suspicion",
  "refutation",
  "timer",
]);
const messages = new Set([
  "ping",
  "indirect",
  "ack",
  "nack",
  "alive",
  "suspect",
  "dead",
  "exchange",
  "user",
]);
function requireThat(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}
function decimal(v: unknown, signed = false): asserts v is string {
  requireThat(
    typeof v === "string" && /^(0|[1-9][0-9]*)$/.test(v),
    "expected decimal string",
  );
  requireThat(
    BigInt(v) <= (signed ? 9223372036854775807n : 18446744073709551615n),
    "integer exceeds v1 range",
  );
}
function exact(value: unknown, root = false) {
  requireThat(
    value === null || typeof value !== "number" || root,
    "unsafe JSON number",
  );
  if (Array.isArray(value)) value.forEach((v) => exact(v));
  else if (value && typeof value === "object")
    for (const [k, v] of Object.entries(value))
      exact(v, root && k === "version");
}

// This validator is shared by incremental loading and random-access replay.
export class Validator {
  header?: Header;
  end?: End;
  seq = 0n;
  time = 0n;
  line = 0;
  names = new Set<string>();
  parse(text: string): RecordLine {
    this.line++;
    try {
      const r = JSON.parse(text) as RecordLine;
      requireThat(r && typeof r === "object", "expected object");
      if (!this.header) {
        requireThat(
          r.type === "header" &&
            r.format === "lightpool-trace" &&
            r.version === 1,
          "unsupported trace format/version",
        );
        exact(r, true);
        requireThat(
          r.manifest &&
            typeof r.manifest.Code === "string" &&
            r.manifest.Code.length,
          "missing source identity",
        );
        requireThat(
          Array.isArray(r.nodes) &&
            r.nodes.length > 0 &&
            r.nodes.length <= 16384,
          "invalid node list",
        );
        for (const n of r.nodes) {
          requireThat(
            typeof n.name === "string" &&
              n.name.length &&
              typeof n.address === "string" &&
              !this.names.has(n.name),
            "invalid/duplicate node",
          );
          this.names.add(n.name);
        }
        requireThat(
          Array.isArray(r.initial) && r.initial.length === this.names.size,
          "invalid initial state",
        );
        const initialNames = new Set<string>();
        for (const p of r.initial) {
          this.process(p);
          requireThat(
            !initialNames.has(p.node) &&
              !p.running &&
              p.lifetime === "0" &&
              p.pausedUntil === "0",
            "invalid initial process",
          );
          initialNames.add(p.node);
        }
        this.header = r;
      } else {
        exact(r);
        requireThat(!this.end, "record after end");
        requireThat(
          r.type === "event" || r.type === "end",
          "unexpected record type",
        );
        decimal(r.seq);
        decimal(r.time, true);
        requireThat(BigInt(r.time) >= this.time, "time moved backwards");
        if (r.type === "event") {
          requireThat(BigInt(r.seq) === this.seq + 1n, "sequence gap");
          decimal(r.id);
          decimal(r.lifetime);
          if (r.actor) this.node(r.actor);
          requireThat(
            typeof r.kind === "string" && r.kind.length,
            "missing event kind",
          );
          requireThat(
            Array.isArray(r.changes) && Array.isArray(r.traffic),
            "missing event arrays",
          );
          for (const c of r.changes) this.change(c);
          for (const t of r.traffic) this.traffic(t);
          this.seq++;
          this.time = BigInt(r.time);
        } else {
          requireThat(
            BigInt(r.seq) === this.seq && BigInt(r.time) === this.time,
            "end does not match last event",
          );
          requireThat(
            typeof r.complete === "boolean" &&
              typeof r.reason === "string" &&
              Array.isArray(r.nodes),
            "invalid end",
          );
          for (const p of r.nodes) {
            this.process(p);
            this.totals(p);
            if (p.summary) this.summary(p.summary);
          }
          this.totals(r.totals);
          this.end = r;
        }
      }
      return r;
    } catch (e) {
      throw new Error(`line ${this.line}: ${(e as Error).message}`);
    }
  }
  node(n: unknown) {
    requireThat(
      typeof n === "string" && this.names.has(n),
      `invalid node reference: ${String(n)}`,
    );
  }
  process(p: Process) {
    requireThat(p && typeof p.running === "boolean", "invalid process");
    this.node(p.node);
    decimal(p.lifetime);
    decimal(p.pausedUntil, true);
  }
  totals(t: Totals) {
    requireThat(t, "missing totals");
    for (const k of [
      "sentBytes",
      "receivedBytes",
      "datagramDrops",
      "detections",
    ] as const)
      decimal(t[k]);
  }
  summary(s: Summary) {
    requireThat(
      s && typeof s.stopped === "boolean" && typeof s.left === "boolean",
      "invalid summary",
    );
    decimal(s.health);
    decimal(s.backlog);
    decimal(s.pendingStreams);
    requireThat(s.stats && typeof s.stats === "object", "missing stats");
    Object.values(s.stats).forEach((v) => decimal(v));
  }
  change(c: Change) {
    requireThat(c && changes.has(c.kind), "unknown change");
    this.node(c.node);
    if (c.subject) this.node(c.subject);
    for (const n of c.helpers ?? []) this.node(n);
    if (c.kind === "process") {
      requireThat(
        c.process && ["start", "pause", "crash"].includes(c.phase ?? ""),
        "invalid process change",
      );
      this.process(c.process);
      requireThat(c.process.node === c.node, "process observer mismatch");
    }
    if (c.kind === "summary") {
      requireThat(c.summary, "missing summary");
      this.summary(c.summary);
    }
    if (c.kind === "member.upsert") {
      const m = c.member;
      requireThat(
        m &&
          typeof m.address === "string" &&
          ["alive", "suspect", "dead", "left"].includes(m.state),
        "invalid member",
      );
      this.node(m.name);
      decimal(m.incarnation);
    }
    if (c.kind === "member.remove")
      requireThat(c.subject, "missing removal subject");
    if (c.kind === "timer") {
      decimal(c.id);
      decimal(c.generation);
      decimal(c.operation);
      decimal(c.deadline, true);
      requireThat(
        ["arm", "cancel", "fire"].includes(c.phase ?? ""),
        "invalid timer phase",
      );
    }
    if (c.kind === "probe") {
      decimal(c.id);
      requireThat(
        ["direct", "indirect", "relay", "finished"].includes(c.phase ?? "") &&
          c.subject,
        "invalid probe",
      );
    }
  }
  traffic(t: Traffic) {
    requireThat(
      t &&
        ["datagram", "stream"].includes(t.transport) &&
        ["attempt", "sent", "arrival", "received", "handle", "drop"].includes(
          t.stage,
        ),
      "invalid traffic",
    );
    decimal(t.id);
    decimal(t.bytes);
    this.node(t.from);
    if (t.to) this.node(t.to);
    requireThat(
      Array.isArray(t.messages) && t.messages.every((m) => messages.has(m)),
      "invalid message kind",
    );
    if (t.received) decimal(t.received, true);
  }
}

// Byte framing retains only the unfinished line, so split UTF-8 and chunk
// boundaries have no effect on either semantics or original Blob offsets.
export class Framer {
  private pending = new Uint8Array();
  private offset = 0;
  private line = 0;
  private emit: (text: string, offset: number, length: number) => void;
  constructor(emit: (text: string, offset: number, length: number) => void) {
    this.emit = emit;
  }
  push(chunk: Uint8Array) {
    const bytes = new Uint8Array(this.pending.length + chunk.length);
    bytes.set(this.pending);
    bytes.set(chunk, this.pending.length);
    let start = 0;
    for (let i = 0; i < bytes.length; i++)
      if (bytes[i] === 10) {
        const length = i - start + 1;
        requireThat(
          length <= maxLine,
          `line ${this.line + 1}: record exceeds 64 MiB`,
        );
        let text: string;
        try {
          text = new TextDecoder("utf-8", { fatal: true }).decode(
            bytes.subarray(start, i),
          );
        } catch {
          throw new Error(`line ${this.line + 1}: invalid UTF-8`);
        }
        this.emit(text, this.offset, length);
        this.line++;
        this.offset += length;
        start = i + 1;
      }
    this.pending = bytes.slice(start);
    requireThat(
      this.pending.length <= maxLine,
      `line ${this.line + 1}: record exceeds 64 MiB`,
    );
  }
  // The pending unterminated line is intentionally not parsed.
}
export type Index = {
  blob: Blob;
  header: Header;
  end?: End;
  offsets: Float64Array;
  lengths: Uint32Array;
  times: BigInt64Array;
  byNode: Map<string, Uint32Array>;
};
export async function buildIndex(
  blob: Blob,
  progress: (bytes: number) => void = () => {},
  current: () => boolean = () => true,
): Promise<Index> {
  const validator = new Validator(),
    offsets: number[] = [],
    lengths: number[] = [],
    times: bigint[] = [],
    byNode = new Map<string, number[]>();
  let loaded = 0,
    last = 0;
  const framer = new Framer((text, offset, length) => {
    const r = validator.parse(text);
    if (r.type !== "event") return;
    const i = offsets.length;
    offsets.push(offset);
    lengths.push(length);
    times.push(BigInt(r.time));
    const relevant = new Set([
      ...(r.actor ? [r.actor] : []),
      ...r.changes.map((c) => c.node),
    ]);
    for (const t of r.traffic) {
      relevant.add(t.from);
      if (t.to) relevant.add(t.to);
    }
    for (const node of relevant) {
      let a = byNode.get(node);
      if (!a) {
        a = [];
        byNode.set(node, a);
      }
      a.push(i);
    }
  });
  const reader = blob.stream().getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!current()) throw new Error("load cancelled");
      framer.push(value);
      loaded += value.byteLength;
      if (loaded - last > 1 << 20) {
        progress(loaded);
        last = loaded;
        await new Promise((r) => setTimeout(r, 0));
      }
    }
  } finally {
    reader.releaseLock();
  }
  requireThat(validator.header, "line 1: missing complete header");
  progress(loaded);
  return {
    blob,
    header: validator.header,
    end: validator.end,
    offsets: Float64Array.from(offsets),
    lengths: Uint32Array.from(lengths),
    times: BigInt64Array.from(times),
    byNode: new Map([...byNode].map(([k, v]) => [k, Uint32Array.from(v)])),
  };
}
// Replay bounded blocks, never materialize an object per event for the file.
export async function* readEvents(
  index: Index,
  start: number,
  end: number,
): AsyncGenerator<Event> {
  let i = start;
  while (i < end) {
    let next = i + 1;
    while (
      next < end &&
      index.offsets[next] + index.lengths[next] - index.offsets[i] < 1 << 20
    )
      next++;
    const text = await index.blob
      .slice(
        index.offsets[i],
        index.offsets[next - 1] + index.lengths[next - 1],
      )
      .text();
    for (const line of text.trimEnd().split("\n"))
      yield JSON.parse(line) as Event;
    i = next;
  }
}
export function positionAt(index: Index, time: bigint): number {
  let lo = 0,
    hi = index.times.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (index.times[mid] <= time) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
