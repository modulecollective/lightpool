/// <reference lib="webworker" />
import { buildIndex, readEvents, positionAt } from "./trace.ts";
import type { Index } from "./trace.ts";
import { apply, initialState, serializable } from "./replay.ts";
import type { ReplayState } from "./replay.ts";
let index: Index | undefined,
  state: ReplayState | undefined,
  generation = 0;
const send = (v: unknown) => self.postMessage(v);
type Task =
  | { type: "load"; blob: Blob }
  | {
      type: "seek";
      observer: string;
      position?: number;
      time?: string;
      request: number;
    };
let pending: Task | undefined,
  running = false;
// Exactly one parse/replay task and one replaceable request can be outstanding.
self.onmessage = ({
  data,
}: MessageEvent<Task | { type: "record"; position: number }>) => {
  if (data.type === "record") {
    if (!index) return;
    const i = data.position - 1;
    if (i < 0 || i >= index.times.length) return;
    send({
      type: "record",
      position: data.position,
      blob: index.blob.slice(
        index.offsets[i],
        index.offsets[i] + index.lengths[i],
        "application/x-ndjson",
      ),
    });
    return;
  }
  pending = data;
  generation++;
  if (!running) void drain();
};
async function drain() {
  running = true;
  while (pending) {
    const data = pending;
    pending = undefined;
    await perform(data, generation);
  }
  running = false;
}
async function perform(data: Task, token: number) {
  try {
    if (data.type === "load") {
      index = undefined;
      state = undefined;
      const next = await buildIndex(
        data.blob,
        (bytes) => {
          if (token === generation)
            send({
              type: "progress",
              phase: "Indexing",
              bytes,
              total: data.blob.size,
            });
        },
        () => token === generation,
      );
      if (token !== generation) return;
      index = next;
      send({
        type: "loaded",
        header: index.header,
        end: index.end,
        count: index.times.length,
        duration: index.times.at(-1)?.toString() ?? "0",
        indexBytes:
          index.offsets.byteLength +
          index.lengths.byteLength +
          index.times.byteLength +
          [...index.byNode.values()].reduce((n, a) => n + a.byteLength, 0),
      });
      return;
    }
    if (!index) return;
    const observer = data.observer ?? index.header.nodes[0].name;
    const target =
      data.position === undefined
        ? positionAt(index, BigInt(data.time ?? "0"))
        : Math.max(0, Math.min(index.times.length, data.position));
    let replay = state;
    if (!replay || replay.observer !== observer || target < replay.position)
      replay = initialState(index.header, observer);
    let count = 0,
      last = performance.now();
    for await (const event of readEvents(index, replay.position, target)) {
      if (token !== generation) {
        state = undefined;
        return;
      }
      apply(replay, event);
      count++;
      if (count % 128 === 0) {
        if (performance.now() - last > 100) {
          send({
            type: "progress",
            phase: "Seeking",
            bytes: replay.position,
            total: target,
          });
          last = performance.now();
        }
        await new Promise((r) => setTimeout(r, 0));
      }
    }
    if (token !== generation) {
      state = undefined;
      return;
    }
    state = replay;
    send({ type: "state", request: data.request, state: serializable(replay) });
  } catch (error) {
    if (token === generation)
      send({ type: "error", message: (error as Error).message });
  }
}
