import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  buildIndex,
  Framer,
  Validator,
  readEvents,
  positionAt,
} from "./trace.ts";
import { apply, initialState } from "./replay.ts";
import type { Event } from "./trace.ts";
const fixture = await readFile(
  new URL("../../sim/trace/testdata/exact.ndjson", import.meta.url),
);
const lines = fixture.toString().trimEnd().split("\n");
test("shared exact integer fixture and random access", async () => {
  const index = await buildIndex(new Blob([fixture]));
  assert.equal(index.end?.complete, true);
  assert.equal(index.times[0], 9007199254740993n);
  const events = [];
  for await (const e of readEvents(index, 0, 1)) events.push(e);
  assert.equal(events[0].id, "18446744073709551615");
  const state = initialState(index.header, "α");
  apply(state, events[0]);
  assert.equal(state.members.get("α")?.incarnation, "9007199254740993");
  assert.equal(positionAt(index, 9007199254740993n), 1);
  assert.equal(positionAt(index, 9007199254740992n), 0);
});
test("arbitrary byte chunks including split UTF-8 and newline", () => {
  for (const size of [1, 2, 3, 7, 64, fixture.length]) {
    const v = new Validator(),
      offsets: number[] = [];
    const f = new Framer((text, offset) => {
      v.parse(text);
      offsets.push(offset);
    });
    for (let i = 0; i < fixture.length; i += size)
      f.push(fixture.subarray(i, i + size));
    assert.equal(v.end?.complete, true);
    assert.equal(offsets[1], Buffer.byteLength(lines[0]) + 1);
  }
});
test("prefix and partial trailing line stay incomplete", async () => {
  for (const tail of ["", '{"type":"end"', "bad trailing bytes"]) {
    const index = await buildIndex(
      new Blob([lines[0] + "\n" + lines[1] + "\n" + tail]),
    );
    assert.equal(index.end, undefined);
    assert.equal(index.times.length, 1);
  }
});
test("interior corruption, unknown versions, references and unsafe integers reject with line", async () => {
  const bad = [
    lines[0] + "\n{broken}\n",
    fixture.toString().replace('"version":1', '"version":2'),
    fixture.toString().replace('"seq":"1"', '"seq":"2"'),
    fixture.toString().replace('"actor":"α"', '"actor":"missing"'),
    fixture
      .toString()
      .replace(
        '"incarnation":"9007199254740993"',
        '"incarnation":9007199254740993',
      ),
    fixture.toString() + lines[1] + "\n",
  ];
  for (const text of bad)
    await assert.rejects(buildIndex(new Blob([text])), /line \d+:/);
});
test("atomic replay preserves pause beliefs, clears restarts, removes members, cancels timers", () => {
  const v = new Validator(),
    h = v.parse(lines[0]);
  assert.equal(h.type, "header");
  if (h.type !== "header") return;
  const event = JSON.parse(lines[1]) as Event,
    state = initialState(h, "α");
  apply(state, event);
  apply(state, {
    ...event,
    changes: [
      {
        kind: "process",
        node: "α",
        phase: "pause",
        process: {
          node: "α",
          lifetime: "1",
          running: true,
          pausedUntil: "9007199254740999",
        },
      },
      {
        kind: "timer",
        node: "α",
        id: "7",
        phase: "arm",
        deadline: "9007199254740999",
      },
    ],
    traffic: [],
  });
  assert.equal(state.members.size, 1);
  assert.equal(state.timers.size, 1);
  apply(state, {
    ...event,
    changes: [
      { kind: "member.remove", node: "α", subject: "α" },
      { kind: "timer", node: "α", id: "7", phase: "cancel" },
    ],
    traffic: [],
  });
  assert.equal(state.members.size, 0);
  assert.equal(state.timers.size, 0);
  apply(state, event);
  apply(state, {
    ...event,
    changes: [
      {
        kind: "process",
        node: "α",
        phase: "start",
        process: { node: "α", lifetime: "2", running: true, pausedUntil: "0" },
      },
    ],
    traffic: [],
  });
  assert.equal(state.members.size, 0);
});
test("straight playback, backwards seek and observer reconstruction agree on Go demos", async () => {
  const bytes = await readFile(
    new URL("../public/demos/pause.ndjson", import.meta.url),
  );
  const index = await buildIndex(new Blob([bytes]));
  for (const observer of ["a", "b"]) {
    const a = initialState(index.header, observer),
      b = initialState(index.header, observer);
    const midpoint = Math.floor(index.times.length / 2);
    for await (const e of readEvents(index, 0, index.times.length)) apply(a, e);
    for await (const e of readEvents(index, 0, midpoint)) apply(b, e);
    for await (const e of readEvents(index, midpoint, index.times.length))
      apply(b, e);
    assert.deepEqual(b, a);
    assert.equal(a.events.length, 80);
    assert.ok(a.trails.length <= 96);
    const reduced = initialState(index.header, observer);
    for await (const e of readEvents(index, 0, midpoint)) apply(reduced, e);
    const again = initialState(index.header, observer);
    for await (const e of readEvents(index, 0, midpoint)) apply(again, e);
    assert.deepEqual(again, reduced);
  }
});

test("Go snapshot fixtures match viewer state at six event boundaries and every observer", async () => {
  const samples = JSON.parse(
    await readFile(
      new URL("../../sim/trace/testdata/pause-states.json", import.meta.url),
      "utf8",
    ),
  ) as {
    horizon: string;
    seq: string;
    nodes: {
      node: string;
      running: boolean;
      pausedUntil: string;
      members: unknown[];
      summary: unknown;
    }[];
  }[];
  const bytes = await readFile(
    new URL("../public/demos/pause.ndjson", import.meta.url),
  );
  const index = await buildIndex(new Blob([bytes]));
  for (const observer of index.header.nodes.map((n) => n.name)) {
    const state = initialState(index.header, observer);
    let previous = 0;
    for (const sample of samples) {
      const position = positionAt(index, BigInt(sample.horizon));
      assert.equal(String(position), sample.seq);
      for await (const e of readEvents(index, previous, position))
        apply(state, e);
      previous = position;
      const want = sample.nodes.find((n) => n.node === observer)!;
      assert.deepEqual(
        [...state.members.values()].sort((a, b) =>
          a.name.localeCompare(b.name),
        ),
        want.members,
      );
      assert.deepEqual(state.nodes.get(observer)?.summary, want.summary);
      assert.equal(state.nodes.get(observer)?.process.running, want.running);
      assert.equal(
        state.nodes.get(observer)?.process.pausedUntil,
        want.pausedUntil,
      );
    }
  }
});

test("invalid UTF-8 reports a line and an incomplete end never becomes complete", async () => {
  const corrupt = new Uint8Array([
    ...new TextEncoder().encode(lines[0] + "\n"),
    0xff,
    10,
  ]);
  await assert.rejects(
    buildIndex(new Blob([corrupt])),
    /line 2: invalid UTF-8/,
  );
  const index = await buildIndex(
    new Blob([
      fixture.toString().replace('"complete":true', '"complete":false'),
    ]),
  );
  assert.equal(index.end?.complete, false);
});
