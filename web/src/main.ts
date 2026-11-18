import "./style.css";
import { Scene } from "./scene.ts";
import type { End, Header } from "./trace.ts";
import type { EventPreview, View } from "./replay.ts";
const el = <T extends HTMLElement>(id: string) =>
  document.getElementById(id) as T;
const text = (id: string, value: string) => {
  el(id).textContent = value;
};
const worker = new Worker(new URL("./worker.ts", import.meta.url), {
  type: "module",
});
const scene = new Scene(el("scene"), (name) => {
  selected = name;
  scene.selected = name;
  render();
});
let activeDemo = false;
let selection = 0;
let header: Header | undefined,
  end: End | undefined,
  view: View | undefined,
  count = 0,
  duration = 0n,
  playing = false,
  busy = false,
  request = 0,
  selected = "",
  lastFrame = performance.now(),
  playTime = 0n,
  lastRequest = 0;
const names: Record<string, string> = {
  pause: "Pause & refutation",
  healthy: "Healthy cluster",
  crash: "Process crash",
  partition: "Partition & rejoin",
};
const params = new URLSearchParams(location.search);
let pendingTime = params.get("t") ?? "0";
const seconds = (v: string | bigint) =>
  `${(Number(BigInt(v)) / 1e9).toFixed(3)} s`;
const bytes = (v: string) => {
  const n = BigInt(v);
  return n >= 1048576n
    ? `${(Number(n) / 1048576).toFixed(2)} MiB`
    : n >= 1024n
      ? `${(Number(n) / 1024).toFixed(1)} KiB`
      : `${n} B`;
};
function status(message: string, error = false) {
  text("status", message);
  el("status").classList.toggle("error", error);
}
function stop() {
  playing = false;
  text("play", "Play");
}
function seek(position?: number, time?: bigint) {
  if (!header) return;
  busy = true;
  worker.postMessage({
    type: "seek",
    request: ++request,
    observer: el<HTMLSelectElement>("observer").value,
    position,
    time: time?.toString(),
  });
}
function load(blob: Blob, label: string, isDemo = false) {
  activeDemo = isDemo;
  if (!isDemo) {
    selection++;
    const url = new URL(location.href);
    url.searchParams.delete("demo");
    url.searchParams.delete("t");
    history.replaceState(null, "", url);
  }
  stop();
  view = undefined;
  header = undefined;
  busy = true;
  text("scene-title", label);
  text("completion", "Indexing recording…");
  status("Reading on this device…");
  el<HTMLButtonElement>("play").disabled = true;
  el<HTMLButtonElement>("step").disabled = true;
  worker.postMessage({ type: "load", blob });
}
async function demo(name: string) {
  const token = ++selection;
  const chosen = names[name] ? name : "pause";
  el<HTMLSelectElement>("demo").value = chosen;
  try {
    status("Fetching bundled recording…");
    const response = await fetch(
      `${import.meta.env.BASE_URL}demos/${chosen}.ndjson`,
    );
    if (!response.ok) throw new Error(`Demo fetch failed (${response.status})`);
    const blob = await response.blob();
    if (token !== selection) return;
    load(blob, names[chosen], true);
    const url = new URL(location.href);
    url.searchParams.set("demo", chosen);
    history.replaceState(null, "", url);
  } catch (e) {
    if (token === selection) status((e as Error).message, true);
  }
}
worker.onmessage = ({ data }) => {
  if (data.type === "record") {
    const url = URL.createObjectURL(data.blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `event-${data.position}.ndjson`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    return;
  }
  if (data.type === "progress") {
    status(
      `${data.phase} · ${data.bytes.toLocaleString()} / ${data.total.toLocaleString()}${data.phase === "Indexing" ? " bytes" : " events"} · playback remains cancellable`,
    );
    return;
  }
  if (data.type === "error") {
    busy = false;
    stop();
    header = undefined;
    text("completion", "Invalid recording");
    el("completion").classList.add("incomplete");
    status(data.message, true);
    el<HTMLButtonElement>("play").disabled = true;
    el<HTMLButtonElement>("step").disabled = true;
    return;
  }
  if (data.type === "loaded") {
    header = data.header;
    end = data.end;
    count = data.count;
    duration = BigInt(data.duration);
    selected = header!.nodes[0].name;
    scene.selected = selected;
    el<HTMLSelectElement>("observer").replaceChildren(
      ...header!.nodes.map((n) => new Option(n.name, n.name)),
    );
    text(
      "completion",
      end?.complete
        ? "Completed recording"
        : end
          ? `Incomplete · ${end.reason}`
          : "Incomplete · no end record",
    );
    el("completion").classList.toggle("incomplete", !end?.complete);
    const manifest = header!.manifest;
    text(
      "identity",
      `Source ${String(manifest.Code)} · ${String(manifest.Toolchain ?? "fixture")} · ${String(manifest.Platform ?? "portable")} · index ${bytes(String(data.indexBytes))}`,
    );
    text("scene-size", `${header!.nodes.length.toLocaleString()} nodes`);
    text("duration", `/ ${seconds(duration)}`);
    scene.load(header!);
    el("scene-placeholder").hidden = true;
    el<HTMLButtonElement>("play").disabled = false;
    el<HTMLButtonElement>("step").disabled = false;
    let time = 0n;
    try {
      time = BigInt(pendingTime);
    } catch {
      /* Invalid URL positions open at the beginning. */
    }
    pendingTime = "0";
    seek(undefined, time);
    return;
  }
  if (data.type === "state" && data.request === request) {
    busy = false;
    view = data.state;
    if (!playing) playTime = BigInt(view!.time);
    render();
    status(
      `${count.toLocaleString()} atomic events · ${view!.position.toLocaleString()} applied · ${end?.complete ? "complete evidence" : `valid prefix only${end ? ": " + end.reason : ""}`} · ${count > 80 ? "showing the last 80 applied events" : "full applied event window"}`,
    );
  }
};
function render() {
  if (!view) return;
  scene.update(view);
  const nodes = new Map(view.nodes),
    members = new Map(view.members),
    observer = view.observer,
    node = nodes.get(observer)!;
  text(
    "observer-caption",
    `Beliefs seen by ${observer} · outlined node is selected`,
  );
  text("health", node.summary?.health ?? "—");
  text("known", String(members.size));
  const paused = BigInt(node.process.pausedUntil) > BigInt(view.time);
  text(
    "process",
    `Process ${node.process.running ? "running" : "stopped"} · lifetime ${node.process.lifetime}${paused ? ` · processing paused until ${seconds(node.process.pausedUntil)}` : ""}${node.summary?.stopped ? " · protocol shut down" : ""}`,
  );
  text("selected-name", selected);
  const member = members.get(selected),
    process = nodes.get(selected)?.process;
  el("member-detail").replaceChildren();
  const lines = [
    `Node ${selected} · ${header?.nodes.find((n) => n.name === selected)?.address}`,
    `Belief at ${observer}: ${member?.state ?? "unknown"}`,
    `Incarnation: ${member?.incarnation ?? "unknown"}`,
    `Actual process: ${process?.running ? "running" : "stopped"} · lifetime ${process?.lifetime ?? "0"}`,
    ...(member?.meta ? [`Metadata (base64): ${member.meta}`] : []),
  ];
  for (const line of lines) {
    const div = document.createElement("div");
    div.textContent = line;
    el("member-detail").append(div);
  }
  text("timer-count", `(${view.timers.length})`);
  el("timers").replaceChildren();
  for (const [, p] of view.probes) {
    const div = document.createElement("div");
    div.className = "timer";
    div.textContent = `Probe → ${p.subject} · ${p.phase}${p.helpers?.length ? " via " + p.helpers.join(", ") : ""}`;
    el("timers").append(div);
  }
  for (const [, t] of view.timers.slice(0, 30)) {
    const div = document.createElement("div");
    div.className = "timer";
    div.textContent = `${t.purpose}${t.subject ? " → " + t.subject : ""} · due ${seconds(t.deadline!)}`;
    div.title = `id ${t.id}, generation ${t.generation}, operation ${t.operation}`;
    el("timers").append(div);
  }
  if (!view.timers.length && !view.probes.length)
    text("timers", "No active probes or timers.");
  text(
    "membership",
    view.members
      .slice(0, 200)
      .map(([n, m]) => `${n}: ${m.state} (${m.incarnation})`)
      .join(" · ") + (view.members.length > 200 ? " · first 200 shown" : ""),
  );
  for (const [id, value] of [
    ["sent", bytes(view.sent)],
    ["received", bytes(view.received)],
    ["attempts", BigInt(view.attempts).toLocaleString()],
    ["drops", BigInt(view.drops).toLocaleString()],
  ] as const)
    text(id, value);
  text("clock", seconds(view.time));
  text(
    "position",
    `Event ${view.position.toLocaleString()} / ${count.toLocaleString()}`,
  );
  el<HTMLInputElement>("timeline").value =
    duration === 0n ? "0" : String((BigInt(view.time) * 10000n) / duration);
  el<HTMLButtonElement>("save-event").disabled = view.position === 0;
  text(
    "sampling",
    `${view.trails.length} / 96 recent trails · animation sampled · exact traffic totals`,
  );
  renderEvents();
  if (activeDemo) {
    const url = new URL(location.href);
    url.searchParams.set("t", view.time);
    history.replaceState(null, "", url);
  }
}
function describe(e: EventPreview) {
  const parts: string[] = [];
  for (const c of e.changes) {
    if (c.kind === "summary") continue;
    parts.push(
      c.kind === "member.upsert"
        ? `${c.node} sees ${c.member!.name} ${c.member!.state} #${c.member!.incarnation}`
        : c.kind === "timer"
          ? `${c.phase} ${c.purpose} @ ${seconds(c.deadline!)}`
          : `${c.node} ${c.kind} ${c.subject ?? ""} ${c.phase ?? ""} ${c.outcome ?? ""}${c.helpers?.length ? " via " + c.helpers.join(", ") : ""}`,
    );
  }
  for (const t of e.traffic)
    parts.push(
      `${t.from} → ${t.to || "?"} · ${t.transport} ${t.stage} · ${t.messages.join("+")} · ${t.bytes} B${t.detail ? " · " + t.detail : ""}${t.received ? " · arrived " + seconds(t.received) : ""}`,
    );
  if (e.omittedChanges || e.omittedTraffic)
    parts.push(
      `+${e.omittedChanges} changes / ${e.omittedTraffic} traffic observations; save original event JSON`,
    );
  return parts.join(" | ") || "No visible state change";
}
function renderEvents() {
  if (!view) return;
  const filter = el<HTMLSelectElement>("filter").value;
  el("events").replaceChildren();
  for (const e of [...view.events].reverse()) {
    const matches =
      filter === "all" ||
      (filter === "traffic" && e.traffic.length > 0) ||
      (filter === "action" && e.kind.startsWith("action.")) ||
      e.categories.some((c) =>
        filter === "suspicion"
          ? c === "suspicion" || c === "refutation"
          : c === filter,
      );
    if (!matches) continue;
    const row = document.createElement("div");
    row.className = "event-row";
    row.tabIndex = 0;
    row.setAttribute("role", "button");
    const d = describe(e);
    for (const [i, value] of [
      seconds(e.time),
      `${e.actor ?? "network"} / ${e.kind}`,
      d,
    ].entries()) {
      const span = document.createElement("span");
      span.textContent = value;
      span.title = value;
      if (i === 1) span.className = "event-kind";
      row.append(span);
    }
    row.title = `Sequence ${e.seq} · event ${e.id}\n${d}`;
    const go = () => {
      stop();
      seek(Number(BigInt(e.seq)));
    };
    row.onclick = go;
    row.onkeydown = (ev) => {
      if (ev.key === "Enter") go();
    };
    el("events").append(row);
  }
}
el("save-event").onclick = () => {
  if (view) worker.postMessage({ type: "record", position: view.position });
};
el("play").onclick = () => {
  if (playing) {
    stop();
    return;
  }
  playing = true;
  lastFrame = performance.now();
  playTime = BigInt(view?.time ?? "0");
  if (view?.position === count) {
    playTime = 0n;
    seek(0);
  }
  text("play", "Pause");
};
el("step").onclick = () => {
  stop();
  seek((view?.position ?? 0) + 1);
};
el("restart").onclick = () => {
  stop();
  seek(0);
};
el("fit").onclick = () => scene.fit();
el("filter").onchange = renderEvents;
el("observer").onchange = () => {
  stop();
  seek(view?.position ?? 0);
};
el("timeline").oninput = () => {
  stop();
  seek(
    undefined,
    (duration * BigInt(el<HTMLInputElement>("timeline").value)) / 10000n,
  );
};
el("demo").onchange = () => {
  pendingTime = "0";
  void demo(el<HTMLSelectElement>("demo").value);
};
el("file").onchange = () => {
  const f = el<HTMLInputElement>("file").files?.[0];
  if (f) {
    pendingTime = "0";
    load(f, f.name);
  }
};
window.addEventListener("dragover", (e) => {
  e.preventDefault();
  document.body.classList.add("dragging");
});
window.addEventListener("dragleave", (e) => {
  if (!e.relatedTarget) document.body.classList.remove("dragging");
});
window.addEventListener("drop", (e) => {
  e.preventDefault();
  document.body.classList.remove("dragging");
  const f = e.dataTransfer?.files[0];
  if (f) {
    pendingTime = "0";
    load(f, f.name);
  }
});
function frame(now: number) {
  const elapsed = Math.min(100, now - lastFrame);
  lastFrame = now;
  if (playing && view && !busy) {
    playTime += BigInt(
      Math.round(elapsed * 1e6 * Number(el<HTMLSelectElement>("speed").value)),
    );
    if (now - lastRequest > 40) {
      seek(undefined, playTime);
      lastRequest = now;
    }
    if (playTime >= duration) stop();
  }
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
try {
  await scene.init();
  await demo(params.get("demo") ?? "pause");
} catch (e) {
  status(`WebGL initialization failed: ${(e as Error).message}`, true);
  text(
    "scene-placeholder",
    "A WebGL-capable browser is required for the cluster view.",
  );
}
