import { test, expect, type Page } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

async function observeDemoCompletion(page: Page, beforeBody = async () => {}) {
  let settled!: () => void;
  const finished = new Promise<void>((resolve) => {
    settled = resolve;
  });
  await page.exposeFunction("demoBody", beforeBody);
  await page.exposeFunction("demoSettled", () => settled());
  // A task after fetch/blob completion lets the app's await continuation run
  // before asserting that an obsolete result left no visible side effects.
  await page.evaluate(() => {
    const hooks = window as unknown as {
      demoBody: () => Promise<void>;
      demoSettled: () => void;
    };
    const original = window.fetch;
    window.fetch = async (...args) => {
      try {
        const response = await original(...args);
        if (!response.url.includes("healthy.ndjson")) return response;
        const blob = response.blob.bind(response);
        response.blob = async () => {
          try {
            const result = await blob();
            await hooks.demoBody();
            return result;
          } finally {
            setTimeout(() => hooks.demoSettled(), 0);
          }
        };
        if (!response.ok) setTimeout(() => hooks.demoSettled(), 0);
        return response;
      } catch (error) {
        setTimeout(() => hooks.demoSettled(), 0);
        throw error;
      }
    };
  });
  return { finished };
}

for (const kind of ["crash", "pause", "busy"])
  test(`export ending after ${kind} opens in the browser`, async ({ page }) => {
    const directory = await mkdtemp(join(tmpdir(), "lightpool-end-"));
    try {
      const scenario = JSON.parse(
        await readFile("../examples/sim/healthy.json", "utf8"),
      );
      scenario.Nodes = scenario.Nodes.slice(0, 1);
      scenario.Horizon = 2000000000;
      scenario.Actions = [
        { At: 100000000, Kind: kind, Node: "a", Duration: 3000000000 },
      ];
      if (kind === "busy") {
        scenario.Actions = [];
        scenario.Nodes[0].Service = 3000000000;
      }
      const input = join(directory, "scenario.json"),
        output = join(directory, "recording.ndjson");
      await writeFile(input, JSON.stringify(scenario));
      execFileSync(
        "go",
        [
          "run",
          "./cmd/lightpool-sim",
          "-scenario",
          input,
          "-out",
          output,
          "-revision",
          "browser-regression",
        ],
        { cwd: "..", timeout: 30000 },
      );
      await page.goto("/");
      await expect(page.locator("#status")).toContainText("atomic events");
      await page.locator("#file").setInputFiles(output);
      await expect(page.locator("#scene-size")).toHaveText("1 nodes");
      await expect(page.locator("#completion")).toHaveText(
        "Completed recording",
      );
      await expect(page.locator("#status")).toContainText("complete evidence");
      await page.locator("#timeline").fill("10000");
      await page.locator("#timeline").dispatchEvent("input");
      await expect(page.locator("#position")).toHaveText(
        kind === "busy" ? "Event 2 / 2" : "Event 4 / 4",
      );
      await expect(page.locator("#clock")).toHaveText(
        kind === "busy" ? "0.000 s" : "0.100 s",
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

for (const outcome of [
  "success",
  "http error",
  "network error",
  "body",
  "body error",
])
  test(`later local selection wins over obsolete demo ${outcome}`, async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator("#status")).toContainText("atomic events");
    let release!: () => void, started!: () => void;
    const held = new Promise<void>((resolve) => {
        release = resolve;
      }),
      pending = new Promise<void>((resolve) => {
        started = resolve;
      });
    const { finished } = await observeDemoCompletion(page, async () => {
      if (outcome.startsWith("body")) {
        started();
        await held;
        if (outcome === "body error") throw new Error("Body read failed");
      }
    });
    await page.route("**/demos/healthy.ndjson", async (route) => {
      if (!outcome.startsWith("body")) {
        started();
        await held;
      }
      if (outcome === "network error") {
        await route.abort();
        return;
      }
      await route.fulfill({
        status: outcome === "http error" ? 500 : 200,
        body: await readFile("public/demos/healthy.ndjson"),
      });
    });
    await page.locator("#demo").selectOption("healthy");
    await pending;
    await page.locator("#file").setInputFiles({
      name: "chosen-local.ndjson",
      mimeType: "application/x-ndjson",
      buffer: await readFile("../sim/trace/testdata/exact.ndjson"),
    });
    await expect(page.locator("#status")).toContainText("1 atomic events");
    await page.locator("#step").click();
    await expect(page.locator("#position")).toHaveText("Event 1 / 1");
    const status = await page.locator("#status").textContent(),
      url = page.url();
    release();
    await finished;
    await expect(page.locator("#scene-title")).toHaveText(
      "chosen-local.ndjson",
    );
    await expect(page.locator("#scene-size")).toHaveText("1 nodes");
    await expect(page.locator("#position")).toHaveText("Event 1 / 1");
    await expect(page.locator("#status")).toHaveText(status!);
    expect(page.url()).toBe(url);
  });

test("out-of-order demo responses preserve the later selection", async ({
  page,
}) => {
  await page.goto("/");
  await expect(page.locator("#status")).toContainText("atomic events");
  let release!: () => void, started!: () => void;
  const held = new Promise<void>((resolve) => {
      release = resolve;
    }),
    pending = new Promise<void>((resolve) => {
      started = resolve;
    });
  const { finished } = await observeDemoCompletion(page);
  await page.route("**/demos/healthy.ndjson", async (route) => {
    started();
    await held;
    await route.fulfill({
      body: await readFile("public/demos/healthy.ndjson"),
    });
  });
  await page.locator("#demo").selectOption("healthy");
  await pending;
  await page.locator("#demo").selectOption("crash");
  await expect(page.locator("#status")).toContainText("3,528 atomic events");
  const url = page.url();
  release();
  await finished;
  await expect(page.locator("#scene-title")).toHaveText("Process crash");
  await expect(page.locator("#status")).toContainText("3,528 atomic events");
  expect(page.url()).toBe(url);
});

test("static demo, controls, observer, local recovery and no upload", async ({
  page,
}) => {
  const errors: string[] = [],
    external: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("request", (r) => {
    if (
      !r.url().startsWith("http://127.0.0.1:4173") &&
      !r.url().startsWith("blob:") &&
      !r.url().startsWith("data:")
    )
      external.push(r.url());
    expect(r.method()).toBe("GET");
  });
  await page.goto("/?demo=pause&t=1700000000");
  await expect(page.locator("#status")).toContainText("atomic events");
  await expect(page.locator("#completion")).toHaveText("Completed recording");
  await expect(page.locator("canvas")).toBeVisible();
  await page.screenshot({
    path: "test-results/pause-desktop.png",
    fullPage: true,
  });
  const before = await page.locator("#position").textContent();
  await page.locator("#step").click();
  await expect(page.locator("#position")).not.toHaveText(before!);
  await page.locator("#observer").selectOption("b");
  await expect(page.locator("#observer-caption")).toContainText("seen by b");
  await page.locator("#timeline").fill("4300");
  await page.locator("#timeline").dispatchEvent("input");
  await expect(page.locator("#process")).toContainText("running");
  await expect(page.locator("#status")).toContainText("applied");
  await page.locator("#filter").selectOption("suspicion");
  await expect(page.locator("#events")).toContainText("refutation");
  await page.screenshot({
    path: "test-results/pause-refutation.png",
    fullPage: true,
  });
  await page.locator("#restart").click();
  await expect(page.locator("#position")).toHaveText("Event 0 / 3,638");
  await page.locator("#play").click();
  await expect(page.locator("#position")).not.toHaveText("Event 0 / 3,638");
  await page.locator("#play").click();
  const bytes = await readFile("../sim/trace/testdata/exact.ndjson");
  const prefix = Buffer.from(
    bytes.toString().split("\n").slice(0, 2).join("\n") + '\n{"type":',
  );
  await page.locator("#file").setInputFiles({
    name: "prefix.ndjson",
    mimeType: "application/x-ndjson",
    buffer: prefix,
  });
  await expect(page.locator("#completion")).toContainText("Incomplete");
  expect(new URL(page.url()).searchParams.has("demo")).toBe(false);
  expect(new URL(page.url()).searchParams.has("t")).toBe(false);
  await expect(page.locator("#status")).toContainText("valid prefix only");
  await page.locator("#step").click();
  await expect(page.locator("#member-detail")).toContainText(
    "9007199254740993",
  );
  const [download] = await Promise.all([
    page.waitForEvent("download"),
    page.locator("#save-event").click(),
  ]);
  const saved = await readFile((await download.path())!);
  expect(saved.toString()).toBe(bytes.toString().split("\n")[1] + "\n");
  await page.locator("#file").setInputFiles({
    name: "bad.ndjson",
    mimeType: "application/x-ndjson",
    buffer: Buffer.from("broken\n"),
  });
  await expect(page.locator("#status")).toContainText("line 1:");
  await expect(page.locator("#play")).toBeDisabled();
  await page.locator("#demo").selectOption("healthy");
  await expect(page.locator("#completion")).toHaveText("Completed recording");
  await expect(page.locator("#status")).toContainText("atomic events");
  expect(errors).toEqual([]);
  expect(external).toEqual([]);
});
test("high density teaching view", async ({ browser }) => {
  const context = await browser.newContext({
    viewport: { width: 1280, height: 900 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();
  await page.goto("/?demo=pause&t=2050000000");
  await expect(page.locator("#status")).toContainText("atomic events");
  await page.screenshot({ path: "test-results/pause-2x.png", fullPage: true });
  expect(
    await page
      .locator("canvas")
      .evaluate((c: HTMLCanvasElement) => c.width / c.clientWidth),
  ).toBeGreaterThan(1.9);
  await context.close();
});
for (const n of [1000, 5000])
  test(`renderer probe ${n} nodes with active state and selection`, async ({
    page,
  }) => {
    const nodes = Array.from({ length: n }, (_, i) => ({
        name: `n${i}`,
        address: `192.0.2.1:${i + 1}`,
      })),
      initial = nodes.map((p) => ({
        node: p.name,
        lifetime: "0",
        running: false,
        pausedUntil: "0",
      }));
    const header = {
      type: "header",
      format: "lightpool-trace",
      version: 1,
      manifest: { Code: `synthetic-renderer-probe-${n}` },
      nodes,
      initial,
    };
    const records: unknown[] = [header];
    for (let i = 0; i < 80; i++)
      records.push({
        type: "event",
        seq: String(i + 1),
        id: String(i + 1),
        time: String(i * 50000000),
        kind: "renderer.probe",
        lifetime: "1",
        changes:
          i === 0
            ? nodes.flatMap((p) => [
                {
                  kind: "process",
                  node: p.name,
                  phase: "start",
                  process: {
                    node: p.name,
                    lifetime: "1",
                    running: true,
                    pausedUntil: "0",
                  },
                },
                {
                  kind: "member.upsert",
                  node: "n0",
                  member: { ...p, state: "alive", incarnation: "1" },
                },
              ])
            : Array.from({ length: 100 }, (_, j) => ({
                kind: "member.upsert",
                node: "n0",
                member: {
                  ...nodes[(i * 100 + j) % n],
                  state: i % 2 ? "suspect" : "alive",
                  incarnation: String(i + 1),
                },
              })),
        traffic: Array.from({ length: 10 }, (_, j) => ({
          id: String(i * 10 + j + 1),
          transport: "datagram",
          stage: "attempt",
          from: `n${j}`,
          to: `n${(j + i + 1) % n}`,
          bytes: "100",
          messages: ["ping"],
        })),
      });
    await page.goto("/");
    await expect(page.locator("#status")).toContainText("atomic events");
    const start = Date.now();
    await page.locator("#file").setInputFiles({
      name: "renderer-probe.ndjson",
      mimeType: "application/x-ndjson",
      buffer: Buffer.from(
        records.map((r) => JSON.stringify(r)).join("\n") + "\n",
      ),
    });
    await expect(page.locator("#scene-size")).toHaveText(
      `${n.toLocaleString()} nodes`,
    );
    await expect(page.locator("#status")).toContainText("80 atomic events");
    const loadMs = Date.now() - start;
    await page.locator("#play").click();
    await page.waitForTimeout(1000);
    await page.locator("canvas").click({ position: { x: 100, y: 100 } });
    await expect(page.locator("#selected-name")).not.toHaveText("n0");
    const frames = await page.evaluate(async () => {
      const samples: number[] = [];
      let last = performance.now();
      for (let i = 0; i < 120; i++)
        await new Promise<void>((resolve) =>
          requestAnimationFrame((now) => {
            samples.push(now - last);
            last = now;
            resolve();
          }),
        );
      samples.sort((a, b) => a - b);
      return {
        median: samples[60],
        p95: samples[114],
        heap: (
          performance as unknown as { memory?: { usedJSHeapSize: number } }
        ).memory?.usedJSHeapSize,
      };
    });
    await page.locator("#play").click();
    await page.screenshot({
      path: `test-results/renderer-${n}.png`,
      fullPage: true,
    });
    console.log(
      JSON.stringify({
        probe: "synthetic renderer (not simulation throughput)",
        nodes: n,
        viewport: "1440x1080",
        loadMs,
        ...frames,
      }),
    );
  });
