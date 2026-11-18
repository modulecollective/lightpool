import {
  Application,
  Container,
  Graphics,
  Sprite,
  Text,
  Texture,
} from "pixi.js";
import type { Header } from "./trace.ts";
import type { View } from "./replay.ts";
const palette = {
  alive: 0x61c7b5,
  suspect: 0xeeb75b,
  dead: 0xee7b70,
  left: 0xbdc5d3,
  unknown: 0x586a7c,
};
export class Scene {
  app = new Application();
  world = new Container();
  trails = new Graphics();
  outline = new Graphics();
  sprites = new Map<string, Sprite>();
  labels = new Map<string, Text>();
  positions = new Map<string, { x: number; y: number }>();
  texture?: Texture;
  focusLabel = new Text({
    text: "",
    style: { fill: 0xffffff, fontFamily: "system-ui", fontSize: 11 },
  });
  selected = "";
  view?: View;
  names: string[] = [];
  zoom = 1;
  pan = { x: 0, y: 0 };
  ready = false;
  private host: HTMLElement;
  private select: (name: string) => void;
  constructor(host: HTMLElement, select: (name: string) => void) {
    this.host = host;
    this.select = select;
  }
  async init() {
    await this.app.init({
      background: "#101e2a",
      antialias: true,
      resolution: Math.min(devicePixelRatio, 2),
      autoDensity: true,
      resizeTo: this.host,
      preference: "webgl",
    });
    this.host.appendChild(this.app.canvas);
    this.app.stage.addChild(this.world);
    this.world.addChild(this.trails, this.outline, this.focusLabel);
    this.ready = true;
    let drag = false,
      moved = false,
      last = { x: 0, y: 0 };
    this.host.addEventListener("pointerdown", (e) => {
      drag = true;
      moved = false;
      last = { x: e.clientX, y: e.clientY };
    });
    window.addEventListener("pointermove", (e) => {
      if (!drag) return;
      let dx = e.clientX - last.x,
        dy = e.clientY - last.y;
      if (Math.abs(dx) + Math.abs(dy) > 2) moved = true;
      this.pan.x += dx;
      this.pan.y += dy;
      last = { x: e.clientX, y: e.clientY };
      this.transform();
    });
    window.addEventListener("pointerup", () => {
      drag = false;
    });
    this.host.addEventListener(
      "wheel",
      (e) => {
        e.preventDefault();
        this.zoom = Math.max(
          0.25,
          Math.min(8, this.zoom * Math.exp(-e.deltaY * 0.001)),
        );
        this.transform();
      },
      { passive: false },
    );
    this.app.stage.eventMode = "static";
    this.app.stage.hitArea = this.app.screen;
    this.app.stage.on("pointertap", (e) => {
      if (moved) return;
      const local = this.world.toLocal(e.global);
      let closest = "",
        d = Infinity;
      for (const [name, p] of this.positions) {
        const distance = Math.hypot(p.x - local.x, p.y - local.y);
        if (distance < d) {
          d = distance;
          closest = name;
        }
      }
      if (d < 25 / this.zoom) this.select(closest);
    });
    new ResizeObserver(() => {
      this.layout();
      this.update(this.view);
    }).observe(this.host);
    this.app.ticker.add(() => this.animate());
  }
  load(header: Header) {
    for (const s of this.sprites.values()) s.destroy();
    for (const s of this.labels.values()) s.destroy();
    this.sprites.clear();
    this.labels.clear();
    this.positions.clear();
    this.names = header.nodes.map((n) => n.name);
    if (!this.texture) {
      const g = new Graphics().circle(0, 0, 12).fill(0xffffff);
      this.texture = this.app.renderer.generateTexture(g);
      g.destroy();
    }
    const texture = this.texture;
    // All nodes share one texture and tint; text stays selective at large scale.
    for (const name of this.names) {
      const sprite = new Sprite(texture);
      sprite.anchor.set(0.5);
      this.world.addChild(sprite);
      this.sprites.set(name, sprite);
      if (this.names.length <= 20) {
        const label = new Text({
          text: name,
          style: { fill: 0xbed2d6, fontFamily: "system-ui", fontSize: 12 },
        });
        label.anchor.set(0.5);
        this.world.addChild(label);
        this.labels.set(name, label);
      }
    }
    this.fit();
    this.layout();
  }
  fit() {
    this.zoom = 1;
    this.pan = { x: 0, y: 0 };
    this.transform();
  }
  transform() {
    this.world.position.set(this.pan.x, this.pan.y);
    this.world.scale.set(this.zoom);
  }
  layout() {
    if (!this.ready) return;
    const w = this.host.clientWidth,
      h = this.host.clientHeight,
      n = this.names.length;
    this.names.forEach((name, i) => {
      let x: number, y: number;
      if (n <= 20) {
        const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
        const r = Math.min(w * 0.32, h * 0.32);
        x = w / 2 + Math.cos(a) * r;
        y = h / 2 + Math.sin(a) * r;
      } else {
        const cols = Math.ceil(Math.sqrt((n * w) / h)),
          rows = Math.ceil(n / cols);
        x = 25 + ((i % cols) * (w - 50)) / cols;
        y = 35 + (Math.floor(i / cols) * (h - 80)) / rows;
      }
      this.positions.set(name, { x, y });
      this.sprites.get(name)!.position.set(x, y);
      this.labels.get(name)?.position.set(x, y + 25);
    });
  }
  update(view?: View) {
    if (!view || !this.ready) return;
    this.view = view;
    const members = new Map(view.members),
      nodes = new Map(view.nodes);
    this.outline.clear();
    this.focusLabel.visible = this.names.length > 20;
    const focus = this.positions.get(this.selected);
    if (focus) {
      this.focusLabel.text = this.selected;
      this.focusLabel.position.set(focus.x + 10, focus.y - 15);
    }
    for (const [name, sprite] of this.sprites) {
      const p = this.positions.get(name)!;
      const node = nodes.get(name);
      const state = members.get(name)?.state ?? "unknown";
      sprite.tint = palette[state];
      const size =
        this.names.length <= 20
          ? 18
          : Math.max(
              2.2,
              Math.min(
                7,
                this.host.clientWidth / Math.sqrt(this.names.length) / 3,
              ),
            );
      sprite.width = sprite.height = size;
      if (!node?.process.running) {
        sprite.rotation = Math.PI / 4;
        sprite.alpha = 0.5;
      } else {
        sprite.rotation = 0;
        sprite.alpha = 1;
      }
      if (
        node?.process.running &&
        BigInt(node.process.pausedUntil) > BigInt(view.time)
      )
        this.outline
          .circle(p.x, p.y, size * 0.85)
          .stroke({ color: 0xeeb75b, width: 1.5 });
      if (name === this.selected || name === view.observer) {
        this.outline.circle(p.x, p.y, size * 0.8 + 5).stroke({
          color: name === this.selected ? 0xffffff : 0x38887f,
          width: name === this.selected ? 1.5 : 1,
        });
      }
      if (!node?.process.running) {
        this.outline
          .rect(p.x - size / 2, p.y - size / 2, size, size)
          .stroke({ color: 0x839aa5, width: 1 });
      }
    }
  }
  animate() {
    if (!this.view) return;
    this.trails.clear();
    const time = performance.now();
    for (const t of this.view.trails) {
      const a = this.positions.get(t.from),
        b = this.positions.get(t.to);
      if (!a || !b) continue;
      const color =
        t.stage === "drop"
          ? 0xee7b70
          : t.stage === "handle"
            ? 0x91b9dc
            : 0x4e9e95;
      this.trails
        .moveTo(a.x, a.y)
        .lineTo(b.x, b.y)
        .stroke({ color, alpha: 0.22, width: 1 });
      const p = (time / 1000 + Number(BigInt(t.id) % 100n) / 100) % 1;
      this.trails
        .circle(
          a.x + (b.x - a.x) * p,
          a.y + (b.y - a.y) * p,
          this.names.length > 20 ? 1.5 : 2.5,
        )
        .fill({ color, alpha: 0.9 });
    }
  }
}
