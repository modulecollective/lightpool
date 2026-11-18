# Simulation explorer

Static TypeScript/PixiJS viewer for [trace v1](../docs/trace-v1.md). Go owns
the protocol and virtual clock. Playback only reduces recorded changes.

Use Node 26.13.0 and npm 11 (the lockfile pins all frontend dependencies):

```sh
cd web
npm ci
npm run dev
```

`npm run build` creates `dist/`; serve that directory on any static HTTP
host. Bundled recordings need no Go/backend, external CDN, account or upload.
Open `?demo=pause&t=2102539563` to select a demo and nanosecond position.
The other demos are `healthy`, `crash` and `partition`. A file picker or drop
opens a local recording on the device. Without a valid complete end record,
the viewer labels the valid prefix incomplete. Invalid complete lines name
the line and refuse playback. Open another recording to recover.

Controls: play/pause, speed, next event, restart, time scrub, observer and
node selection, event filters, scroll zoom and drag pan. Colors are the
selected observer's beliefs; squares show stopped processes, rings show
processing pauses. Unknown is distinct from alive. Stable layout implies
neither network distance nor reachability. Messages animate illustratively
between recorded endpoints; animation is not an extra protocol state.

The worker retains the original Blob, numeric offset/time indices and
per-node indices. Seeking replays authoritative changes from the beginning
when needed, reconstructing one observer's membership. It is cancellable.
Only 80 event previews (24 changes/traffic entries each), 96 recent trails
and one requested state are sent to the UI. Choose a row and **Save current
event JSON** to recover its full original evidence. Filtering never changes
state. The membership list shows the first 200 entries; selecting any node
inspects that member. There is no all-pairs belief matrix in the browser.
Index/file memory grows with recording size, and long seeks remain linear.

```sh
npm run typecheck
npm test
npm run build
npx playwright install --with-deps chromium
npm run smoke
```

The browser suite starts a static preview, probes it, and tears it down.
Its 1,000/5,000-node files are explicitly synthetic renderer probes; their
frame times do not measure Go simulation throughput. Browser screenshots
and reports land under ignored `test-results/`.
