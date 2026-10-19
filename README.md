# @profullstack/scraper-daemon

One updater daemon for every listing site. Each site keeps its own repo and runs the daemon as a role of its own process (no new service, no new subdomain); the package is the shared architecture: how work is scheduled, how the web is read, where state lives and how it reports.

Zero dependencies. Node 20+ and Bun.

```sh
npm i @profullstack/scraper-daemon
```

## Three kinds of work

```js
import { createDaemon, sqliteStore } from '@profullstack/scraper-daemon';

const daemon = createDaemon({
  name: 'search.peptides',
  store: sqliteStore(db),             // or pgStore(query), memoryStore()
  concurrency: 6,                     // tasks in flight across the daemon
  fetch: { userAgent: 'search.peptides (+https://search.peptides.pit.world)', delayMs: 1000 },
  onStall: ({ job }) => alert(`${job} has not succeeded in a while`),
});

// 1. A periodic job: sync a register, run a discovery source, purge old rows.
daemon.job({
  name: 'register',
  every: '1h',
  staleAfter: '6h',
  run: async (ctx) => {
    const doc = await ctx.fetch.json('https://example.com/data.json');
    await save(doc);
    return { vendors: doc.vendors.length };
  },
});

// 2. A queue: one refresh per item, oldest due first, a host's items in series.
daemon.queue({
  name: 'contacts',
  every: '7d',
  retireAfter: 10,                    // stop polling an item after 10 failures in a row
  items: async () => vendors().map((v) => ({ key: v.slug, url: v.website })),
  run: async (item, ctx) => {
    const page = await ctx.fetch.text(item.url);
    await saveContacts(item.key, parse(page.text));
    // optional: { nextIn: '1d' } sets this item's own cadence, { retire: true } stops it
  },
});

// 3. A pipeline: a bulk import that only runs when the source changed, and resumes
//    from the step that failed instead of starting over.
daemon.pipeline({
  name: 'dump',
  every: '6h',
  gate: async (ctx) => ({ token: await headStamp('https://example.com/dump.tgz') }),
  steps: [
    { name: 'download', run: async (ctx) => download(ctx.token) },
    { name: 'build', run: async () => build() },
    { name: 'swap', run: async () => swapTables() },
  ],
});

daemon.start();
app.get('/api/v1/daemon', async (c) => c.json(await daemon.status()));
process.on('SIGTERM', () => daemon.stop().then(() => process.exit(0)));
```

## What every site gets

- **Scheduling.** Due work only, oldest first; `every` with jitter; an item can set its own cadence (`nextIn`), retire itself, or be retired after `retireAfter` failures in a row. Failures climb an error ladder (`5m, 15m, 1h, 3h, 6h, 12h, 1d` by default, `backoff` to change it). One run per job at a time; ticks never overlap. Whatever a discovery job adds to a queue's `items()` is picked up within `itemsEvery` (or at once with `daemon.refreshItems(name)`).
- **Polite fetching.** `ctx.fetch` keeps one request at a time per host with `delayMs` between them (hosts run side by side), reads and caches robots.txt (RFC 9309 longest match), and on a 429 (or a 503 with Retry-After) holds the whole host back for as long as it asked, without counting it against the item.
- **Safe fetching.** http(s) on 80/443 only, every resolved address must be public (private, loopback, link-local, CGNAT, v4-mapped and NAT64 refused), every redirect hop re-checked, time and size capped. Errors are `FetchError` with a `code`: `blocked`, `robots`, `status`, `timeout`, `too-large`, `network`.
- **State.** `scraper_daemon_runs` (last try, last success, next due, failures in a row, last error per job and key) and `scraper_daemon_state` (cursors, stamps, pipeline progress) in the database the site already has: `sqliteStore(db)` (bun:sqlite, node:sqlite, better-sqlite3) or `pgStore(query)` (any `query(text, params)`). `ctx.state.get/set` is per task.
- **Reporting.** `daemon.status()` (tasks, due counts, failing and retired items, held hosts, the last 20 runs) for `/api/v1/daemon` and `/health`; `onRun(entry)` for a site's own run log; `onStall` when a task has not succeeded within `staleAfter`.
- **CLI.** `npx @profullstack/scraper-daemon status https://site` prints what a site's daemon is doing.

## Adopting it on a site that already has a loop

Wrap, do not rewrite: a mature crawler (its own due-row selection, cadence and per-host spreading) becomes one `job` whose `run` calls the existing function, and gains the shared status, stall alerts and run log. New work (contact enrichment, discovery sources) goes in as `queue`s and `job`s beside it.

## Runs on

search.peptides, ch4nnels.com, r4ck.dev, rssamplifier.com, p0dcasters.com.

MIT
