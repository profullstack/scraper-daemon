# @profullstack/scraper-daemon

One updater daemon for every listing site, on [BullMQ](https://bullmq.io). Each site keeps its own repo and runs the daemon as a role of its own process (no new service, no new subdomain). The package is the standard: how work is queued and scheduled (BullMQ on the site's Redis), how the web is read, how it fails and backs off, and how it reports. **Each app puts its data where it wants**: the daemon hands your `run` functions the work, and they write to the app's own SQLite, Postgres or anything else; the daemon keeps only its queue and run records, in Redis.

Node 20+ and Bun. `bullmq` and `ioredis` are peer dependencies.

```sh
npm i @profullstack/scraper-daemon bullmq ioredis
```

```js
const daemon = createDaemon({ name: 'my-site', redis: process.env.REDIS_URL });
```

Give each site its own Redis (`redis:8-alpine`, `--maxmemory-policy noeviction`, as BullMQ requires). Without `redis` the local engine runs the same tasks in-process with a memory, SQLite or Postgres store: for tests and dev, not production.

### What the BullMQ engine does for you

- One queue per site (`queueName`, default `name`). A BullMQ job scheduler fires the dispatcher tick every `tickMs`, so only one tick runs at a time however many processes there are; the dispatcher enqueues due work and one Worker runs it at `concurrency`.
- Job ids are `<task>--<key>`, URL-encoded, never containing `:` (BullMQ rejects colons in custom ids). A unit already queued is not queued twice.
- Processors return nothing and completed jobs are removed; event streams are capped (`maxLen: 100`) on the Queue and the Worker. Nothing grows without bound in Redis.
- No `QueueEvents`, no waiting on a job's result: callers never block on a lost notification.
- A watchdog pings Redis through the daemon's own connection every 30 s and, after three misses, exits the process (ioredis with `maxRetriesPerRequest: null` waits forever on a dead socket; a fresh process is the fix). Override with `onRedisDown`. `status()` gives up on Redis after 3 s rather than hanging a status page.
- Run records and `ctx.state` live in Redis (`sd:<queue>:runs:<task>`, `sd:<queue>:state`) unless you pass a `store`.

## Three kinds of work

```js
import { createDaemon } from '@profullstack/scraper-daemon';

const daemon = createDaemon({
  name: 'search.peptides',
  redis: process.env.REDIS_URL,       // BullMQ; the site's listings stay in its own database
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
- **State.** Per job and key: last try, last success, next due, failures in a row, last error; plus cursors, stamps and pipeline progress through `ctx.state.get/set` (per task). In Redis with the BullMQ engine; `sqliteStore(db)` / `pgStore(query)` / `memoryStore()` are there for the local engine or a site that wants its run records in its own database.
- **Reporting.** `daemon.status()` (tasks, due counts, failing and retired items, held hosts, the last 20 runs) for `/api/v1/daemon` and `/health`; `onRun(entry)` for a site's own run log; `onStall` when a task has not succeeded within `staleAfter`.
- **CLI.** `npx @profullstack/scraper-daemon status https://site` prints what a site's daemon is doing.

## Adopting it on a site that already has a loop

Wrap, do not rewrite: a mature crawler (its own due-row selection, cadence and per-host spreading) becomes one `job` whose `run` calls the existing function, and gains the shared status, stall alerts and run log. New work (contact enrichment, discovery sources) goes in as `queue`s and `job`s beside it.

## Runs on

search.peptides, ch4nnels.com, r4ck.dev, rssamplifier.com, p0dcasters.com.

MIT
