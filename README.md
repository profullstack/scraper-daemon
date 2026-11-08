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

- **Scheduling.** Due work only, oldest first; `every` with jitter; an item can set its own cadence (`nextIn`), retire itself, or be retired after `retireAfter` failures in a row (and then rechecked once every 30 days, `retireRecheck`). Failures climb an error ladder (`5m, 15m, 1h, 3h, 6h, 12h, 1d` by default, `backoff` to change it), except slow or broken targets, which are flagged for 30 days (below). One run per job at a time; ticks never overlap. Whatever a discovery job adds to a queue's `items()` is picked up within `itemsEvery` (or at once with `daemon.refreshItems(name)`).
- **Polite fetching.** `ctx.fetch` keeps one request at a time per host with `delayMs` between them (hosts run side by side), reads and caches robots.txt (RFC 9309 longest match), and on a 429 (or a 503 with Retry-After) holds the whole host back for as long as it asked, without counting it against the item.
- **Safe fetching.** http(s) on 80/443 only, every resolved address must be public (private, loopback, link-local, CGNAT, v4-mapped and NAT64 refused), every redirect hop re-checked, time and size capped: **10 s for the whole request** (every redirect hop, headers and body; `fetch: { timeoutMs }` to change it) and 2 MB. Errors are `FetchError` with a `code`: `blocked`, `robots`, `status`, `timeout`, `too-large`, `network` (with `dnsCode` / `causeCode` keeping the system code).
- **State.** Per job and key: last try, last success, next due, failures in a row, last error; plus cursors, stamps and pipeline progress through `ctx.state.get/set` (per task). In Redis with the BullMQ engine; `sqliteStore(db)` / `pgStore(query)` / `memoryStore()` are there for the local engine or a site that wants its run records in its own database.
- **Reporting.** `daemon.status()` (tasks, due counts, failing and retired items, held hosts, the last 20 runs) for `/api/v1/daemon` and `/health`; `onRun(entry)` for a site's own run log; `onStall` when a task has not succeeded within `staleAfter`.
- **CLI.** `npx @profullstack/scraper-daemon status https://site` prints what a site's daemon is doing.

## Slow or broken targets: flagged for 30 days

The fleet crawler rule: a third-party fetch gets **10 s** (`CRAWL_FETCH_TIMEOUT_MS`), a headless-browser render **30 s** (`CRAWL_BROWSER_TIMEOUT_MS`, for your own Playwright/Obscura code), and a target that is slow or broken is **left alone for 30 days**, then gets exactly one recheck. Success clears the flag; another failure flags it for another 30 days. Nothing walks a minutes/hours ladder against a site that is down, and nothing is dead forever.

- **Slow**: a timeout (`FetchError` code `timeout`, an `AbortSignal.timeout`, undici/Bun connect/headers/body timeouts, or the task's own `timeout` firing).
- **Broken**: DNS (`ENOTFOUND`), TLS/certificate errors, refused/reset/unreachable connections, any HTTP 5xx.
- **Neither** (the ladder, as before): 4xx, robots, blocked, too-large, parse errors, bugs in your own code, and `EAI_AGAIN` (your own resolver failing for a moment fails every host at once; it is not the site). A 429, or a 503 with Retry-After, still holds the host for as long as it asked (capped at a day) and does not count against the item.

Queue items record it in their run record: `flag` (`'slow'`, `'broken'`, `'retired'`, null), `flaggedAt`, `lastError`, `lastTry` (the failure time) and `nextAt` 30 days out; `status()` counts them as `flagged`. Feeds get it in `fail(feed, failure)`: `failure.flag` and `failure.minutes` (43,200) / `failure.nextAt`, so your `fail` writes the 30-day next check into your own table as it already writes the ladder's. The BullMQ engine never retries a job (`attempts: 1`), so the flag is the only schedule.

Options (per task, or as `createDaemon` defaults): `flagFor` (default `'30d'` for `queue` and `feeds`; `false` for `job` and `pipeline`, which are whole syncs rather than one target), `retireRecheck` (default `'30d'`), `classify(err)` to override the classification. **Exceptions keep their fast rechecks with `flagFor: false`**: uptime/health monitors, ad link-health, and calls to your own services or vendors. `{ retire: true }` returned by `run` is your own decision and still retires for good; `retireRecheck: false` restores retire-forever for `retireAfter` too.

`failureClass(err)` and `flagPlan(err, { fails, retireAfter, ladderMs })` are exported pure functions, so a site's own loop can apply the same rule.

## Feeds: adaptive, tiered crawls over a directory

For a directory of feeds (hundreds of thousands of rows), `daemon.feeds()` keeps the schedule in **your** database and the policy here. Nothing per feed is stored in Redis; only in-flight jobs are.

```js
import { createDaemon, tierSql } from '@profullstack/scraper-daemon';

daemon.feeds({
  name: 'crawl',
  concurrency: 32,
  // One indexed read per tier: `${tierSql('interval_minutes')} = $tier and next_at <= now() order by next_at limit $n`
  due: (tier, limit) => db.dueFeeds(tier, limit),
  run: async (feed, ctx) => {
    const doc = await ctx.fetch.text(feed.url);
    const items = parse(doc.text);
    await db.storeItems(feed.id, items);
    return { dates: items.map((i) => i.published), changed: items.length > 0 };
  },
  save: (feed, plan) => db.schedule(feed.id, { nextAt: plan.nextAt, minutes: plan.minutes, archived: plan.archived }),
  // f.flag is 'slow' | 'broken' (f.nextAt is then 30 days out), 'retired', or null (the ladder).
  fail: (feed, f) => db.failed(feed.id, { nextAt: f.nextAt, fails: f.fails, error: f.error, held: f.held, flag: f.flag }),
});
```

- **Cadence** (`@profullstack/scraper-daemon/cadence`, pure): from a feed's publish dates, `planNext()` picks the next read between **15 minutes** (a feed posting every hour) and **one week**. Between posts a feed is read four times per rhythm but never sleeps past the point its next post is due; inside that window it is read 24 times per rhythm (at least every 6 h), so a weekly show is checked every few hours around its release day instead of every 3.5 days. A feed silent for 3 rhythms backs off on its silence. **Silent 180 days and it is archived**: kept, out of the queue, looked at once every 90 days in case it came back. Undated feeds use `planUndated()` (halve on change, double on none, within a day). Every threshold is an option.
- **Tiers**: `hot` (≤ 1 h), `active` (≤ 1 d), `slow` (< 7 d), `dormant` (7 d, gone quiet), `archived`. The tier is derived from the interval alone (`tierFor`, and `tierSql()` for the same thing in SQL), so a site needs no new column, only an expression index.
- **Dispatch**: each tick splits the free slots by tier (`shares`, default 40/30/18/10/2), asks `due()` per tier, hands unused slots down and then back up. Under load the quiet tiers lag, never the live ones; with room to spare everything due is read. One feed per host at a time (`perHost`), a 429 holds the whole host and does not count against the feed, slow or broken feeds are flagged for 30 days, other failures climb the error ladder from the feed's own `fails`.
- **BullMQ**: each feed is a job carrying its row, priority = tier, so a backed-up queue still drains hot feeds first. `daemon.runNow('crawl', row)` reads one feed at once (a refresh button).

## Adopting it on a site that already has a loop

Wrap, do not rewrite: a mature crawler (its own due-row selection, cadence and per-host spreading) becomes one `job` whose `run` calls the existing function, and gains the shared status, stall alerts and run log. New work (contact enrichment, discovery sources) goes in as `queue`s and `job`s beside it.

## Runs on

search.peptides, ch4nnels.com, r4ck.dev, rssamplifier.com, p0dcasters.com.

MIT
