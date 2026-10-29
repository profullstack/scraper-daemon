import { ms } from './duration.js';
import { createFetcher, FetchError } from './fetch.js';
import { hostGate } from './gate.js';
import { memoryStore } from './store.js';
import { DEFAULT_SHARES, planNext, planUndated, quotas, tierFor, TIERS } from './cadence.js';

/**
 * One long-running process per site that keeps its listings fresh. Three kinds of work:
 *
 *   daemon.job({ name, every, run(ctx) })
 *     Periodic work: sync a register, run a discovery source, purge old rows. One
 *     run at a time per job; a failure retries on the error ladder, not on `every`.
 *
 *   daemon.queue({ name, every, items(ctx), run(item, ctx), host(item) })
 *     A refresh per item (a vendor's contact page, a feed, a storefront), oldest
 *     due first. `items` is re-read every `itemsEvery`, so whatever a discovery job
 *     adds is picked up without a restart. Items of one host run one after another;
 *     hosts run side by side. `run` may return { nextIn } to set its own cadence,
 *     or { retire: true } to stop polling an item; after `retireAfter` failures in a
 *     row an item is retired on its own.
 *
 *   daemon.pipeline({ name, every, gate(ctx), steps: [{ name, run(ctx) }] })
 *     A bulk import: `gate` says whether there is anything new (a HEAD stamp), the
 *     steps run in order, and a run that dies part way resumes from the step that
 *     failed instead of starting over.
 *
 *   daemon.feeds({ name, due(tier, limit, ctx), run(feed, ctx), save(feed, plan, result, ctx), fail(feed, failure, ctx) })
 *     A crawl over hundreds of thousands of feeds whose schedule lives in the site's
 *     own database, not here. Every tick asks `due` for each tier in turn (hot, active,
 *     slow, dormant, archived) with a share of the free slots, so live feeds are read
 *     first and archived ones only trickle; `run` returns the publish dates it saw and
 *     the cadence module turns them into the next read, which `save` writes back.
 *
 * A 429 (or a FetchError carrying retryAfterMs) holds the whole host back, not just
 * the item, and does not count as the item failing. status() is what /health and
 * /api/v1/daemon serve; onRun and onStall are the hooks for a site's own run log and
 * alerting.
 */
const DEFAULT_LADDER = ['5m', '15m', '1h', '3h', '6h', '12h', '1d'];

const TICK = '__tick';

/** A BullMQ job id for a task's key. BullMQ rejects ':' in custom ids, so every part is encoded. */
export const jobIdFor = (task, key) => `${encodeURIComponent(task)}--${encodeURIComponent(key)}`.replace(/:/g, '%3A');

/** Settle with `fallback` if `p` has not settled in `ms`: ioredis can wait forever on a dead socket. */
export function withTimeout(p, msLimit, fallback) {
  let t;
  return Promise.race([Promise.resolve(p).finally(() => clearTimeout(t)), new Promise((resolve) => (t = setTimeout(() => resolve(fallback), msLimit)))]);
}

export function createDaemon({
  name = 'scraper-daemon',
  // BullMQ: a Redis URL or a ready ioredis client. Production passes one; without it the local
  // engine runs everything in this process (tests, dev).
  redis = null,
  queueName,
  onRedisDown,
  // How often the watchdog pings Redis; three misses in a row and the process exits (or onRedisDown runs).
  redisCheckMs = 30_000,
  store: storeOption,
  concurrency = 4,
  tickMs = 5_000,
  jitter = 0.1,
  fetch: fetchOptions = {},
  fetcher,
  log = defaultLog(name),
  onRun,
  onStall,
  now = Date.now,
} = {}) {
  const gate = fetchOptions.gate ?? hostGate({ concurrency: fetchOptions.perHost ?? 1, delayMs: fetchOptions.delayMs ?? 1000, now });
  const web = fetcher ?? createFetcher({ ...fetchOptions, gate, now });
  const tasks = [];
  const heldHosts = new Map();
  // With Redis, run records and task state live beside the queue unless the site passes its own store.
  let ownRedis = null;
  let store = storeOption ?? (redis ? null : memoryStore());
  let engine = null;
  let ready = null;
  let timer = null;
  let stopping = null;
  let started = null;
  let active = 0;
  let controller = new AbortController();
  const history = [];

  const spread = (n) => Math.round(n * (1 + (Math.random() * 2 - 1) * jitter));
  const ladderAt = (ladder, fails) => ms(ladder[Math.min(fails - 1, ladder.length - 1)]);

  function define(kind, spec) {
    if (!spec?.name || tasks.some((t) => t.name === spec.name)) throw new TypeError(`${kind} needs a unique name`);
    const t = {
      kind,
      name: spec.name,
      every: ms(spec.every ?? '1h'),
      timeoutMs: ms(spec.timeout ?? '10m'),
      ladder: spec.backoff ?? DEFAULT_LADDER,
      retireAfter: spec.retireAfter ?? null,
      staleAfter: spec.staleAfter ? ms(spec.staleAfter) : null,
      concurrency: spec.concurrency ?? (kind === 'queue' ? 2 : kind === 'feeds' ? 16 : 1),
      perHost: spec.perHost ?? 1,
      inflight: new Map(),
      byTier: Object.fromEntries(TIERS.map((x) => [x, { ok: 0, failed: 0 }])),
      itemsEvery: ms(spec.itemsEvery ?? '10m'),
      spec,
      runs: null,
      items: null,
      itemsAt: 0,
      running: new Set(),
      ok: 0,
      failed: 0,
      lastOk: null,
      lastError: null,
      stalled: false,
    };
    tasks.push(t);
    return api;
  }

  async function runs(t) {
    if (!t.runs) t.runs = await (await storeReady()).load(t.name);
    return t.runs;
  }

  /** The store, once it exists: with `redis` and no store given, a Redis store on the engine's connection. */
  async function storeReady() {
    if (store) return store;
    const client = await connection();
    const { redisStore } = await import('./redis-store.js');
    store = redisStore(client, { prefix: `sd:${queueName ?? name}` });
    return store;
  }

  let conn = null;
  let connecting = null;
  /** The one connection. Memoised as a promise: two callers racing the import must not each make a client. */
  function connection() {
    if (conn) return Promise.resolve(conn);
    if (typeof redis !== 'string') return Promise.resolve((conn = redis));
    connecting ??= import('ioredis').then(({ default: IORedis }) => {
      // BullMQ requires maxRetriesPerRequest: null; the watchdog covers the hang that causes.
      conn = new IORedis(redis, { maxRetriesPerRequest: null, enableReadyCheck: true, connectionName: `${name}-scraper-daemon` });
      ownRedis = conn;
      connecting = null;
      return conn;
    });
    return connecting;
  }

  async function startBullmq() {
    const { Queue, Worker } = await import('bullmq');
    const client = await connection();

    // Armed before anything waits on Redis: a Redis that is down at boot makes the first
    // awaited command hang (maxRetriesPerRequest: null), and this is what notices.
    let misses = 0;
    const watchdog = setInterval(async () => {
      const pong = await withTimeout(client.ping().catch(() => null), Math.min(5_000, redisCheckMs), null);
      if (pong === 'PONG') {
        misses = 0;
        return;
      }
      misses++;
      log(`redis did not answer (${misses} in a row)`);
      if (misses >= 3) {
        clearInterval(watchdog);
        if (onRedisDown) onRedisDown();
        else {
          log('redis unreachable for 3 checks: exiting so the supervisor restarts with a fresh connection');
          process.exit(1);
        }
      }
    }, redisCheckMs);
    watchdog.unref?.();
    client.on?.('error', (err) => {
      if (misses === 0) log(`redis: ${err?.message ?? err}`);
    });

    // No Queue or Worker until Redis answers: a Worker made against a dead Redis starts
    // timers only a completed close() clears, and close() cannot complete without Redis.
    while ((await withTimeout(client.ping().catch(() => null), 5_000, null)) !== 'PONG') {
      if (controller.signal.aborted) {
        clearInterval(watchdog);
        if (ownRedis) ownRedis.disconnect();
        conn = null;
        ownRedis = null;
        return;
      }
      await new Promise((r) => setTimeout(r, Math.min(1_000, redisCheckMs)));
    }

    await storeReady();
    const qname = (queueName ?? name).replace(/[^A-Za-z0-9._-]/g, '-');
    // Events streams carry every job's return value; ours return nothing, and the cap keeps it that way.
    const streams = { events: { maxLen: 100 } };
    // Job ids are fixed per task and key, and BullMQ silently ignores an add whose id it still holds,
    // so a kept failed job would block that task forever: drop failures too (runs() keeps the error).
    const queue = new Queue(qname, { connection: client, streams, defaultJobOptions: { removeOnComplete: true, removeOnFail: true, attempts: 1 } });
    // The Worker blocks on its own connection; BullMQ does not close a connection it was handed, so close() does.
    const workerConn = client.duplicate();
    const worker = new Worker(qname, (job) => processJob(job), { connection: workerConn, concurrency: concurrency + 1, streams, lockDuration: 60_000 });
    worker.on('error', (err) => log(`worker error: ${err?.message ?? err}`));

    engine = {
      async enqueue(t, key, extra = {}) {
        const jobId = jobIdFor(t.name, key);
        // A finished job left under this id (one kept by 0.2.1, say) would swallow the add.
        const old = await queue.getJob(jobId);
        if (old && ['failed', 'completed'].includes(await old.getState())) await old.remove().catch(() => {});
        // A feed travels with its job (the site's row, not a key to look up again); its tier
        // is the BullMQ priority, so a backed-up queue still drains hot feeds first.
        const priority = extra.tier ? TIERS.indexOf(extra.tier) + 1 : undefined;
        return queue.add(t.name, { key, ...extra }, { jobId, ...(priority ? { priority } : {}) });
      },
      counts: () => queue.getJobCounts('active', 'waiting', 'delayed', 'failed'),
      async close() {
        clearInterval(watchdog);
        await worker.close();
        await queue.close();
        await workerConn.quit().catch(() => {});
        if (ownRedis) await ownRedis.quit().catch(() => {});
        conn = null;
        ownRedis = null;
      },
      /** After a graceful close that could not finish (Redis down): drop the sockets, stop reconnecting. */
      kill() {
        clearInterval(watchdog);
        // BullMQ's own clients (the Worker's blocking connection is a duplicate it made) first.
        Promise.resolve(worker.disconnect?.()).catch(() => {});
        Promise.resolve(queue.disconnect?.()).catch(() => {});
        workerConn.disconnect();
        ownRedis?.disconnect();
        conn = null;
        ownRedis = null;
      },
    };
    // One tick at a time across every process: BullMQ fires it, the Worker runs it.
    await queue.upsertJobScheduler(TICK, { every: tickMs }, { name: TICK, data: {}, opts: { removeOnComplete: true, removeOnFail: true } });
    log(`started (bullmq queue ${qname}): ${tasks.map((t) => `${t.kind} ${t.name}`).join(', ')}`);
  }

  async function record(t, rec) {
    t.runs.set(rec.key, rec);
    await (await storeReady()).save(t.name, rec);
  }

  const ctxFor = (t, key, signal) => ({
    daemon: name,
    job: t.name,
    key,
    signal,
    log: (...a) => log(`[${t.name}${key && key !== '*' ? ` ${key}` : ''}]`, ...a),
    fetch: web,
    holdHost: (host, forMs) => holdHost(host, forMs),
    state: {
      get: async (k) => (await storeReady()).get(`${t.name}:${k}`),
      set: async (k, v) => (await storeReady()).set(`${t.name}:${k}`, v),
    },
  });

  function holdHost(host, forMs) {
    const until = now() + Math.min(Math.max(ms(forMs), 60_000), 86_400_000);
    heldHosts.set(String(host).toLowerCase(), Math.max(heldHosts.get(String(host).toLowerCase()) ?? 0, until));
    log(`holding ${host} back until ${new Date(until).toISOString()}`);
  }
  const held = (host) => host && (heldHosts.get(String(host).toLowerCase()) ?? 0) > now();

  async function attempt(t, key, fn, host) {
    const rec = { ...((await runs(t)).get(key) ?? { key, lastTry: null, lastOk: null, nextAt: null, fails: 0, lastError: null }) };
    const started = now();
    rec.lastTry = started;
    const ac = new AbortController();
    const stopListener = () => ac.abort(new Error('daemon stopping'));
    controller.signal.addEventListener('abort', stopListener, { once: true });
    const timeout = setTimeout(() => ac.abort(new Error(`timed out after ${t.timeoutMs} ms`)), t.timeoutMs);
    let outcome;
    try {
      const result = (await fn(ctxFor(t, key, ac.signal))) ?? {};
      rec.lastOk = now();
      rec.fails = 0;
      rec.lastError = null;
      if (result.retire) rec.nextAt = Number.MAX_SAFE_INTEGER;
      else rec.nextAt = now() + spread(result.nextIn !== undefined ? ms(result.nextIn) : t.every);
      t.ok++;
      t.lastOk = rec.lastOk;
      t.stalled = false;
      outcome = { ok: true, result };
    } catch (err) {
      const retryAfter = err?.retryAfterMs ?? (err instanceof FetchError && err.status === 429 ? 30 * 60_000 : null);
      if (retryAfter !== null && host) {
        // The host asked us to slow down: hold all of it, and do not count it against the item.
        holdHost(host, retryAfter);
        rec.nextAt = now() + ms(retryAfter);
        outcome = { ok: false, held: true, error: err };
      } else {
        rec.fails++;
        rec.lastError = String(err?.message ?? err).slice(0, 500);
        rec.nextAt = t.retireAfter && rec.fails >= t.retireAfter ? Number.MAX_SAFE_INTEGER : now() + ladderAt(t.ladder, rec.fails);
        t.failed++;
        t.lastError = `${key === '*' ? '' : `${key}: `}${rec.lastError}`;
        outcome = { ok: false, error: err, retired: rec.nextAt === Number.MAX_SAFE_INTEGER };
        log(`[${t.name}${key === '*' ? '' : ` ${key}`}] failed (${rec.fails} in a row): ${rec.lastError}`);
      }
    } finally {
      clearTimeout(timeout);
      controller.signal.removeEventListener('abort', stopListener);
    }
    await record(t, rec);
    const entry = { job: t.name, kind: t.kind, key, ok: outcome.ok, held: !!outcome.held, retired: !!outcome.retired, ms: now() - started, at: started, error: outcome.error ? rec.lastError : null };
    history.push(entry);
    if (history.length > 200) history.shift();
    if (onRun) await Promise.resolve(onRun(entry, outcome.result)).catch((e) => log(`onRun hook failed: ${e.message}`));
    return outcome;
  }

  /* ---------- the three kinds ---------- */

  function dueJob(t) {
    if (t.running.size) return [];
    const rec = t.runs.get('*');
    return !rec || (rec.nextAt ?? 0) <= now() ? [{ key: '*' }] : [];
  }

  async function runJob(t) {
    return attempt(t, '*', (ctx) => t.spec.run(ctx));
  }

  async function itemsOf(t) {
    if (!t.items || now() - t.itemsAt >= t.itemsEvery) {
      const list = await t.spec.items(ctxFor(t, null, controller.signal));
      t.items = new Map();
      for (const it of list ?? []) {
        const item = typeof it === 'string' ? { key: it } : it;
        if (item?.key !== undefined && item.key !== null) t.items.set(String(item.key), item);
      }
      t.itemsAt = now();
    }
    return t.items;
  }

  function hostOf(t, item) {
    if (t.spec.host) return t.spec.host(item) ?? null;
    if (item.url) {
      try {
        return new URL(item.url).host;
      } catch {}
    }
    return null;
  }

  async function dueQueue(t, room) {
    const items = await itemsOf(t);
    const busyHosts = new Set([...t.running].map((k) => hostOf(t, items.get(k) ?? { key: k })).filter(Boolean));
    const candidates = [];
    for (const [key, item] of items) {
      if (t.running.has(key)) continue;
      const rec = t.runs.get(key);
      const at = rec?.nextAt ?? 0;
      if (at > now()) continue;
      candidates.push({ key, item, at, prio: item.priority ?? 0 });
    }
    // Never-run and higher priority first, then oldest due.
    candidates.sort((a, b) => b.prio - a.prio || a.at - b.at);
    const out = [];
    for (const c of candidates) {
      if (out.length >= room) break;
      const host = hostOf(t, c.item);
      if (held(host)) continue;
      // One item per host at a time: hosts in parallel, a host's items in series.
      if (host && busyHosts.has(host)) continue;
      if (host) busyHosts.add(host);
      out.push(c);
    }
    return out;
  }

  async function runQueueItem(t, c) {
    return attempt(t, c.key, (ctx) => t.spec.run(c.item, ctx), hostOf(t, c.item));
  }

  async function runPipeline(t) {
    return attempt(t, '*', async (ctx) => {
      const progress = (await ctx.state.get('progress')) ?? null;
      let from = 0;
      let token = progress?.token ?? null;
      if (progress && progress.next < t.spec.steps.length) {
        from = progress.next;
        ctx.log(`resuming at step ${t.spec.steps[from].name}`);
      } else {
        const open = t.spec.gate ? await t.spec.gate(ctx) : { token: String(now()) };
        if (!open) return { skipped: true };
        token = open.token ?? String(now());
        if (token === (await ctx.state.get('done'))) return { skipped: true, token };
      }
      const results = {};
      for (let i = from; i < t.spec.steps.length; i++) {
        const step = t.spec.steps[i];
        await ctx.state.set('progress', { token, next: i, step: step.name, at: now() });
        results[step.name] = await step.run({ ...ctx, token, step: step.name, results });
      }
      await ctx.state.set('progress', { token, next: t.spec.steps.length, at: now() });
      await ctx.state.set('done', token);
      return { token, results };
    });
  }

  /* ---------- feeds: the schedule is the site's, the policy is cadence.js ---------- */

  const feedKey = (t, feed) => String(t.spec.key ? t.spec.key(feed) : (feed.key ?? feed.id));

  async function dueFeeds(t, room) {
    const ctx = ctxFor(t, null, controller.signal);
    const perHost = new Map();
    for (const { host } of t.inflight.values()) if (host) perHost.set(host, (perHost.get(host) ?? 0) + 1);
    const picked = [];
    const seen = new Set();
    const exhausted = new Set();
    const take = async (tier, want) => {
      // Over-read: rows already in flight, on a busy host or a held one are skipped here.
      const rows = (await t.spec.due(tier, want * 3 + t.running.size + picked.length, ctx)) ?? [];
      let got = 0;
      for (const feed of rows) {
        if (got >= want) break;
        const key = feedKey(t, feed);
        if (t.running.has(key) || seen.has(key)) continue;
        const host = hostOf(t, feed);
        if (held(host)) continue;
        if (host && (perHost.get(host) ?? 0) >= t.perHost) continue;
        if (host) perHost.set(host, (perHost.get(host) ?? 0) + 1);
        seen.add(key);
        picked.push({ key, feed, host, tier });
        got++;
      }
      if (got < want) exhausted.add(tier);
      return got;
    };
    let carry = 0;
    for (const [tier, quota] of quotas(room, t.spec.shares ?? DEFAULT_SHARES)) {
      const want = quota + carry;
      // Slots a tier had nothing due for go to the next one down.
      carry = want > 0 ? want - (await take(tier, want)) : 0;
    }
    // Still room once every tier had its share: back up from the hottest tier that has more.
    for (const tier of TIERS) {
      if (carry <= 0) break;
      if (!exhausted.has(tier)) carry -= await take(tier, carry);
    }
    return picked;
  }

  /** The next read, from what the crawl saw: its own nextIn, the dates, or the undated ladder. */
  function decide(t, feed, result) {
    const opts = { ...(t.spec.cadence ?? {}), now: now() };
    let plan;
    if (result.nextIn !== undefined) {
      const minutes = Math.max(1, Math.round(ms(result.nextIn) / 60_000));
      plan = { minutes, tier: tierFor(minutes, opts), archived: tierFor(minutes, opts) === 'archived', rhythm: null, silence: null, reason: 'nextIn' };
    } else {
      plan = planNext(result.dates ?? [], opts) ?? planUndated({ changed: Boolean(result.changed), previous: t.spec.previous ? t.spec.previous(feed) : feed.minutes }, opts);
    }
    return { ...plan, nextAt: now() + spread(plan.minutes * 60_000) };
  }

  async function runFeed(t, c) {
    const started = now();
    const ac = new AbortController();
    const stopListener = () => ac.abort(new Error('daemon stopping'));
    controller.signal.addEventListener('abort', stopListener, { once: true });
    const timeout = setTimeout(() => ac.abort(new Error(`timed out after ${t.timeoutMs} ms`)), t.timeoutMs);
    const ctx = ctxFor(t, c.key, ac.signal);
    const tierStats = t.byTier[c.tier] ?? (t.byTier[c.tier] = { ok: 0, failed: 0 });
    let outcome;
    try {
      const result = (await t.spec.run(c.feed, ctx)) ?? {};
      const plan = decide(t, c.feed, result);
      await t.spec.save(c.feed, plan, result, ctx);
      t.ok++;
      tierStats.ok++;
      t.lastOk = now();
      t.stalled = false;
      outcome = { ok: true, result, plan };
    } catch (err) {
      const message = String(err?.message ?? err).slice(0, 500);
      const retryAfter = err?.retryAfterMs ?? (err instanceof FetchError && err.status === 429 ? 30 * 60_000 : null);
      let failure;
      if (retryAfter !== null && c.host) {
        // The host asked us to slow down: hold all of it, and do not count it against the feed.
        holdHost(c.host, retryAfter);
        failure = { error: message, held: true, fails: null, minutes: Math.ceil(ms(retryAfter) / 60_000), retire: false };
      } else {
        const fails = Number(t.spec.fails ? t.spec.fails(c.feed) : (c.feed.fails ?? 0)) + 1;
        failure = { error: message, held: false, fails, minutes: Math.ceil(ladderAt(t.ladder, fails) / 60_000), retire: Boolean(t.retireAfter && fails >= t.retireAfter) };
        t.failed++;
        tierStats.failed++;
        t.lastError = `${c.key}: ${message}`;
      }
      failure.nextAt = now() + failure.minutes * 60_000;
      try {
        await t.spec.fail?.(c.feed, failure, ctx);
      } catch (e) {
        log(`[${t.name} ${c.key}] could not record the failure: ${e?.message ?? e}`);
      }
      outcome = { ok: false, held: failure.held, retired: failure.retire, error: err };
    } finally {
      clearTimeout(timeout);
      controller.signal.removeEventListener('abort', stopListener);
    }
    const entry = { job: t.name, kind: t.kind, key: c.key, tier: c.tier, ok: outcome.ok, held: !!outcome.held, retired: !!outcome.retired, ms: now() - started, at: started, error: outcome.error ? String(outcome.error?.message ?? outcome.error).slice(0, 500) : null };
    history.push(entry);
    if (history.length > 200) history.shift();
    if (onRun) await Promise.resolve(onRun(entry, outcome.result)).catch((e) => log(`onRun hook failed: ${e.message}`));
    return outcome;
  }

  function launchFeed(t, c) {
    t.inflight.set(c.key, { host: c.host, tier: c.tier });
    const settle = () => t.inflight.delete(c.key);
    active++;
    t.running.add(c.key);
    if (engine) {
      engine.enqueue(t, c.key, { feed: c.feed, tier: c.tier }).catch((err) => {
        if (t.running.delete(c.key)) active--;
        settle();
        log(`[${t.name}] could not enqueue ${c.key}: ${err?.message ?? err}`);
      });
      return;
    }
    runFeed(t, c)
      .finally(() => {
        if (t.running.delete(c.key)) active--;
        settle();
      })
      .catch(() => {});
  }

  /* ---------- the loop ---------- */

  /**
   * Start one unit of work. The local engine runs it here; the BullMQ engine enqueues a
   * job the Worker runs (through the same attempt()), so it survives a restart and the
   * queue is visible in Redis. Either way the work is counted until it settles.
   */
  function launch(t, key, run) {
    active++;
    t.running.add(key);
    const settle = () => {
      if (t.running.delete(key)) active--;
    };
    if (engine) {
      engine.enqueue(t, key).catch((err) => {
        settle();
        log(`[${t.name}] could not enqueue ${key}: ${err?.message ?? err}`);
      });
      return;
    }
    run().finally(settle).catch(() => {});
  }

  /** The BullMQ Worker's processor: the tick, or one unit of a task. Returns nothing, so Redis stores nothing. */
  async function processJob(job) {
    if (job.name === TICK) {
      await tick();
      return;
    }
    const t = tasks.find((x) => x.name === job.name);
    if (!t) return;
    const key = String(job.data?.key ?? '*');
    // A job enqueued before a restart is not counted yet; count it while it runs.
    if (!t.running.has(key)) {
      t.running.add(key);
      active++;
    }
    try {
      if (t.kind === 'feeds') {
        const feed = job.data?.feed ?? { key };
        await runFeed(t, { key, feed, host: hostOf(t, feed), tier: job.data?.tier ?? 'slow' });
        return;
      }
      await runs(t);
      if (t.kind === 'queue') {
        const items = await itemsOf(t);
        await runQueueItem(t, { key, item: items.get(key) ?? { key } });
      } else await (t.kind === 'pipeline' ? runPipeline(t) : runJob(t));
    } finally {
      if (t.running.delete(key)) active--;
      t.inflight.delete(key);
    }
  }

  let ticking = false;
  async function tick() {
    // One tick at a time: two overlapping ticks would both see a host as free.
    if (stopping || ticking) return;
    ticking = true;
    try {
      await scheduleAll();
    } finally {
      ticking = false;
    }
  }

  function checkStall(t) {
    if (!t.staleAfter || t.stalled || started === null) return;
    const since = t.lastOk ?? [...(t.runs?.values() ?? [])].reduce((m, r) => Math.max(m, r.lastOk ?? 0), 0);
    if (now() - Math.max(since, started) <= t.staleAfter) return;
    t.stalled = true;
    log(`[${t.name}] stalled: no success in ${Math.round(t.staleAfter / 60_000)} min`);
    if (onStall) Promise.resolve(onStall({ job: t.name, lastOk: since || null, lastError: t.lastError })).catch((e) => log(`onStall hook failed: ${e.message}`));
  }

  async function scheduleAll() {
    for (const t of tasks) {
      checkStall(t);
      if (active >= concurrency) continue;
      try {
        if (t.kind === 'feeds') {
          const room = Math.min(concurrency - active, t.concurrency - t.running.size);
          if (room > 0) for (const c of await dueFeeds(t, room)) launchFeed(t, c);
          continue;
        }
        await runs(t);
        if (t.kind === 'queue') {
          const room = Math.min(concurrency - active, t.concurrency - t.running.size);
          if (room <= 0) continue;
          for (const c of await dueQueue(t, room)) launch(t, c.key, () => runQueueItem(t, c));
        } else if (dueJob(t).length) {
          launch(t, '*', () => (t.kind === 'pipeline' ? runPipeline(t) : runJob(t)));
        }
      } catch (err) {
        log(`[${t.name}] could not schedule: ${err?.message ?? err}`);
      }
    }
  }

  const api = {
    job: (spec) => define('job', spec),
    queue: (spec) => {
      if (typeof spec?.items !== 'function' || typeof spec?.run !== 'function') throw new TypeError('queue needs items() and run()');
      return define('queue', spec);
    },
    feeds: (spec) => {
      for (const f of ['due', 'run', 'save']) if (typeof spec?.[f] !== 'function') throw new TypeError(`feeds needs ${f}()`);
      return define('feeds', { every: '1h', timeout: '2m', ...spec });
    },
    pipeline: (spec) => {
      if (!Array.isArray(spec?.steps) || !spec.steps.length) throw new TypeError('pipeline needs steps');
      return define('pipeline', spec);
    },

    /** Start the loop. Safe to call once; returns the daemon (the BullMQ engine connects in the background). */
    start() {
      if (timer || ready) return api;
      started = now();
      stopping = null;
      controller = new AbortController();
      if (redis) {
        ready = startBullmq().catch((err) => {
          log(`could not start the BullMQ engine: ${err?.message ?? err}`);
          throw err;
        });
        ready.catch(() => {});
        return api;
      }
      log(`started (local engine): ${tasks.map((t) => `${t.kind} ${t.name}`).join(', ')}`);
      const loop = () => tick().catch((err) => log(`tick failed: ${err?.message ?? err}`));
      // A daemon is what keeps its process alive (a poller has no server to do it): never unref this timer.
      timer = setInterval(loop, tickMs);
      loop();
      return api;
    },

    /** Resolves once the engine is up (at once for the local engine). */
    ready: () => ready ?? Promise.resolve(),

    /** Stop scheduling, abort what is running, and wait (up to graceMs) for it to settle. */
    async stop({ graceMs = 10_000 } = {}) {
      clearInterval(timer);
      timer = null;
      stopping ??= (async () => {
        controller.abort();
        if (engine) {
          const closed = await withTimeout(engine.close().then(() => true), graceMs, false).catch(() => false);
          if (!closed) engine.kill();
          engine = null;
        } else if (ownRedis) {
          // Still waiting for Redis to answer: drop the connection now so ioredis stops reconnecting.
          ownRedis.disconnect();
          conn = null;
          ownRedis = null;
        }
        const until = now() + graceMs;
        while (active > 0 && now() < until) await new Promise((r) => setTimeout(r, 50));
      })();
      return stopping;
    },

    /** Run one task (or one queue item, or one feed: pass the site's row) now, whatever its schedule. */
    async runNow(taskName, key) {
      const t = tasks.find((x) => x.name === taskName);
      if (!t) throw new Error(`no task ${taskName}`);
      if (t.kind === 'feeds') {
        if (!key || typeof key !== 'object') throw new TypeError('runNow on feeds takes the feed row');
        const c = { key: feedKey(t, key), feed: key, host: hostOf(t, key), tier: key.tier ?? 'hot' };
        if (t.running.has(c.key)) return { ok: false, busy: true };
        return runFeed(t, c);
      }
      await runs(t);
      if (t.kind === 'queue') {
        const items = await itemsOf(t);
        const keys = key !== undefined ? [String(key)] : [...items.keys()];
        const out = [];
        for (const k of keys) {
          const item = items.get(k) ?? { key: k };
          out.push(await runQueueItem(t, { key: k, item }));
        }
        return out;
      }
      return t.kind === 'pipeline' ? runPipeline(t) : runJob(t);
    },

    /** Re-read a queue's items on the next tick (after discovery added some). */
    refreshItems(taskName) {
      const t = tasks.find((x) => x.name === taskName);
      if (t) t.itemsAt = 0;
    },

    /** Everything a status page needs, as plain JSON. */
    async status() {
      // A status page must answer even when the store cannot: ioredis waits forever on a
      // refused connection, so every store read here is bounded, and what is cached is shown.
      let storeAnswered = true;
      const bounded = async (p, fallback) => {
        const miss = Symbol('miss');
        const v = await withTimeout(p, 2000, miss).catch(() => miss);
        if (v === miss) {
          storeAnswered = false;
          return fallback;
        }
        return v;
      };
      const out = [];
      for (const t of tasks) {
        const r = await bounded(runs(t), t.runs ?? new Map());
        const recs = [...r.values()];
        const base = { name: t.name, kind: t.kind, every: t.every, running: t.running.size, ok: t.ok, failed: t.failed, lastOk: t.lastOk ?? recs.reduce((m, x) => Math.max(m, x.lastOk ?? 0), 0) ?? null, lastError: t.lastError, stalled: t.stalled };
        if (t.kind === 'feeds') {
          out.push({ ...base, inflight: t.running.size, concurrency: t.concurrency, byTier: t.byTier });
          continue;
        }
        if (t.kind === 'queue') {
          const items = t.items ?? new Map();
          let due = 0;
          let retired = 0;
          let failing = 0;
          let never = 0;
          for (const k of items.keys()) {
            const x = r.get(k);
            if (!x) never++;
            else if (x.nextAt === Number.MAX_SAFE_INTEGER) retired++;
            else if ((x.nextAt ?? 0) <= now()) due++;
            if (x?.fails) failing++;
          }
          out.push({ ...base, items: items.size, due: due + never, neverRun: never, failing, retired });
        } else {
          const x = r.get('*');
          out.push({ ...base, nextAt: x?.nextAt ?? null, fails: x?.fails ?? 0, progress: t.kind === 'pipeline' ? await bounded((async () => (await storeReady()).get(`${t.name}:progress`))(), null) : undefined });
        }
      }
      return {
        name,
        engine: redis ? 'bullmq' : 'local',
        queue: engine ? await withTimeout(engine.counts(), 3000, { error: 'redis did not answer in 3 s' }) : redis ? { error: 'connecting to redis' } : undefined,
        running: Boolean(timer) || Boolean(engine),
        startedAt: started,
        active,
        concurrency,
        store: store?.kind ?? (redis ? 'redis' : 'custom'),
        // false when the store did not answer within 2 s (Redis down): the numbers above are the last ones seen.
        storeAnswered,
        heldHosts: [...heldHosts].filter(([, until]) => until > now()).map(([host, until]) => ({ host, until })),
        tasks: out,
        recent: history.slice(-20).reverse(),
      };
    },

    holdHost,
    fetch: web,
    /** The run/state store (null until the Redis store exists; `await daemon.ready()` first). */
    get store() {
      return store;
    },
  };
  return api;
}

function defaultLog(name) {
  return (...a) => console.log(`[${name}]`, ...a);
}
