import { ms } from './duration.js';
import { createFetcher, FetchError } from './fetch.js';
import { hostGate } from './gate.js';
import { memoryStore } from './store.js';

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
 * A 429 (or a FetchError carrying retryAfterMs) holds the whole host back, not just
 * the item, and does not count as the item failing. status() is what /health and
 * /api/v1/daemon serve; onRun and onStall are the hooks for a site's own run log and
 * alerting.
 */
const DEFAULT_LADDER = ['5m', '15m', '1h', '3h', '6h', '12h', '1d'];

export function createDaemon({
  name = 'scraper-daemon',
  store = memoryStore(),
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
      concurrency: spec.concurrency ?? (kind === 'queue' ? 2 : 1),
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
    if (!t.runs) t.runs = await store.load(t.name);
    return t.runs;
  }

  async function record(t, rec) {
    t.runs.set(rec.key, rec);
    await store.save(t.name, rec);
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
      get: (k) => store.get(`${t.name}:${k}`),
      set: (k, v) => store.set(`${t.name}:${k}`, v),
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

  /* ---------- the loop ---------- */

  function launch(t, key, p) {
    active++;
    t.running.add(key);
    p.finally(() => {
      active--;
      t.running.delete(key);
    }).catch(() => {});
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
        await runs(t);
        if (t.kind === 'queue') {
          const room = Math.min(concurrency - active, t.concurrency - t.running.size);
          if (room <= 0) continue;
          for (const c of await dueQueue(t, room)) launch(t, c.key, runQueueItem(t, c));
        } else if (dueJob(t).length) {
          launch(t, '*', t.kind === 'pipeline' ? runPipeline(t) : runJob(t));
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
    pipeline: (spec) => {
      if (!Array.isArray(spec?.steps) || !spec.steps.length) throw new TypeError('pipeline needs steps');
      return define('pipeline', spec);
    },

    /** Start the loop. Safe to call once; returns the daemon. */
    start() {
      if (timer) return api;
      started = now();
      stopping = null;
      controller = new AbortController();
      log(`started: ${tasks.map((t) => `${t.kind} ${t.name}`).join(', ')}`);
      const loop = () => tick().catch((err) => log(`tick failed: ${err?.message ?? err}`));
      timer = setInterval(loop, tickMs);
      timer.unref?.();
      loop();
      return api;
    },

    /** Stop scheduling, abort what is running, and wait (up to graceMs) for it to settle. */
    async stop({ graceMs = 10_000 } = {}) {
      clearInterval(timer);
      timer = null;
      stopping ??= (async () => {
        controller.abort();
        const until = now() + graceMs;
        while (active > 0 && now() < until) await new Promise((r) => setTimeout(r, 50));
      })();
      return stopping;
    },

    /** Run one task (or one queue item) now, whatever its schedule. Resolves with the outcome. */
    async runNow(taskName, key) {
      const t = tasks.find((x) => x.name === taskName);
      if (!t) throw new Error(`no task ${taskName}`);
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
      const out = [];
      for (const t of tasks) {
        const r = await runs(t);
        const recs = [...r.values()];
        const base = { name: t.name, kind: t.kind, every: t.every, running: t.running.size, ok: t.ok, failed: t.failed, lastOk: t.lastOk ?? recs.reduce((m, x) => Math.max(m, x.lastOk ?? 0), 0) ?? null, lastError: t.lastError, stalled: t.stalled };
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
          out.push({ ...base, nextAt: x?.nextAt ?? null, fails: x?.fails ?? 0, progress: t.kind === 'pipeline' ? await store.get(`${t.name}:progress`) : undefined });
        }
      }
      return {
        name,
        running: Boolean(timer),
        startedAt: started,
        active,
        concurrency,
        store: store.kind ?? 'custom',
        heldHosts: [...heldHosts].filter(([, until]) => until > now()).map(([host, until]) => ({ host, until })),
        tasks: out,
        recent: history.slice(-20).reverse(),
      };
    },

    holdHost,
    fetch: web,
    store,
  };
  return api;
}

function defaultLog(name) {
  return (...a) => console.log(`[${name}]`, ...a);
}
