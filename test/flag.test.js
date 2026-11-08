import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { createDaemon, createFetcher, CRAWL_BROWSER_TIMEOUT_MS, CRAWL_FETCH_TIMEOUT_MS, failureClass, FetchError, FLAG_FOR_MS, flagPlan, RETIRE_RECHECK_MS } from '../src/index.js';

const quiet = () => {};
const DAY = 86_400_000;
const sysErr = (code) => Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new Error(code), { code }) });

describe('the fleet crawler rule: constants', () => {
  test('10 s fetch, 30 s browser, 30 days flagged, 30 days between rechecks of a retired item', () => {
    assert.equal(CRAWL_FETCH_TIMEOUT_MS, 10_000);
    assert.equal(CRAWL_BROWSER_TIMEOUT_MS, 30_000);
    assert.equal(FLAG_FOR_MS, 30 * DAY);
    assert.equal(RETIRE_RECHECK_MS, 30 * DAY);
  });
});

describe('failureClass', () => {
  test('timeouts are slow', () => {
    assert.equal(failureClass(new FetchError('timeout', 'a.example: The operation was aborted due to timeout')), 'slow');
    assert.equal(failureClass(Object.assign(new Error('x'), { name: 'TimeoutError' })), 'slow');
    assert.equal(failureClass(sysErr('UND_ERR_CONNECT_TIMEOUT')), 'slow');
    assert.equal(failureClass(sysErr('ETIMEDOUT')), 'slow');
    assert.equal(failureClass(new Error('timed out after 600000 ms')), 'slow');
  });
  test('DNS, TLS, refused/reset connections and 5xx are broken', () => {
    assert.equal(failureClass(new FetchError('network', 'no address for gone.example (ENOTFOUND)', { dnsCode: 'ENOTFOUND' })), 'broken');
    assert.equal(failureClass(sysErr('ECONNREFUSED')), 'broken');
    assert.equal(failureClass(sysErr('ECONNRESET')), 'broken');
    assert.equal(failureClass(sysErr('CERT_HAS_EXPIRED')), 'broken');
    assert.equal(failureClass(sysErr('ERR_TLS_CERT_ALTNAME_INVALID')), 'broken');
    assert.equal(failureClass(sysErr('UNABLE_TO_VERIFY_LEAF_SIGNATURE')), 'broken');
    assert.equal(failureClass(new FetchError('status', '502 from x', { status: 502 })), 'broken');
    assert.equal(failureClass(Object.assign(new Error('upstream'), { status: 503 })), 'broken');
  });
  test('4xx, robots, blocked, too-large, parse errors and our own bugs keep the ladder', () => {
    assert.equal(failureClass(new FetchError('status', '404 from x', { status: 404 })), null);
    assert.equal(failureClass(new FetchError('status', '403 from x', { status: 403 })), null);
    assert.equal(failureClass(new FetchError('robots', 'robots.txt disallows x')), null);
    assert.equal(failureClass(new FetchError('blocked', 'private address')), null);
    assert.equal(failureClass(new FetchError('too-large', 'over the cap')), null);
    assert.equal(failureClass(new SyntaxError('Unexpected token <')), null);
    assert.equal(failureClass(new TypeError("Cannot read properties of undefined (reading 'x')")), null);
    assert.equal(failureClass(null), null);
  });
  test('our own resolver failing (EAI_AGAIN) is not the site being broken', () => {
    assert.equal(failureClass(new FetchError('network', 'no address for a.example (EAI_AGAIN)', { dnsCode: 'EAI_AGAIN' })), null);
    assert.equal(failureClass(sysErr('EAI_AGAIN')), null);
  });
  test('a 429 or a 503 with Retry-After is held, not flagged; a thrown flag wins', () => {
    assert.equal(failureClass(new FetchError('status', '503', { status: 503, retryAfterMs: 60_000 })), null);
    assert.equal(failureClass(Object.assign(new Error('mine'), { flag: 'broken' })), 'broken');
    assert.equal(failureClass(Object.assign(new FetchError('timeout', 'x'), { flag: null })), null);
  });
});

describe('flagPlan', () => {
  test('slow or broken: 30 days, never the ladder', () => {
    assert.deepEqual(flagPlan(new FetchError('timeout', 'x'), { fails: 1, ladderMs: 300_000 }), { flag: 'slow', nextIn: 30 * DAY });
    assert.deepEqual(flagPlan(new FetchError('status', '500', { status: 500 }), { fails: 7, ladderMs: DAY }), { flag: 'broken', nextIn: 30 * DAY });
  });
  test('anything else: the ladder', () => {
    assert.deepEqual(flagPlan(new FetchError('status', '404', { status: 404 }), { fails: 1, ladderMs: 300_000 }), { flag: null, nextIn: 300_000 });
  });
  test('retireAfter is a 30-day recheck, not forever (unless retireRecheck is off)', () => {
    assert.deepEqual(flagPlan(new Error('x'), { fails: 3, retireAfter: 3, ladderMs: 1 }), { flag: 'retired', nextIn: 30 * DAY });
    assert.deepEqual(flagPlan(new Error('x'), { fails: 3, retireAfter: 3, retireRecheck: false, ladderMs: 1 }), { flag: 'retired', nextIn: Infinity });
  });
  test('flagFor: false restores the pre-0.4.0 ladder', () => {
    assert.deepEqual(flagPlan(new FetchError('timeout', 'x'), { fails: 1, flagFor: false, ladderMs: 300_000 }), { flag: null, nextIn: 300_000 });
  });
});

describe('createFetcher: one 10 s budget for the whole request', () => {
  const lookup = async () => [{ address: '93.184.216.34' }];
  test('the default is 10 s', async () => {
    let seen;
    const fetcher = createFetcher({ robots: false, lookup, fetchImpl: async (_u, init) => {
      seen = init.signal;
      return new Response('ok');
    } });
    await fetcher.text('https://a.example/');
    // The signal is a timeout signal; it must not have fired, and must fire on its own (no caller signal).
    assert.equal(seen.aborted, false);
    assert.equal(CRAWL_FETCH_TIMEOUT_MS, 10_000);
  });
  test('a body that trickles past the budget rejects as timeout (headers alone do not reset it)', async () => {
    const fetchImpl = async (_u, init) =>
      new Response(
        new ReadableStream({
          async pull(c) {
            await new Promise((r, j) => {
              const t = setTimeout(r, 30);
              init.signal.addEventListener('abort', () => (clearTimeout(t), j(init.signal.reason)), { once: true });
            });
            c.enqueue(new TextEncoder().encode('x'));
          },
        }),
      );
    const fetcher = createFetcher({ robots: false, lookup, fetchImpl, timeoutMs: 100 });
    const t0 = Date.now();
    await assert.rejects(fetcher.text('https://a.example/'), (e) => e instanceof FetchError && e.code === 'timeout' && failureClass(e) === 'slow');
    assert.ok(Date.now() - t0 < 1_000);
  });
  test('a connection error keeps its system code and classifies as broken; NXDOMAIN too', async () => {
    const refused = createFetcher({ robots: false, lookup, fetchImpl: async () => Promise.reject(sysErr('ECONNREFUSED')) });
    await assert.rejects(refused.text('https://a.example/'), (e) => e.code === 'network' && e.causeCode === 'ECONNREFUSED' && failureClass(e) === 'broken');
    const nx = createFetcher({ robots: false, lookup: async () => Promise.reject(Object.assign(new Error('nx'), { code: 'ENOTFOUND' })) });
    await assert.rejects(nx.text('https://gone.example/'), (e) => e.code === 'network' && e.dnsCode === 'ENOTFOUND' && failureClass(e) === 'broken');
    const again = createFetcher({ robots: false, lookup: async () => Promise.reject(Object.assign(new Error('again'), { code: 'EAI_AGAIN' })) });
    await assert.rejects(again.text('https://a.example/'), (e) => e.dnsCode === 'EAI_AGAIN' && failureClass(e) === null);
  });
});

describe('daemon: slow or broken targets are flagged for 30 days, then rechecked once', () => {
  test('queue: timeout flags slow for 30 days; the recheck succeeds and clears it; a 404 keeps the ladder', async () => {
    let clock = 1_000_000_000;
    let mode = 'timeout';
    const d = createDaemon({ log: quiet, jitter: 0, now: () => clock });
    d.queue({ name: 'pages', every: '1d', backoff: ['5m', '1h'], items: async () => [{ key: 'p', url: 'https://p.example/' }], run: async () => {
      if (mode === 'timeout') throw new FetchError('timeout', 'p.example: aborted due to timeout');
      if (mode === '502') throw new FetchError('status', '502 from p', { status: 502 });
      if (mode === '404') throw new FetchError('status', '404 from p', { status: 404 });
    } });
    await d.runNow('pages', 'p');
    let rec = (await d.store.load('pages')).get('p');
    assert.equal(rec.flag, 'slow');
    assert.equal(rec.flaggedAt, clock);
    assert.equal(rec.nextAt, clock + 30 * DAY);
    assert.match(rec.lastError, /timeout/);
    let s = (await d.status()).tasks[0];
    assert.equal(s.flagged, 1);
    assert.equal(s.due, 0);

    // 30 days on it is due again, exactly once; it fails broken this time: another 30 days.
    clock += 30 * DAY;
    assert.equal((await d.status()).tasks[0].due, 1);
    mode = '502';
    await d.runNow('pages', 'p');
    rec = (await d.store.load('pages')).get('p');
    assert.equal(rec.flag, 'broken');
    assert.equal(rec.fails, 2);
    assert.equal(rec.nextAt, clock + 30 * DAY);

    // The next recheck succeeds: flag cleared, back on its own cadence.
    clock += 30 * DAY;
    mode = 'ok';
    await d.runNow('pages', 'p');
    rec = (await d.store.load('pages')).get('p');
    assert.equal(rec.flag, null);
    assert.equal(rec.flaggedAt, null);
    assert.equal(rec.fails, 0);
    assert.equal(rec.nextAt, clock + DAY);

    // A 4xx is not slow or broken: the ladder, as before.
    mode = '404';
    await d.runNow('pages', 'p');
    rec = (await d.store.load('pages')).get('p');
    assert.equal(rec.flag, null);
    assert.equal(rec.nextAt, clock + 300_000);
  });

  test('the daemon’s own per-run timeout flags the item slow, whatever the run threw', async () => {
    const d = createDaemon({ log: quiet, jitter: 0 });
    d.queue({ name: 'render', timeout: 30, items: async () => ['x'], run: async (_item, ctx) => {
      await new Promise((_r, j) => ctx.signal.addEventListener('abort', () => j(new Error('aborted')), { once: true }));
    } });
    const [out] = await d.runNow('render', 'x');
    assert.equal(out.flagged, 'slow');
    const rec = (await d.store.load('render')).get('x');
    assert.equal(rec.flag, 'slow');
    assert.ok(rec.nextAt - Date.now() > 29 * DAY);
  });

  test('flagFor: false keeps the ladder (an uptime monitor, our own services)', async () => {
    let clock = 5_000_000;
    const d = createDaemon({ log: quiet, jitter: 0, now: () => clock });
    d.queue({ name: 'uptime', flagFor: false, backoff: ['1m'], items: async () => ['me'], run: async () => {
      throw new FetchError('timeout', 'slow');
    } });
    await d.runNow('uptime', 'me');
    const rec = (await d.store.load('uptime')).get('me');
    assert.equal(rec.flag, null);
    assert.equal(rec.nextAt, clock + 60_000);
  });

  test('jobs keep the ladder by default (a whole sync is not one target)', async () => {
    let clock = 5_000_000;
    const d = createDaemon({ log: quiet, jitter: 0, now: () => clock });
    d.job({ name: 'sync', backoff: ['5m'], run: async () => {
      throw new FetchError('timeout', 'slow');
    } });
    await d.runNow('sync');
    assert.equal((await d.status()).tasks[0].nextAt, clock + 300_000);
  });

  test('feeds: a broken feed gets flag and 30 days of minutes in fail(); retireAfter rechecks in 30 days', async () => {
    const failed = [];
    const d = createDaemon({ log: quiet, jitter: 0 });
    d.feeds({
      name: 'crawl',
      retireAfter: 5,
      due: async () => [],
      run: async (feed) => {
        if (feed.id === 'dns') throw new FetchError('network', 'no address for gone.example (ENOTFOUND)', { dnsCode: 'ENOTFOUND' });
        throw new Error('parse error');
      },
      save: async () => {},
      fail: async (feed, f) => void failed.push({ id: feed.id, f }),
    });
    await d.runNow('crawl', { id: 'dns', url: 'https://gone.example/feed', fails: 0 });
    await d.runNow('crawl', { id: 'parse', url: 'https://p.example/feed', fails: 0 });
    await d.runNow('crawl', { id: 'old', url: 'https://o.example/feed', fails: 4 });
    const by = Object.fromEntries(failed.map((x) => [x.id, x.f]));
    assert.equal(by.dns.flag, 'broken');
    assert.equal(by.dns.minutes, 30 * 24 * 60);
    assert.equal(by.parse.flag, null);
    assert.equal(by.parse.minutes, 5);
    assert.equal(by.old.retire, true);
    assert.equal(by.old.flag, 'retired');
    assert.equal(by.old.minutes, 30 * 24 * 60);
  });
});
