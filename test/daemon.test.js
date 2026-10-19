import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { createDaemon, createFetcher, FetchError, hostGate, isPublicAddress, memoryStore, ms, retryAfter, robotsAllows, sqliteStore } from '../src/index.js';

const quiet = () => {};

describe('ms', () => {
  test('reads durations', () => {
    assert.equal(ms('90s'), 90_000);
    assert.equal(ms('1h30m'), 5_400_000);
    assert.equal(ms('7d'), 604_800_000);
    assert.equal(ms(250), 250);
    assert.throws(() => ms('soon'));
  });
});

describe('robots', () => {
  const robots = 'User-agent: *\nDisallow: /*?add-to-cart=\nDisallow: /wp-admin/\nAllow: /wp-admin/admin-ajax.php\n\nUser-agent: badbot\nDisallow: /\n';
  test('a wildcard rule does not disallow the whole site', () => {
    assert.equal(robotsAllows(robots, '/'), true);
    assert.equal(robotsAllows(robots, '/shop?add-to-cart=1'), false);
  });
  test('longest rule wins, Allow on a tie', () => {
    assert.equal(robotsAllows(robots, '/wp-admin/x'), false);
    assert.equal(robotsAllows(robots, '/wp-admin/admin-ajax.php'), true);
  });
  test('a group naming the agent replaces *', () => {
    assert.equal(robotsAllows(robots, '/', 'BadBot/1.0'), false);
    assert.equal(robotsAllows('', '/anything'), true);
  });
});

describe('addresses', () => {
  test('private, loopback, link-local, CGNAT, mapped and NAT64 are refused', () => {
    for (const ip of ['10.0.0.1', '127.0.0.1', '169.254.169.254', '100.64.0.1', '192.168.1.1', '::1', 'fd00::1', 'fe80::1', '::ffff:10.0.0.1', '64:ff9b::7f00:1']) assert.equal(isPublicAddress(ip), false, ip);
    for (const ip of ['1.1.1.1', '2606:4700:4700::1111']) assert.equal(isPublicAddress(ip), true, ip);
  });
  test('Retry-After as seconds or a date', () => {
    assert.equal(retryAfter('120'), 120_000);
    assert.equal(retryAfter(new Date(Date.now() + 60_000).toUTCString(), () => Date.now()) > 50_000, true);
    assert.equal(retryAfter(null), null);
  });
});

describe('hostGate', () => {
  test('one request at a time per host, hosts side by side', async () => {
    const gate = hostGate({ concurrency: 1, delayMs: 0 });
    const order = [];
    const job = (host, id, wait) => gate.run(host, async () => {
      order.push(`start ${id}`);
      await new Promise((r) => setTimeout(r, wait));
      order.push(`end ${id}`);
    });
    await Promise.all([job('a', 'a1', 20), job('a', 'a2', 1), job('b', 'b1', 1)]);
    assert.ok(order.indexOf('end a1') < order.indexOf('start a2'));
    assert.ok(order.indexOf('start b1') < order.indexOf('end a1'));
  });
});

/** A fake web: url → { status, headers, body }, every host resolving to `ip`. */
function fakeWeb(routes, { ip = '93.184.216.34', ips = {} } = {}) {
  const seen = [];
  const fetchImpl = async (url) => {
    seen.push(url);
    const r = routes[url] ?? { status: 404, body: '' };
    return new Response(r.body ?? '', { status: r.status ?? 200, headers: r.headers ?? {} });
  };
  const lookup = async (host) => [{ address: ips[host] ?? ip }];
  return { seen, fetcher: createFetcher({ fetchImpl, lookup, gate: hostGate({ delayMs: 0 }), maxBytes: 1000 }) };
}

describe('fetch', () => {
  test('reads a page after a public redirect', async () => {
    const { fetcher } = fakeWeb({ 'https://a.example/': { status: 301, headers: { location: '/home' } }, 'https://a.example/home': { body: 'hi' } });
    const r = await fetcher.text('https://a.example/');
    assert.equal(r.text, 'hi');
    assert.equal(r.url, 'https://a.example/home');
  });
  test('refuses a private address, also behind a redirect', async () => {
    const { fetcher } = fakeWeb({ 'https://a.example/go': { status: 302, headers: { location: 'http://inside.example/' } } }, { ips: { 'inside.example': '10.1.2.3', 'meta.example': '169.254.169.254' } });
    await assert.rejects(fetcher.text('http://meta.example/'), (e) => e.code === 'blocked');
    await assert.rejects(fetcher.text('https://a.example/go'), (e) => e.code === 'blocked');
    await assert.rejects(fetcher.text('ftp://a.example/'), (e) => e.code === 'blocked');
    await assert.rejects(fetcher.text('https://a.example:8443/'), (e) => e.code === 'blocked');
  });
  test('honours robots.txt and caps size', async () => {
    const { fetcher } = fakeWeb({ 'https://a.example/robots.txt': { body: 'User-agent: *\nDisallow: /private' }, 'https://a.example/big': { body: 'x'.repeat(5000) } });
    await assert.rejects(fetcher.text('https://a.example/private/1'), (e) => e.code === 'robots');
    await assert.rejects(fetcher.text('https://a.example/big'), (e) => e.code === 'too-large');
  });
  test('a 429 carries how long to wait', async () => {
    const { fetcher } = fakeWeb({ 'https://a.example/x': { status: 429, headers: { 'retry-after': '120' } } });
    await assert.rejects(fetcher.text('https://a.example/x'), (e) => e instanceof FetchError && e.retryAfterMs === 120_000);
  });
});

describe('daemon', () => {
  test('a job succeeds, then a failing run climbs the error ladder', async () => {
    let clock = 1_000_000;
    let fail = false;
    const d = createDaemon({ log: quiet, jitter: 0, now: () => clock });
    d.job({ name: 'sync', every: '1h', backoff: ['5m', '1h'], run: async () => {
      if (fail) throw new Error('upstream down');
      return { rows: 3 };
    } });
    assert.equal((await d.runNow('sync')).ok, true);
    let s = (await d.status()).tasks[0];
    assert.equal(s.nextAt, clock + 3_600_000);
    fail = true;
    clock += 10;
    await d.runNow('sync');
    s = (await d.status()).tasks[0];
    assert.equal(s.fails, 1);
    assert.equal(s.nextAt, clock + 300_000);
    await d.runNow('sync');
    assert.equal((await d.status()).tasks[0].nextAt, clock + 3_600_000);
    assert.match((await d.status()).tasks[0].lastError, /upstream down/);
  });

  test('a queue runs items oldest first, a host’s items in series, and picks up new items', async () => {
    const items = [{ key: 'a1', url: 'https://a.example/1' }, { key: 'a2', url: 'https://a.example/2' }, { key: 'b1', url: 'https://b.example/1' }];
    const ran = [];
    const d = createDaemon({ log: quiet, concurrency: 8, tickMs: 5 });
    d.queue({ name: 'pages', every: '1d', concurrency: 8, itemsEvery: 1, items: async () => items, run: async (item) => {
      ran.push(item.key);
      await new Promise((r) => setTimeout(r, 40));
    } });
    d.start();
    await new Promise((r) => setTimeout(r, 20));
    // While a1 runs, a2 (same host) waits; b1 (another host) does not.
    assert.deepEqual(ran.toSorted(), ['a1', 'b1']);
    await new Promise((r) => setTimeout(r, 90));
    assert.deepEqual(ran.toSorted(), ['a1', 'a2', 'b1']);
    items.push({ key: 'c1', url: 'https://c.example/' });
    await new Promise((r) => setTimeout(r, 40));
    await d.stop();
    assert.ok(ran.includes('c1'));
    const s = (await d.status()).tasks[0];
    assert.equal(s.items, 4);
    assert.equal(s.due, 0);
  });

  test('an item can set its own cadence or retire; repeated failure retires it', async () => {
    const d = createDaemon({ log: quiet, jitter: 0 });
    let n = 0;
    d.queue({ name: 'feeds', every: '1h', retireAfter: 2, items: async () => ['fast', 'gone', 'broken'], run: async (item) => {
      if (item.key === 'fast') return { nextIn: '5m' };
      if (item.key === 'gone') return { retire: true };
      n++;
      throw new Error('500');
    } });
    await d.runNow('feeds');
    await d.runNow('feeds', 'broken');
    const runs = await d.store.load('feeds');
    assert.ok(runs.get('fast').nextAt - Date.now() <= 300_000 + 50);
    assert.equal(runs.get('gone').nextAt, Number.MAX_SAFE_INTEGER);
    assert.equal(runs.get('broken').nextAt, Number.MAX_SAFE_INTEGER);
    assert.equal(n, 2);
    const s = (await d.status()).tasks[0];
    assert.equal(s.retired, 2);
  });

  test('a 429 holds the whole host and does not count against the item', async () => {
    const d = createDaemon({ log: quiet });
    d.queue({ name: 'shops', items: async () => [{ key: 's1', url: 'https://slow.example/1' }], run: async () => {
      throw new FetchError('status', '429', { status: 429, retryAfterMs: 600_000 });
    } });
    const [out] = await d.runNow('shops');
    assert.equal(out.held, true);
    const rec = (await d.store.load('shops')).get('s1');
    assert.equal(rec.fails, 0);
    assert.deepEqual((await d.status()).heldHosts.map((h) => h.host), ['slow.example']);
  });

  test('a pipeline resumes from the step that failed and skips a token it already finished', async () => {
    const d = createDaemon({ log: quiet });
    const did = [];
    let breakSwap = true;
    d.pipeline({ name: 'dump', gate: async () => ({ token: 'stamp-1' }), steps: [
      { name: 'download', run: async () => did.push('download') },
      { name: 'swap', run: async () => {
        did.push('swap');
        if (breakSwap) throw new Error('disk full');
      } },
    ] });
    assert.equal((await d.runNow('dump')).ok, false);
    breakSwap = false;
    assert.equal((await d.runNow('dump')).ok, true);
    assert.deepEqual(did, ['download', 'swap', 'swap']);
    const again = await d.runNow('dump');
    assert.equal(again.result.skipped, true);
  });

  test('stall hook fires when a task has not succeeded within staleAfter', async () => {
    let clock = 0;
    const stalls = [];
    const d = createDaemon({ log: quiet, tickMs: 5, now: () => clock, onStall: (s) => stalls.push(s.job) });
    d.job({ name: 'flaky', every: '1h', staleAfter: '1h', run: async () => {
      throw new Error('nope');
    } });
    d.start();
    await new Promise((r) => setTimeout(r, 20));
    clock += 2 * 3_600_000;
    await new Promise((r) => setTimeout(r, 20));
    await d.stop();
    assert.deepEqual(stalls, ['flaky']);
  });
});

// bun:sqlite under Bun, node:sqlite under Node 22.5+; Node 20 skips the SQLite case.
const sqlite = await (async () => {
  try {
    const { Database } = await import('bun:sqlite');
    return new Database(':memory:');
  } catch {}
  try {
    const { DatabaseSync } = await import('node:sqlite');
    return new DatabaseSync(':memory:');
  } catch {}
  return null;
})();

describe('stores', () => {
  for (const [label, make] of [['memory', () => memoryStore()], ['sqlite', () => (sqlite ? sqliteStore(sqlite) : null)]]) {
    test(`${label}: runs and state round-trip`, async (t) => {
      const s = make();
      if (!s) return t.skip('no sqlite in this runtime');
      await s.save('j', { key: 'k', lastTry: 1, lastOk: 2, nextAt: 3, fails: 0, lastError: null });
      await s.save('j', { key: 'k', lastTry: 4, lastOk: 2, nextAt: 9, fails: 1, lastError: 'x' });
      const got = (await s.load('j')).get('k');
      assert.deepEqual(got, { key: 'k', lastTry: 4, lastOk: 2, nextAt: 9, fails: 1, lastError: 'x' });
      await s.set('cursor', { page: 3 });
      assert.deepEqual(await s.get('cursor'), { page: 3 });
      await s.forget('j', ['k']);
      assert.equal((await s.load('j')).size, 0);
    });
  }
});

describe('process lifetime', () => {
  test('a started daemon keeps a process alive on its own; stop lets it exit', async () => {
    const { spawn } = await import('node:child_process');
    const script = `import { createDaemon } from ${JSON.stringify(new URL('../src/index.js', import.meta.url).href)};
      let n = 0;
      const d = createDaemon({ log: () => {}, tickMs: 20 });
      d.job({ name: 'tick', every: 30, run: async () => { n++; } });
      d.start();
      setTimeout(async () => { console.log('ran', n > 1); await d.stop(); }, 300);`;
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'inherit'] });
    let out = '';
    child.stdout.on('data', (b) => (out += b));
    const code = await new Promise((resolve) => child.on('exit', resolve));
    assert.equal(out.trim(), 'ran true');
    assert.equal(code, 0);
  });
});
