import assert from 'node:assert/strict';
import { after, describe, test } from 'node:test';
import { createDaemon, jobIdFor } from '../src/index.js';

/**
 * The BullMQ engine against a real Redis: REDIS_URL=redis://127.0.0.1:6379 node --test.
 * Skipped without one. Every test uses its own queue name and removes only its own keys.
 */
const url = process.env.REDIS_URL;
const quiet = () => {};
const sleep = (n) => new Promise((r) => setTimeout(r, n));
const until = async (fn, msLimit = 8000) => {
  const end = Date.now() + msLimit;
  while (Date.now() < end) {
    if (await fn()) return true;
    await sleep(50);
  }
  return false;
};

let redis = null;
const used = [];
if (url) {
  const { default: IORedis } = await import('ioredis');
  redis = new IORedis(url, { maxRetriesPerRequest: null });
}
after(async () => {
  if (!redis) return;
  for (const q of used) {
    for (const pattern of [`bull:${q}:*`, `sd:${q}:*`]) {
      const keys = await redis.keys(pattern);
      if (keys.length) await redis.del(...keys);
    }
  }
  await redis.quit();
});
const qname = (label) => {
  const q = `sd-test-${label}-${process.pid}-${Date.now()}`;
  used.push(q);
  return q;
};

describe('job ids', () => {
  test('never contain a colon, and stay unique per task and key', () => {
    const ids = [jobIdFor('price-watch', 'iptv:argontv'), jobIdFor('price-watch', 'seedbox:argontv'), jobIdFor('a:b', 'c:d:e'), jobIdFor('register', '*')];
    for (const id of ids) assert.equal(id.includes(':'), false, id);
    assert.equal(new Set(ids).size, ids.length);
  });
});

describe('bullmq engine', { skip: !url && 'REDIS_URL not set' }, () => {
  test('a job runs through the Worker; its record and state live in Redis; nothing is left behind', async () => {
    const q = qname('job');
    let runs = 0;
    const d = createDaemon({ name: 'test', queueName: q, redis: url, log: quiet, tickMs: 100 });
    d.job({ name: 'sync', every: '1h', run: async (ctx) => {
      runs++;
      await ctx.state.set('cursor', { page: 2 });
      return { rows: 3 };
    } });
    d.start();
    await d.ready();
    assert.ok(await until(async () => runs === 1), 'job ran');
    await sleep(300);
    const s = await d.status();
    assert.equal(s.engine, 'bullmq');
    assert.equal(s.store, 'redis');
    assert.equal(s.tasks[0].ok, 1);
    assert.ok(s.tasks[0].nextAt > Date.now() + 3_000_000);
    assert.equal(runs, 1, 'not run again before it is due');
    assert.equal(await redis.hget(`sd:${q}:state`, 'sync:cursor'), '{"page":2}');
    assert.ok(JSON.parse(await redis.hget(`sd:${q}:runs:sync`, '*')).lastOk > 0);
    // Completed jobs are removed and return nothing: no return values pile up in Redis.
    assert.equal((await redis.zcard(`bull:${q}:completed`)) ?? 0, 0);
    assert.ok((await redis.xlen(`bull:${q}:events`)) <= 120);
    await d.stop();
  });

  test('a queue runs every item, one per host at a time, and failures climb the ladder', async () => {
    const q = qname('queue');
    const items = [
      { key: 'a:1', url: 'https://a.example/1' },
      { key: 'a:2', url: 'https://a.example/2' },
      { key: 'b:1', url: 'https://b.example/1' },
      { key: 'bad', url: 'https://c.example/' },
    ];
    const live = new Map();
    let overlap = false;
    const ran = [];
    const d = createDaemon({ name: 'test', queueName: q, redis: url, log: quiet, tickMs: 50, concurrency: 4, jitter: 0 });
    d.queue({ name: 'pages', every: '1d', concurrency: 4, backoff: ['10m'], items: async () => items, run: async (item) => {
      const host = new URL(item.url).host;
      if (live.get(host)) overlap = true;
      live.set(host, true);
      await sleep(80);
      live.set(host, false);
      ran.push(item.key);
      if (item.key === 'bad') throw new Error('500 from upstream');
    } });
    d.start();
    await d.ready();
    assert.ok(await until(async () => ran.length >= 4), `all ran (${ran})`);
    assert.equal(overlap, false, 'two items of one host never ran together');
    await sleep(200);
    const s = (await d.status()).tasks[0];
    assert.equal(s.items, 4);
    assert.equal(s.failing, 1);
    const bad = JSON.parse(await redis.hget(`sd:${q}:runs:pages`, 'bad'));
    assert.equal(bad.fails, 1);
    assert.ok(bad.nextAt >= Date.now() + 9 * 60_000);
    assert.equal(ran.length, 4, 'nothing re-ran before it was due');
    await d.stop();
  });

  test('a pipeline resumes at the failed step across a restart, with its progress in Redis', async () => {
    const q = qname('pipe');
    const did = [];
    let breakSwap = true;
    const build = () => {
      const d = createDaemon({ name: 'test', queueName: q, redis: url, log: quiet, tickMs: 50, jitter: 0 });
      d.pipeline({ name: 'dump', every: '1h', backoff: ['1s'], gate: async () => ({ token: 'stamp-1' }), steps: [
        { name: 'download', run: async () => did.push('download') },
        { name: 'swap', run: async () => {
          did.push('swap');
          if (breakSwap) throw new Error('disk full');
        } },
      ] });
      return d;
    };
    let d = build().start();
    await d.ready();
    assert.ok(await until(async () => did.includes('swap')));
    await d.stop();
    breakSwap = false;
    d = build().start();
    await d.ready();
    assert.ok(await until(async () => did.filter((x) => x === 'swap').length === 2, 6000), `resumed (${did})`);
    assert.deepEqual(did, ['download', 'swap', 'swap']);
    assert.equal(JSON.parse(await redis.hget(`sd:${q}:state`, 'dump:done')), 'stamp-1');
    await d.stop();
  });
});
