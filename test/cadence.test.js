import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { createDaemon, DAY, HOUR, planNext, planUndated, quotas, tierFor, tierSql, WEEK } from '../src/index.js';

const NOW = Date.parse('2026-10-29T12:00:00Z');
const quiet = () => {};
/** `n` posts `every` minutes apart, the newest `ago` minutes before NOW. */
const posts = (n, every, ago = 0) => Array.from({ length: n }, (_, i) => new Date(NOW - (ago + i * every) * 60_000).toISOString());

describe('planNext', () => {
  test('a feed posting every half hour is read on the 15-minute floor', () => {
    const p = planNext(posts(20, 30, 25), { now: NOW });
    assert.equal(p.minutes, 15);
    assert.equal(p.tier, 'hot');
  });

  test('a weekly show just after an episode waits a quarter of its week', () => {
    const p = planNext(posts(10, WEEK, 60), { now: NOW });
    assert.equal(p.reason, 'between');
    assert.equal(p.minutes, WEEK / 4);
    assert.equal(p.tier, 'slow');
  });

  test('a weekly show never sleeps past the day its next episode is due', () => {
    // 4.5 days in: the window opens at 5.25 days, 0.75 days away.
    const p = planNext(posts(10, WEEK, 4.5 * DAY), { now: NOW });
    assert.equal(p.minutes, Math.round(0.75 * DAY));
  });

  test('a weekly show whose episode is due is read every six hours, not every 3.5 days', () => {
    const p = planNext(posts(10, WEEK, 6 * DAY), { now: NOW });
    assert.equal(p.reason, 'expected');
    assert.equal(p.minutes, 6 * HOUR);
    assert.equal(p.tier, 'active');
  });

  test('a daily feed in its window is read hourly at most every six hours', () => {
    assert.equal(planNext(posts(10, DAY, 20 * HOUR), { now: NOW }).minutes, 60);
    assert.equal(planNext(posts(10, 30 * DAY, 25 * DAY), { now: NOW }).minutes, 6 * HOUR);
  });

  test('a feed that went quiet backs off on its silence, capped at a week, and reads dormant', () => {
    const p = planNext(posts(10, DAY, 60 * DAY), { now: NOW });
    assert.equal(p.reason, 'quiet');
    assert.equal(p.minutes, WEEK);
    assert.equal(p.tier, 'dormant');
  });

  test('a live monthly feed stops a minute short of the ceiling so it never reads dormant', () => {
    const p = planNext(posts(10, 40 * DAY, DAY), { now: NOW });
    assert.equal(p.minutes, WEEK - 1);
    assert.equal(p.tier, 'slow');
  });

  test('six months of silence archives the feed, revisited quarterly', () => {
    const p = planNext(posts(10, DAY, 200 * DAY), { now: NOW });
    assert.equal(p.archived, true);
    assert.equal(p.tier, 'archived');
    assert.equal(p.minutes, 90 * DAY);
  });

  test('no usable dates is null, and far-future dates do not count', () => {
    assert.equal(planNext([], { now: NOW }), null);
    assert.equal(planNext([null, 'nonsense', new Date(NOW + 5 * DAY * 60_000).toISOString()], { now: NOW }), null);
  });

  test('options override the defaults', () => {
    assert.equal(planNext(posts(20, 30, 25), { now: NOW, floor: 60 }).minutes, 60);
  });
});

describe('planUndated', () => {
  test('halves on a change, doubles on none, within a day', () => {
    assert.equal(planUndated({ changed: true, previous: 120 }).minutes, 60);
    assert.equal(planUndated({ changed: false, previous: 120 }).minutes, 240);
    assert.equal(planUndated({ changed: false, previous: DAY }).minutes, DAY);
    assert.equal(planUndated({ changed: false, previous: DAY }).archived, false);
  });
});

describe('tiers', () => {
  test('tierFor and tierSql agree on the boundaries', () => {
    const cases = [[15, 'hot'], [60, 'hot'], [61, 'active'], [DAY, 'active'], [DAY + 1, 'slow'], [WEEK - 1, 'slow'], [WEEK, 'dormant'], [WEEK + 1, 'archived'], [90 * DAY, 'archived']];
    const sql = tierSql('m');
    for (const [m, tier] of cases) {
      assert.equal(tierFor(m), tier, `${m}`);
      // Evaluate the SQL case expression by hand: it is a ladder of `m <= n` / `m < n`.
      const js = sql.replace(/case when /, '').replace(/ end\)$/, '').replace(/^\(/, '');
      const arms = js.split(/ when /).map((a) => a.match(/m (<=|<) (\d+) then (\d)/)).filter(Boolean);
      const hit = arms.find(([, op, n]) => (op === '<=' ? m <= Number(n) : m < Number(n)));
      const index = hit ? Number(hit[3]) : 4;
      assert.equal(['hot', 'active', 'slow', 'dormant', 'archived'][index], tier, `sql ${m}`);
    }
  });

  test('quotas favour live tiers and always leave archived a slot', () => {
    const q = Object.fromEntries(quotas(100));
    assert.deepEqual(q, { hot: 40, active: 30, slow: 18, dormant: 10, archived: 2 });
    assert.equal(quotas(3).reduce((s, [, n]) => s + n, 0), 3);
  });
});

describe('daemon.feeds', () => {
  function site(rows) {
    const saved = [];
    const failed = [];
    const asked = [];
    return {
      saved,
      failed,
      asked,
      spec: {
        name: 'crawl',
        concurrency: 10,
        due: async (tier, limit) => {
          asked.push([tier, limit]);
          return rows.filter((r) => r.tier === tier && !saved.some((s) => s.feed.id === r.id) && !failed.some((f) => f.feed.id === r.id)).slice(0, limit);
        },
        run: async (feed) => {
          if (feed.broken) throw new Error('boom');
          return { dates: feed.dates };
        },
        save: async (feed, plan) => void saved.push({ feed, plan }),
        fail: async (feed, failure) => void failed.push({ feed, failure }),
      },
    };
  }

  test('reads hot feeds before quiet ones and hands unused slots down', async () => {
    const rows = [
      ...Array.from({ length: 3 }, (_, i) => ({ id: `h${i}`, tier: 'hot', url: `https://h${i}.example/feed`, dates: posts(10, 30) })),
      ...Array.from({ length: 20 }, (_, i) => ({ id: `s${i}`, tier: 'slow', url: `https://s${i}.example/feed`, dates: posts(10, WEEK) })),
    ];
    const s = site(rows);
    const d = createDaemon({ log: quiet, concurrency: 10, tickMs: 60_000 });
    d.feeds(s.spec);
    d.start();
    await new Promise((r) => setTimeout(r, 50));
    await d.stop();
    // One tick: 10 slots, all 3 hot feeds, and the other 7 went down to slow.
    assert.equal(s.saved.length, 10);
    assert.equal(s.saved.filter((x) => x.feed.tier === 'hot').length, 3);
    assert.equal(s.asked[0][0], 'hot');
    for (const { feed, plan } of s.saved) {
      assert.equal(plan.tier, feed.tier === 'hot' ? 'hot' : 'slow');
      assert.ok(plan.nextAt > Date.now());
    }
  });

  test('one feed per host at a time', async () => {
    const rows = Array.from({ length: 5 }, (_, i) => ({ id: `r${i}`, tier: 'hot', url: `https://reddit.example/r/${i}.rss`, dates: posts(5, 30) }));
    const s = site(rows);
    const d = createDaemon({ log: quiet, concurrency: 10, tickMs: 60_000 });
    d.feeds(s.spec);
    d.start();
    await new Promise((r) => setTimeout(r, 50));
    await d.stop();
    assert.equal(s.saved.length, 1);
  });

  test('a failure walks the ladder from the feed’s own count; a 429 holds the host and counts nothing', async () => {
    const s = site([]);
    const d = createDaemon({ log: quiet });
    d.feeds({
      ...s.spec,
      run: async (feed) => {
        if (feed.id === 'limited') throw Object.assign(new Error('429'), { retryAfterMs: 600_000 });
        throw new Error('boom');
      },
    });
    await d.runNow('crawl', { id: 'x', url: 'https://a.example/f', fails: 2 });
    await d.runNow('crawl', { id: 'limited', url: 'https://b.example/f' });
    assert.equal(s.failed[0].failure.fails, 3);
    assert.equal(s.failed[0].failure.minutes, 60);
    assert.equal(s.failed[1].failure.held, true);
    assert.equal(s.failed[1].failure.fails, null);
    const status = await d.status();
    assert.ok(status.heldHosts.some((h) => h.host === 'b.example'));
    assert.equal(status.tasks[0].kind, 'feeds');
  });

  test('runNow reads one feed at once (the refresh button) and the undated ladder applies', async () => {
    const s = site([]);
    const d = createDaemon({ log: quiet });
    d.feeds({ ...s.spec, run: async () => ({ changed: true }) });
    const out = await d.runNow('crawl', { id: 'u', url: 'https://u.example/f', minutes: 240 });
    assert.equal(out.ok, true);
    assert.equal(s.saved[0].plan.minutes, 120);
    assert.equal(s.saved[0].plan.reason, 'undated-changed');
  });

  test('needs due, run and save', () => {
    const d = createDaemon({ log: quiet });
    assert.throws(() => d.feeds({ name: 'x', due: async () => [], run: async () => {} }), /save/);
  });
});
