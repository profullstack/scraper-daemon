/**
 * Where a daemon remembers what it did: one row per (job, key) with when it last
 * tried, last succeeded, is next due, how many times in a row it has failed and the
 * last error; and a small key/value table for cursors and watermarks. Every store
 * keeps the same contract, so a site picks the one its database already is:
 *
 *   memoryStore()            tests, or a site happy to start over on restart
 *   sqliteStore(db)          bun:sqlite or better-sqlite3 (anything with .prepare/.exec)
 *   pgStore(query)           Postgres through any query(text, params) function
 *
 * Times are epoch milliseconds.
 */

const fromRow = (r) =>
  r && {
    key: r.key,
    lastTry: r.last_try === null || r.last_try === undefined ? null : Number(r.last_try),
    lastOk: r.last_ok === null || r.last_ok === undefined ? null : Number(r.last_ok),
    nextAt: r.next_at === null || r.next_at === undefined ? null : Number(r.next_at),
    fails: Number(r.fails ?? 0),
    lastError: r.last_error ?? null,
  };

export function memoryStore() {
  const runs = new Map();
  const state = new Map();
  return {
    kind: 'memory',
    async load(job) {
      return new Map([...(runs.get(job) ?? new Map())].map(([k, v]) => [k, { ...v }]));
    },
    async save(job, rec) {
      if (!runs.has(job)) runs.set(job, new Map());
      runs.get(job).set(rec.key, { ...rec });
    },
    async forget(job, keys) {
      for (const k of keys) runs.get(job)?.delete(k);
    },
    async get(key) {
      return state.has(key) ? structuredClone(state.get(key)) : null;
    },
    async set(key, value) {
      state.set(key, structuredClone(value));
    },
  };
}

export function sqliteStore(db, { prefix = 'scraper_daemon' } = {}) {
  if (!/^[a-z_][a-z0-9_]*$/i.test(prefix)) throw new TypeError(`bad table prefix ${prefix}`);
  const runs = `${prefix}_runs`;
  const kv = `${prefix}_state`;
  db.exec(`CREATE TABLE IF NOT EXISTS ${runs} (
    job TEXT NOT NULL, key TEXT NOT NULL, last_try INTEGER, last_ok INTEGER, next_at INTEGER,
    fails INTEGER NOT NULL DEFAULT 0, last_error TEXT, PRIMARY KEY (job, key))`);
  db.exec(`CREATE TABLE IF NOT EXISTS ${kv} (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
  const q = {
    load: db.prepare(`SELECT key, last_try, last_ok, next_at, fails, last_error FROM ${runs} WHERE job = ?`),
    save: db.prepare(`INSERT INTO ${runs} (job, key, last_try, last_ok, next_at, fails, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (job, key) DO UPDATE SET last_try = excluded.last_try, last_ok = excluded.last_ok, next_at = excluded.next_at, fails = excluded.fails, last_error = excluded.last_error`),
    forget: db.prepare(`DELETE FROM ${runs} WHERE job = ? AND key = ?`),
    get: db.prepare(`SELECT value FROM ${kv} WHERE key = ?`),
    set: db.prepare(`INSERT INTO ${kv} (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`),
  };
  return {
    kind: 'sqlite',
    async load(job) {
      return new Map(q.load.all(job).map((r) => [r.key, fromRow(r)]));
    },
    async save(job, r) {
      q.save.run(job, r.key, r.lastTry, r.lastOk, r.nextAt, r.fails, r.lastError);
    },
    async forget(job, keys) {
      for (const k of keys) q.forget.run(job, k);
    },
    async get(key) {
      const row = q.get.get(key);
      return row ? JSON.parse(row.value) : null;
    },
    async set(key, value) {
      q.set.run(key, JSON.stringify(value));
    },
  };
}

/**
 * `query(text, params)` may return an array of rows or `{ rows }` (pg, postgres.js
 * via sql.unsafe, Bun's SQL via sql.unsafe all fit). Tables are created on first use.
 */
export function pgStore(query, { prefix = 'scraper_daemon' } = {}) {
  if (!/^[a-z_][a-z0-9_]*$/i.test(prefix)) throw new TypeError(`bad table prefix ${prefix}`);
  const runs = `${prefix}_runs`;
  const kv = `${prefix}_state`;
  const rows = async (text, params) => {
    const r = await query(text, params);
    return Array.isArray(r) ? r : (r?.rows ?? []);
  };
  let ready = null;
  const init = () =>
    (ready ??= (async () => {
      await rows(`CREATE TABLE IF NOT EXISTS ${runs} (job text NOT NULL, key text NOT NULL, last_try bigint, last_ok bigint, next_at bigint, fails integer NOT NULL DEFAULT 0, last_error text, PRIMARY KEY (job, key))`, []);
      await rows(`CREATE TABLE IF NOT EXISTS ${kv} (key text PRIMARY KEY, value jsonb NOT NULL)`, []);
      // 0.1.0 wrote state double-encoded through Bun's SQL (a jsonb string holding JSON text). Unwrap those once.
      for (const r of await rows(`SELECT key, value #>> '{}' AS inner FROM ${kv} WHERE jsonb_typeof(value) = 'string'`, [])) {
        const inner = String(r.inner ?? '');
        if (!/^[[{"]/.test(inner)) continue;
        try {
          JSON.parse(inner);
        } catch {
          continue;
        }
        await rows(`UPDATE ${kv} SET value = $2::text::jsonb WHERE key = $1`, [r.key, inner]);
      }
    })());
  return {
    kind: 'postgres',
    async load(job) {
      await init();
      return new Map((await rows(`SELECT key, last_try, last_ok, next_at, fails, last_error FROM ${runs} WHERE job = $1`, [job])).map((r) => [r.key, fromRow(r)]));
    },
    async save(job, r) {
      await init();
      await rows(
        `INSERT INTO ${runs} (job, key, last_try, last_ok, next_at, fails, last_error) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (job, key) DO UPDATE SET last_try = EXCLUDED.last_try, last_ok = EXCLUDED.last_ok, next_at = EXCLUDED.next_at, fails = EXCLUDED.fails, last_error = EXCLUDED.last_error`,
        [job, r.key, r.lastTry, r.lastOk, r.nextAt, r.fails, r.lastError],
      );
    },
    async forget(job, keys) {
      await init();
      if (keys.length) await rows(`DELETE FROM ${runs} WHERE job = $1 AND key = ANY($2::text[])`, [job, `{${keys.map((k) => `"${String(k).replace(/["\\]/g, '\\$&')}"`).join(',')}}`]);
    },
    // Values cross the driver as JSON text both ways (`$2::text::jsonb` in, `value::text` out): drivers
    // disagree on how they encode a jsonb parameter and decode a jsonb column, and text means the same to all.
    async get(key) {
      await init();
      const [row] = await rows(`SELECT value::text AS value FROM ${kv} WHERE key = $1`, [key]);
      return row ? JSON.parse(row.value) : null;
    },
    async set(key, value) {
      await init();
      await rows(`INSERT INTO ${kv} (key, value) VALUES ($1, $2::text::jsonb) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, [key, JSON.stringify(value ?? null)]);
    },
  };
}
