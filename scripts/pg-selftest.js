#!/usr/bin/env bun
/**
 * pgStore against a real Postgres through Bun's SQL client, on throwaway tables
 * (prefix scraper_daemon_selftest) that are dropped at the end.
 *
 *   DATABASE_URL=postgres://… bun scripts/pg-selftest.js
 */
import { SQL } from 'bun';
import { pgStore } from '../src/index.js';

const sql = new SQL(process.env.DATABASE_URL);
const query = (text, params) => sql.unsafe(text, params);
const PREFIX = 'scraper_daemon_selftest';
const sorted = (v) => JSON.stringify(v, (_, x) => (x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort()) : x));
const typeOf = (v) => (v === null ? 'null' : Array.isArray(v) ? 'array' : typeof v === 'object' ? 'object' : typeof v);
let failed = 0;
const check = (ok, ...msg) => {
  if (!ok) failed++;
  console.log(ok ? 'ok  ' : 'FAIL', ...msg);
};

try {
  await sql.unsafe(`DROP TABLE IF EXISTS ${PREFIX}_runs, ${PREFIX}_state`);
  // A row as 0.1.0 wrote it through Bun's SQL: a jsonb string holding JSON text.
  await sql.unsafe(`CREATE TABLE ${PREFIX}_state (key text PRIMARY KEY, value jsonb NOT NULL)`);
  await sql.unsafe(`INSERT INTO ${PREFIX}_state VALUES ('old', to_jsonb('{"page":3}'::text)), ('plain', to_jsonb('stamp'::text))`);
  const s = pgStore(query, { prefix: PREFIX });
  check(sorted(await s.get('old')) === sorted({ page: 3 }), 'a 0.1.0 double-encoded object is unwrapped on init');
  check((await s.get('plain')) === 'stamp', 'a plain string stays a string');

  for (const v of ['stamp-1', 'Sat, 17 Oct 2026 23:15:52 GMT|1800', 42, true, null, { page: 3, at: 'z' }, ['a', 1], '{"looks":"like json"}']) {
    await s.set('k', v);
    const got = await s.get('k');
    const [{ t }] = await sql.unsafe(`SELECT jsonb_typeof(value) AS t FROM ${PREFIX}_state WHERE key = $1`, ['k']);
    const want = typeOf(v) === 'number' ? 'number' : typeOf(v) === 'boolean' ? 'boolean' : typeOf(v);
    check(sorted(got) === sorted(v) && t === want, JSON.stringify(v), '->', JSON.stringify(got), `(jsonb ${t})`);
  }
  await s.save('j', { key: 'k', lastTry: 4, lastOk: null, nextAt: Number.MAX_SAFE_INTEGER, fails: 1, lastError: 'x' });
  const rec = (await s.load('j')).get('k');
  check(rec.nextAt === Number.MAX_SAFE_INTEGER && rec.lastOk === null, 'runs round trip', JSON.stringify(rec));
} finally {
  await sql.unsafe(`DROP TABLE IF EXISTS ${PREFIX}_runs, ${PREFIX}_state`);
  await sql.close();
}
process.exit(failed ? 1 : 0);
