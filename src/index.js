export { createDaemon, jobIdFor, withTimeout } from './daemon.js';
export { redisStore } from './redis-store.js';
export { ms } from './duration.js';
export { CADENCE_DEFAULTS, DAY, DEFAULT_SHARES, HOUR, planNext, planUndated, publishTimes, quotas, tierFor, tierSql, TIERS, WEEK } from './cadence.js';
export { createFetcher, FetchError, isPublicAddress, retryAfter } from './fetch.js';
export { hostGate } from './gate.js';
export { robotsAllows } from './robots.js';
export { memoryStore, pgStore, sqliteStore } from './store.js';

/**
 * A fetch-style handler that serves daemon.status() as JSON, for any framework that
 * speaks Request/Response (Hono: `app.get('/api/v1/daemon', (c) => statusHandler(d)(c.req.raw))`).
 */
export function statusHandler(daemon) {
  return async () => new Response(JSON.stringify(await daemon.status()), { headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*' } });
}
