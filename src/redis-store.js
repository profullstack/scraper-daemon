/**
 * The store the BullMQ engine uses by default: run records and task state in Redis,
 * beside the queue, so a site's own database holds only its own data.
 *
 *   <prefix>:runs:<job>   hash  key -> JSON run record
 *   <prefix>:state        hash  key -> JSON value
 *
 * `client` is an ioredis client (the engine's own connection).
 */
export function redisStore(client, { prefix = 'sd' } = {}) {
  const runsKey = (job) => `${prefix}:runs:${job}`;
  const stateKey = `${prefix}:state`;
  return {
    kind: 'redis',
    async load(job) {
      const all = await client.hgetall(runsKey(job));
      const out = new Map();
      for (const [k, v] of Object.entries(all ?? {})) {
        try {
          out.set(k, JSON.parse(v));
        } catch {}
      }
      return out;
    },
    async save(job, rec) {
      await client.hset(runsKey(job), rec.key, JSON.stringify(rec));
    },
    async forget(job, keys) {
      if (keys.length) await client.hdel(runsKey(job), ...keys);
    },
    async get(key) {
      const v = await client.hget(stateKey, key);
      return v === null || v === undefined ? null : JSON.parse(v);
    },
    async set(key, value) {
      await client.hset(stateKey, key, JSON.stringify(value ?? null));
    },
  };
}
