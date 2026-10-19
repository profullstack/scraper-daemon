/**
 * Politeness per host: at most `concurrency` requests in flight to one host, and
 * at least `delayMs` between the start of one and the next. Hosts are independent,
 * so a slow site never holds up the others.
 */
export function hostGate({ concurrency = 1, delayMs = 1000, now = Date.now, sleep = defaultSleep } = {}) {
  const hosts = new Map();
  const slot = (host) => {
    let h = hosts.get(host);
    if (!h) hosts.set(host, (h = { active: 0, last: 0, waiting: [] }));
    return h;
  };
  const release = (host) => {
    const h = slot(host);
    h.active--;
    const next = h.waiting.shift();
    if (next) next();
    else if (!h.active) hosts.delete(host);
  };
  return {
    async run(host, fn) {
      const key = String(host ?? '').toLowerCase();
      const h = slot(key);
      if (h.active >= concurrency) await new Promise((resolve) => h.waiting.push(resolve));
      h.active++;
      try {
        const wait = h.last + delayMs - now();
        h.last = Math.max(now(), h.last + delayMs);
        if (wait > 0) await sleep(wait);
        return await fn();
      } finally {
        release(key);
      }
    },
    /** Hosts with work in flight or waiting, for status pages. */
    busy() {
      return [...hosts].map(([host, h]) => ({ host, active: h.active, waiting: h.waiting.length }));
    },
  };
}

const defaultSleep = (n) => new Promise((resolve) => setTimeout(resolve, n));
