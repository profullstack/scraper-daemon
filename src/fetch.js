import { lookup as dnsLookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { hostGate } from './gate.js';
import { robotsAllows } from './robots.js';

/**
 * The one way a daemon job reads the web: polite (per-host gate, robots.txt) and
 * safe (http(s) on 80/443 only, every resolved address public, every redirect hop
 * re-checked, time and size capped). Errors are FetchError with a `code` a job can
 * branch on: blocked, robots, status, timeout, too-large, network.
 */
export class FetchError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.name = 'FetchError';
    this.code = code;
    Object.assign(this, extra);
  }
}

const privateV4 = (ip) => {
  const [a, b] = ip.split('.').map(Number);
  return a === 0 || a === 10 || a === 127 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127) || (a === 198 && (b === 18 || b === 19)) || a >= 224;
};

/** True for an address on the public internet (v4, v6, v4-mapped and NAT64 all checked). */
export function isPublicAddress(ip) {
  const v = String(ip).toLowerCase();
  if (isIP(v) === 4) return !privateV4(v);
  if (isIP(v) !== 6) return false;
  const mapped = v.match(/^(?:::ffff:|64:ff9b::)(\d+\.\d+\.\d+\.\d+)$/);
  if (mapped) return !privateV4(mapped[1]);
  return !(v === '::' || v === '::1' || /^f[cd]/.test(v) || /^fe[89ab]/.test(v) || v.startsWith('ff') || v.startsWith('64:ff9b:') || v.startsWith('2001:db8'));
}

/** Retry-After as seconds or an HTTP date → milliseconds from now, or null. */
export function retryAfter(value, now = Date.now) {
  if (!value) return null;
  const s = String(value).trim();
  if (/^\d+$/.test(s)) return Number(s) * 1000;
  const t = Date.parse(s);
  return Number.isFinite(t) ? Math.max(0, t - now()) : null;
}

export function createFetcher({
  userAgent = 'scraper-daemon (+https://github.com/profullstack/scraper-daemon)',
  timeoutMs = 15_000,
  maxBytes = 2_000_000,
  maxRedirects = 5,
  robots = true,
  allowPrivate = false,
  gate = hostGate(),
  robotsTtlMs = 6 * 3_600_000,
  fetchImpl = globalThis.fetch,
  lookup = (host) => dnsLookup(host, { all: true }),
  now = Date.now,
} = {}) {
  const robotsCache = new Map();

  async function check(url) {
    let u;
    try {
      u = new URL(url);
    } catch {
      throw new FetchError('blocked', `not a URL: ${url}`);
    }
    if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new FetchError('blocked', `only http(s): ${url}`);
    if (u.port && u.port !== '80' && u.port !== '443') throw new FetchError('blocked', `only ports 80 and 443: ${url}`);
    if (allowPrivate) return u;
    const host = u.hostname.replace(/^\[|\]$/g, '');
    const addrs = isIP(host) ? [{ address: host }] : await lookup(host).catch(() => []);
    if (!addrs.length) throw new FetchError('network', `no address for ${host}`);
    if (!addrs.every((a) => isPublicAddress(a.address))) throw new FetchError('blocked', `${host} resolves to a private address`);
    return u;
  }

  async function raw(url, { accept, signal, method = 'GET', headers = {} } = {}) {
    let at = url;
    for (let hop = 0; hop <= maxRedirects; hop++) {
      const u = await check(at);
      const res = await gate.run(u.host, () =>
        fetchImpl(u.toString(), {
          method,
          redirect: 'manual',
          headers: { 'user-agent': userAgent, accept: accept ?? '*/*', ...headers },
          signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]) : AbortSignal.timeout(timeoutMs),
        }),
      ).catch((err) => {
        if (err instanceof FetchError) throw err;
        throw new FetchError(err?.name === 'TimeoutError' ? 'timeout' : 'network', `${u.host}: ${err?.message ?? err}`);
      });
      const location = res.headers.get('location');
      if (res.status >= 300 && res.status < 400 && location) {
        res.body?.cancel().catch(() => {});
        at = new URL(location, u).toString();
        continue;
      }
      return { res, url: u.toString() };
    }
    throw new FetchError('blocked', `more than ${maxRedirects} redirects from ${url}`);
  }

  async function body(res) {
    const declared = Number(res.headers.get('content-length'));
    if (declared > maxBytes) {
      res.body?.cancel().catch(() => {});
      throw new FetchError('too-large', `${declared} bytes is over the ${maxBytes} cap`);
    }
    const reader = res.body?.getReader();
    if (!reader) return '';
    const chunks = [];
    let size = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > maxBytes) {
        reader.cancel().catch(() => {});
        throw new FetchError('too-large', `over the ${maxBytes} byte cap`);
      }
      chunks.push(value);
    }
    const all = new Uint8Array(size);
    let at = 0;
    for (const c of chunks) {
      all.set(c, at);
      at += c.length;
    }
    return new TextDecoder().decode(all);
  }

  async function allowed(url, signal) {
    if (!robots) return true;
    const u = new URL(url);
    const hit = robotsCache.get(u.origin);
    let text = hit && hit.at + robotsTtlMs > now() ? hit.text : null;
    if (text === null) {
      text = '';
      try {
        const { res } = await raw(`${u.origin}/robots.txt`, { accept: 'text/plain', signal });
        // A missing robots.txt allows everything; a server error means "try later", which we read as allow-all too.
        if (res.ok) text = await body(res);
        else res.body?.cancel().catch(() => {});
      } catch (err) {
        if (err?.code === 'blocked') throw err;
      }
      robotsCache.set(u.origin, { at: now(), text });
    }
    return robotsAllows(text, `${u.pathname}${u.search}`, userAgent);
  }

  /** GET a page: { url (after redirects), status, headers, text }. Throws FetchError on anything but 2xx. */
  async function text(url, opts = {}) {
    if (!(await allowed(url, opts.signal))) throw new FetchError('robots', `robots.txt disallows ${url}`);
    const { res, url: final } = await raw(url, opts);
    if (!res.ok) {
      res.body?.cancel().catch(() => {});
      const extra = { status: res.status, url: final };
      // A host that says "slow down" gets held back for as long as it asked (default 30 min).
      if (res.status === 429 || (res.status === 503 && res.headers.get('retry-after'))) extra.retryAfterMs = retryAfter(res.headers.get('retry-after'), now) ?? 30 * 60_000;
      throw new FetchError('status', `${res.status} from ${final}`, extra);
    }
    return { url: final, status: res.status, headers: res.headers, text: await body(res) };
  }

  async function json(url, opts = {}) {
    const r = await text(url, { accept: 'application/json', ...opts });
    try {
      return JSON.parse(r.text);
    } catch {
      throw new FetchError('status', `not JSON from ${r.url}`, { url: r.url });
    }
  }

  return { text, json, allowed, check, gate };
}
