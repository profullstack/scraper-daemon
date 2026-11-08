/**
 * The fleet crawler rule: a third-party target that is slow (a timeout) or broken (DNS,
 * TLS, a refused or reset connection, an HTTP 5xx) is flagged and left alone for 30 days,
 * then gets one recheck. Success clears the flag; another failure flags it for another 30.
 * No walking a minutes/hours ladder against a site that is down, and nothing dead forever.
 *
 * Pure functions, so a site can apply the same rule to its own loops.
 */

/** Total budget for one plain HTTP fetch of a third-party URL (connect + headers + body). */
export const CRAWL_FETCH_TIMEOUT_MS = 10_000;
/** Total navigation budget for a headless-browser render (JS, images, CSS). */
export const CRAWL_BROWSER_TIMEOUT_MS = 30_000;
/** How long a slow or broken target is left alone before its one recheck. */
export const FLAG_FOR_MS = 30 * 86_400_000;
/** How often an item retired after `retireAfter` failures in a row is rechecked. */
export const RETIRE_RECHECK_MS = 30 * 86_400_000;

// A timeout anywhere between us and the target.
const SLOW_CODES = new Set(['ETIMEDOUT', 'ESOCKETTIMEDOUT', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'ConnectionTimeout', 'TimeoutError']);
// The target cannot be reached or spoken to. EAI_AGAIN is deliberately absent: it is our own
// resolver failing for a moment (a Docker DNS hiccup fails every host at once), not the site.
const BROKEN_CODES = new Set([
  'ENOTFOUND',
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EHOSTDOWN',
  'EPIPE',
  'EPROTO',
  'UND_ERR_SOCKET',
  'UND_ERR_CLOSED',
  'ConnectionRefused',
  'ConnectionClosed',
  'FailedToOpenSocket',
]);
const TLS_CODE = /^(ERR_TLS_|ERR_SSL_|CERT_|UNABLE_TO_|DEPTH_ZERO_SELF_SIGNED_CERT|SELF_SIGNED_CERT_IN_CHAIN|HOSTNAME_MISMATCH)/;
const TRANSIENT_CODES = new Set(['EAI_AGAIN']);

function codesOf(err) {
  const out = [];
  for (let e = err, depth = 0; e && depth < 4; e = e.cause, depth++) {
    if (typeof e !== 'object' && typeof e !== 'function') break;
    for (const c of [e.code, e.errno, e.dnsCode, e.name]) if (typeof c === 'string') out.push(c);
  }
  return out;
}

/**
 * 'slow', 'broken' or null (anything else: robots, blocked, too-large, a 4xx, a parse error,
 * a bug in the site's own code, which keep the error ladder).
 *
 * Understands FetchError (code timeout/network/status), undici and Bun fetch errors (the
 * system code on `err.cause`), an AbortSignal.timeout TimeoutError, and any error carrying
 * `status` >= 500. A site can force the answer by throwing an error with `flag: 'slow' | 'broken' | null`.
 */
export function failureClass(err) {
  if (!err || (typeof err !== 'object' && typeof err !== 'function')) return null;
  if ('flag' in err && (err.flag === 'slow' || err.flag === 'broken' || err.flag === null)) return err.flag;
  // A host that asked us to slow down (429, 503 + Retry-After) is held, not flagged.
  if (err.retryAfterMs !== undefined && err.retryAfterMs !== null) return null;
  const codes = codesOf(err);
  if (codes.some((c) => TRANSIENT_CODES.has(c))) return null;
  if (err.name === 'FetchError' && err.code === 'timeout') return 'slow';
  if (codes.some((c) => SLOW_CODES.has(c))) return 'slow';
  const status = Number(err.status ?? err.statusCode ?? err.response?.status);
  if (Number.isFinite(status) && status >= 500 && status <= 599) return 'broken';
  if (err.name === 'FetchError' && err.code === 'network') return 'broken';
  if (codes.some((c) => BROKEN_CODES.has(c) || TLS_CODE.test(c))) return 'broken';
  // The daemon's own per-run timeout (its abort reason) and timeout messages from other clients.
  if (/\btimed out after\b|\boperation timed out\b|\btimeout of \d+ ?ms exceeded\b/i.test(String(err.message ?? ''))) return 'slow';
  return null;
}

/**
 * The schedule after a failure, as a pure function: { flag, nextIn } where flag is
 * 'slow' | 'broken' | 'retired' | null and nextIn is milliseconds (Infinity: never).
 * `ladderMs` is the error ladder's answer, used for failures the rule does not cover.
 * `flagFor: false` turns the 30-day flag off; `retireRecheck: false` retires for good (pre-0.4.0).
 */
export function flagPlan(err, { fails = 1, retireAfter = null, flagFor = FLAG_FOR_MS, retireRecheck = RETIRE_RECHECK_MS, ladderMs = 0, classify = failureClass } = {}) {
  if (retireAfter && fails >= retireAfter) return { flag: 'retired', nextIn: retireRecheck ? retireRecheck : Infinity };
  const cls = flagFor ? classify(err) : null;
  if (cls === 'slow' || cls === 'broken') return { flag: cls, nextIn: flagFor };
  return { flag: null, nextIn: ladderMs };
}
