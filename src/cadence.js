/**
 * How often a feed is worth re-reading, judged by how often it actually posts.
 *
 * Pure: no clock, no database. Every site that crawls feeds (rssamplifier, p0dcasters)
 * asks the same three questions of a feed's publish dates, and this is the one answer:
 *
 *   * how long until the next read (15 minutes for a feed posting every hour, up to a
 *     week for one posting monthly),
 *   * which tier that puts it in, so the dispatcher reads live feeds before quiet ones,
 *   * whether it has gone silent long enough to archive: kept for the record, out of the
 *     crawl queue, looked at again once a quarter in case it came back.
 *
 * All durations are minutes, the unit the sites already store.
 */

export const HOUR = 60;
export const DAY = 24 * HOUR;
export const WEEK = 7 * DAY;

export const CADENCE_DEFAULTS = Object.freeze({
  /** Never read a feed more often than this, however fast it posts. */
  floor: 15,
  /** Never wait longer than this between reads of a feed that is not archived. */
  ceiling: WEEK,
  /** Silent this long and the feed is archived. */
  archiveAfter: 180 * DAY,
  /** How often an archived feed is looked at again, in case it came back. */
  revisitEvery: 90 * DAY,
  /** This many rhythms of silence and a feed counts as gone quiet, not between posts. */
  quietMultiple: 3,
  /** The share of its rhythm after which the next post is expected. */
  windowStart: 0.75,
  /** Inside that window, read this many times per rhythm... */
  windowReads: 24,
  /** ...but at least this often. */
  windowCeiling: 6 * HOUR,
  /** Outside it, this many times per rhythm. */
  idleReads: 4,
  /** How many of the newest dates to read the rhythm from. */
  sample: 30,
});

/** Tiers in the order the dispatcher serves them. */
export const TIERS = Object.freeze(['hot', 'active', 'slow', 'dormant', 'archived']);

/**
 * The tier an interval puts a feed in. Derived from the interval rather than stored beside
 * it, so a site can compute it in SQL from the one column it already has:
 *
 *   case when m <= 60 then 0 when m <= 1440 then 1 when m < 10080 then 2
 *        when m <= 10080 then 3 else 4 end
 *
 * @param {number} minutes
 * @param {{ ceiling?: number }} [opts]
 * @returns {'hot'|'active'|'slow'|'dormant'|'archived'}
 */
export function tierFor(minutes, { ceiling = CADENCE_DEFAULTS.ceiling } = {}) {
  const m = Number(minutes);
  if (!Number.isFinite(m) || m <= HOUR) return 'hot';
  if (m <= DAY) return 'active';
  if (m < ceiling) return 'slow';
  if (m <= ceiling) return 'dormant';
  return 'archived';
}

/** {@link tierFor} as SQL over an integer minutes column, for an index and an `order by`. */
export function tierSql(column = 'fetch_interval_minutes', { ceiling = CADENCE_DEFAULTS.ceiling } = {}) {
  return `(case when ${column} <= ${HOUR} then 0 when ${column} <= ${DAY} then 1 when ${column} < ${ceiling} then 2 when ${column} <= ${ceiling} then 3 else 4 end)`;
}

/**
 * The publish instants a document carries, newest first: unparseable, absent and
 * far-future dates dropped (a feed stamping tomorrow on every entry is broken, not fast).
 *
 * @param {Array<unknown>} dates Date, epoch ms, or anything Date.parse reads
 * @param {number} now epoch ms
 * @returns {number[]}
 */
export function publishTimes(dates, now, { sample = CADENCE_DEFAULTS.sample } = {}) {
  const out = [];
  for (const d of dates ?? []) {
    if (d === null || d === undefined || d === '') continue;
    const t = d instanceof Date ? d.getTime() : typeof d === 'number' ? d : Date.parse(String(d));
    if (!Number.isFinite(t) || t > now + DAY * 60_000) continue;
    out.push(t);
  }
  return out.sort((a, b) => b - a).slice(0, sample);
}

/**
 * When to read a feed next, from the dates it published at.
 *
 * Returns null when there are no usable dates: an undated feed is common on the small
 * web, and the caller's own fallback (a change log, a doubling ladder) knows more than
 * a guess made here would.
 *
 * @param {Array<unknown>} dates publish dates, any order
 * @param {object} [opts] any of CADENCE_DEFAULTS, plus `now` (epoch ms)
 * @returns {null | { minutes: number, tier: string, archived: boolean, rhythm: number|null, silence: number, reason: string }}
 */
export function planNext(dates, opts = {}) {
  const o = { ...CADENCE_DEFAULTS, ...opts };
  const now = opts.now ?? Date.now();
  const times = publishTimes(dates, now, o);
  if (times.length === 0) return null;

  const silence = Math.max(0, (now - times[0]) / 60_000);
  const done = (minutes, reason, rhythm = null) => {
    const archived = reason === 'archived';
    // Exactly the ceiling is reserved for feeds that went quiet, so the tier read back off
    // the interval alone says dormant only of those; a live monthly feed stops a minute short.
    const quiet = reason === 'quiet' || reason === 'single';
    const m = archived ? Math.round(o.revisitEvery) : clamp(minutes, o.floor, quiet ? o.ceiling : o.ceiling - 1);
    return { minutes: m, tier: archived ? 'archived' : tierFor(m, o), archived, rhythm, silence: Math.round(silence), reason };
  };

  if (silence >= o.archiveAfter) return done(0, 'archived');

  const spacing = gaps(times);
  // One publishing event (or one date): no rhythm to read, only the silence.
  if (spacing.length === 0) return done(silence / o.idleReads, 'single');

  const rhythm = median(spacing);
  if (silence > rhythm * o.quietMultiple) return done(silence / o.idleReads, 'quiet', Math.round(rhythm));

  // The next post is due: read often until it shows up.
  const windowOpens = rhythm * o.windowStart;
  if (silence >= windowOpens) return done(Math.min(rhythm / o.windowReads, o.windowCeiling), 'expected', Math.round(rhythm));

  // Between posts: read now and then, but never sleep past the point the window opens.
  const untilWindow = windowOpens - silence;
  return done(Math.min(rhythm / o.idleReads, Math.max(untilWindow, rhythm / o.windowReads)), 'between', Math.round(rhythm));
}

/**
 * The fallback for a feed with no dates: halve on a change, double on none, within the
 * floor and the ceiling. Undated means unknown, not abandoned, so it never archives.
 *
 * @param {{ changed: boolean, previous?: number|null }} input
 * @param {object} [opts]
 */
export function planUndated({ changed, previous = null }, opts = {}) {
  const o = { ...CADENCE_DEFAULTS, ...opts };
  const prev = Number(previous) > 0 ? Number(previous) : o.floor * 4;
  const m = clamp(changed ? prev / 2 : prev * 2, o.floor, Math.min(o.ceiling - 1, DAY));
  return { minutes: m, tier: tierFor(m, o), archived: false, rhythm: null, silence: null, reason: changed ? 'undated-changed' : 'undated-unchanged' };
}

/**
 * Split `room` dispatch slots across tiers by share, live tiers first. Slots a tier
 * cannot use (nothing due) are handed down by the dispatcher, so the shares are a
 * floor for each tier under load, never a cap when there is room.
 *
 * @param {number} room
 * @param {Record<string, number>} shares
 * @returns {Array<[string, number]>}
 */
export function quotas(room, shares = DEFAULT_SHARES) {
  const total = TIERS.reduce((s, t) => s + (shares[t] ?? 0), 0) || 1;
  let left = room;
  const out = [];
  for (const t of TIERS) {
    const q = Math.min(left, Math.round((room * (shares[t] ?? 0)) / total));
    out.push([t, q]);
    left -= q;
  }
  // Whatever rounding left over goes to the hottest tier.
  if (left > 0 && out.length) out[0][1] += left;
  return out;
}

/** Under load, most reads go to feeds that post often; archived feeds get a trickle. */
export const DEFAULT_SHARES = Object.freeze({ hot: 0.4, active: 0.3, slow: 0.18, dormant: 0.1, archived: 0.02 });

function gaps(timesNewestFirst) {
  const sorted = [...timesNewestFirst].sort((a, b) => a - b);
  const out = [];
  for (let i = 1; i < sorted.length; i++) {
    const g = (sorted[i] - sorted[i - 1]) / 60_000;
    // Posts stamped in the same instant are one act, not a rhythm of zero.
    if (g > 0) out.push(g);
  }
  return out.sort((a, b) => a - b);
}

function median(sorted) {
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function clamp(n, lo, hi) {
  if (!Number.isFinite(n)) return lo;
  return Math.round(Math.min(Math.max(n, lo), hi));
}
