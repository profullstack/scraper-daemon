const UNITS = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 };

/** '90s', '15m', '1h', '7d', '1h30m' or a number of milliseconds → milliseconds. */
export function ms(value) {
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) return value;
  const s = String(value ?? '').trim().toLowerCase();
  if (/^\d+(\.\d+)?$/.test(s)) return Number(s);
  let total = 0;
  let rest = s;
  const re = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)/;
  while (rest) {
    const m = rest.match(re);
    if (!m) throw new TypeError(`not a duration: ${JSON.stringify(value)}`);
    total += Number(m[1]) * UNITS[m[2]];
    rest = rest.slice(m[0].length).trim();
  }
  if (!s) throw new TypeError(`not a duration: ${JSON.stringify(value)}`);
  return total;
}
