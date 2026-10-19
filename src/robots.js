/** A robots.txt path pattern (`*` any run, trailing `$` end) as a prefix regex. */
const patternRe = (pattern) => new RegExp(`^${pattern.replace(/[.+?^{}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\\?\$$/, '$')}`);

/**
 * Whether `path` (with its query) may be fetched by `agent`. The group naming the
 * agent wins over `*`; inside a group the longest matching rule wins and Allow wins
 * a tie, as RFC 9309 says. No robots.txt (or an empty one) allows everything.
 */
export function robotsAllows(robots, path, agent = '*') {
  const groups = [];
  let current = null;
  let lastWasAgent = false;
  for (const raw of String(robots ?? '').split(/\r?\n/)) {
    const line = raw.replace(/#.*/, '').trim();
    const i = line.indexOf(':');
    if (i < 0) continue;
    const key = line.slice(0, i).trim().toLowerCase();
    const value = line.slice(i + 1).trim();
    if (key === 'user-agent') {
      if (!lastWasAgent) groups.push((current = { agents: [], rules: [] }));
      current.agents.push(value.toLowerCase());
      lastWasAgent = true;
    } else {
      lastWasAgent = false;
      if (current && (key === 'allow' || key === 'disallow') && value) current.rules.push({ allow: key === 'allow', len: value.length, re: patternRe(value) });
    }
  }
  const me = String(agent).toLowerCase().split('/')[0];
  const named = groups.filter((g) => g.agents.some((a) => a !== '*' && me.includes(a)));
  const chosen = named.length ? named : groups.filter((g) => g.agents.includes('*'));
  let best = { len: -1, allow: true };
  for (const g of chosen) {
    for (const r of g.rules) {
      if (!r.re.test(path)) continue;
      if (r.len > best.len || (r.len === best.len && r.allow)) best = r;
    }
  }
  return best.allow;
}
