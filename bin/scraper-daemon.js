#!/usr/bin/env node
/**
 * scraper-daemon status <site-url>   what a site's daemon is doing (reads <url>/api/v1/daemon)
 * scraper-daemon status <url> --json
 */
const [cmd, target, ...rest] = process.argv.slice(2);
const json = rest.includes('--json');

const ago = (t) => {
  if (!t) return 'never';
  const s = Math.round((Date.now() - t) / 1000);
  if (s < 0) return `in ${fmt(-s)}`;
  return `${fmt(s)} ago`;
};
const fmt = (s) => (s < 90 ? `${s}s` : s < 5400 ? `${Math.round(s / 60)}m` : s < 172800 ? `${Math.round(s / 3600)}h` : `${Math.round(s / 86400)}d`);
const pad = (s, n) => String(s ?? '').padEnd(n).slice(0, n);

async function status(url) {
  if (!url) throw new Error('usage: scraper-daemon status <site-url>');
  const base = url.replace(/\/$/, '');
  const endpoint = /\/api\/v1\/daemon$/.test(base) ? base : `${base}/api/v1/daemon`;
  const res = await fetch(endpoint, { headers: { accept: 'application/json' } });
  if (!res.ok) throw new Error(`${res.status} from ${endpoint}`);
  const s = await res.json();
  if (json) return console.log(JSON.stringify(s, null, 2));
  console.log(`${s.name}  ${s.running ? 'running' : 'stopped'} since ${ago(s.startedAt)}  ${s.active}/${s.concurrency} busy  store ${s.store}`);
  console.log(`\n${pad('task', 22)}${pad('kind', 10)}${pad('ok', 8)}${pad('failed', 8)}${pad('last ok', 12)}detail`);
  for (const t of s.tasks) {
    const detail = t.kind === 'queue' ? `${t.items} items, ${t.due} due, ${t.failing} failing, ${t.retired} retired` : t.nextAt ? `next ${ago(t.nextAt)}` : '';
    console.log(`${pad(t.name, 22)}${pad(t.kind, 10)}${pad(t.ok, 8)}${pad(t.failed, 8)}${pad(ago(t.lastOk), 12)}${detail}${t.stalled ? '  STALLED' : ''}`);
    if (t.lastError) console.log(`  last error: ${t.lastError}`);
  }
  for (const h of s.heldHosts ?? []) console.log(`held: ${h.host} until ${new Date(h.until).toISOString()}`);
}

const run = { status: () => status(target) }[cmd];
if (!run) {
  console.log('scraper-daemon status <site-url> [--json]');
  process.exit(cmd ? 1 : 0);
}
run().catch((err) => {
  console.error(`scraper-daemon: ${err.message}`);
  process.exit(1);
});
