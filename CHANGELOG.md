# Changelog

## 0.4.0 (2026-11-08)

The fleet crawler rule: "crawlers should timeout after 10 seconds for fetch, 30 for browser. We don't want broken slow shit. Flag to recheck later in a month."

- `createFetcher` defaults to a **10 s** budget (was 15 s), and it now covers the whole request: every redirect hop, the headers and the body (it was per hop, headers only). Body-read failures are `FetchError`s too. `fetch: { timeoutMs }` still overrides.
- **Slow or broken targets are flagged for 30 days** instead of walking the `5m … 1d` ladder: timeouts (`slow`); DNS `ENOTFOUND`, TLS, refused/reset connections and HTTP 5xx (`broken`). After 30 days the item gets one recheck; success clears the flag, failure flags it another 30 days. Applies to `queue` items and `feeds` by default; `job` and `pipeline` keep the ladder. 4xx, robots, parse errors and `EAI_AGAIN` keep the ladder.
- **`retireAfter` no longer means forever**: a retired item is rechecked once every 30 days (`retireRecheck`). `{ retire: true }` from `run` still retires for good.
- Run records gain `flag` ('slow' | 'broken' | 'retired' | null) and `flaggedAt`. `sqliteStore` and `pgStore` add the two columns to an existing 0.3.x table on first use. `status()` reports `flagged` per queue; run entries carry `flagged`; `fail(feed, failure)` gets `failure.flag`, with `minutes`/`nextAt` 30 days out for a flagged or retired feed.
- `FetchError` keeps the system code: `dnsCode` for resolver errors, `causeCode` for connection/TLS errors.
- New exports: `CRAWL_FETCH_TIMEOUT_MS` (10 000), `CRAWL_BROWSER_TIMEOUT_MS` (30 000), `FLAG_FOR_MS`, `RETIRE_RECHECK_MS`, `failureClass(err)`, `flagPlan(err, opts)`.
- Backward compatibility: `flagFor: false` (per task or on `createDaemon`) restores the ladder, for uptime monitors and calls to your own services; `retireRecheck: false` restores retire-forever.
- Tests: the cadence dispatch test used the wall clock against fixed post dates and failed once the calendar moved on; it now runs on the fixed clock.
