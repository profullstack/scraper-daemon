export type Duration = number | string;

export interface Store {
  kind?: string;
  load(job: string): Promise<Map<string, RunRecord>>;
  save(job: string, rec: RunRecord): Promise<void>;
  forget(job: string, keys: string[]): Promise<void>;
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown): Promise<void>;
}

export interface RunRecord {
  key: string;
  lastTry: number | null;
  lastOk: number | null;
  nextAt: number | null;
  fails: number;
  lastError: string | null;
  /** 'slow' | 'broken': flagged by the crawler rule until nextAt (30 d); 'retired'; null: fine. Since 0.4.0. */
  flag?: Flag | null;
  /** When the flag was set (the failure time). */
  flaggedAt?: number | null;
}

export type Flag = 'slow' | 'broken' | 'retired';
export type FailureClass = 'slow' | 'broken' | null;

export interface Fetched {
  url: string;
  status: number;
  headers: Headers;
  text: string;
}

export interface Fetcher {
  text(url: string, opts?: { accept?: string; signal?: AbortSignal; headers?: Record<string, string> }): Promise<Fetched>;
  json<T = unknown>(url: string, opts?: { signal?: AbortSignal; headers?: Record<string, string> }): Promise<T>;
  allowed(url: string, signal?: AbortSignal): Promise<boolean>;
  check(url: string): Promise<URL>;
}

export interface Ctx {
  daemon: string;
  job: string;
  key: string | null;
  signal: AbortSignal;
  log: (...args: unknown[]) => void;
  fetch: Fetcher;
  holdHost(host: string, forMs: Duration): void;
  state: { get(key: string): Promise<unknown>; set(key: string, value: unknown): Promise<void> };
}

export interface RunResult {
  nextIn?: Duration;
  retire?: boolean;
  [k: string]: unknown;
}

export interface TaskOptions {
  name: string;
  every?: Duration;
  timeout?: Duration;
  backoff?: Duration[];
  staleAfter?: Duration;
  concurrency?: number;
  /**
   * How long a slow or broken target (timeout, DNS/TLS/connection error, 5xx) is left alone
   * instead of walking `backoff`. Default '30d' for queue and feeds, false for job and pipeline.
   * false restores the pre-0.4.0 ladder (uptime monitors, calls to your own services).
   */
  flagFor?: Duration | false;
  /** How often an item retired by `retireAfter` is rechecked. Default '30d'; false = retired for good (pre-0.4.0). */
  retireRecheck?: Duration | false;
  /** Override how a thrown error is classified (default failureClass). */
  classify?(err: unknown): FailureClass;
}

export interface Item {
  key: string;
  url?: string;
  priority?: number;
  [k: string]: unknown;
}

export type Tier = 'hot' | 'active' | 'slow' | 'dormant' | 'archived';

export interface CadenceOptions {
  floor?: number;
  ceiling?: number;
  archiveAfter?: number;
  revisitEvery?: number;
  quietMultiple?: number;
  windowStart?: number;
  windowReads?: number;
  windowCeiling?: number;
  idleReads?: number;
  sample?: number;
}

export interface Plan {
  /** Minutes until the next read. */
  minutes: number;
  tier: Tier;
  archived: boolean;
  /** Median minutes between posts, when there was a rhythm to read. */
  rhythm: number | null;
  silence: number | null;
  reason: string;
}

export interface FeedRunResult {
  /** The publish dates the document carried (any order, any parseable form). */
  dates?: Array<string | number | Date | null | undefined>;
  /** For an undated feed: whether its contents changed since the last read. */
  changed?: boolean;
  /** Override the cadence for this read. */
  nextIn?: Duration;
  [k: string]: unknown;
}

export interface FeedFailure {
  error: string;
  /** A 429: the host is held back and this read does not count against the feed. */
  held: boolean;
  fails: number | null;
  minutes: number;
  nextAt: number;
  /** `retireAfter` failures in a row (minutes is then the 30-day recheck). */
  retire: boolean;
  /** 'slow' | 'broken' (minutes is then 30 days), 'retired', or null (the ladder). Since 0.4.0. */
  flag: Flag | null;
}

export interface Daemon {
  job(spec: TaskOptions & { run(ctx: Ctx): Promise<RunResult | void> }): Daemon;
  queue<I extends Item = Item>(
    spec: TaskOptions & {
      items(ctx: Ctx): Promise<Array<I | string>>;
      run(item: I, ctx: Ctx): Promise<RunResult | void>;
      host?(item: I): string | null;
      itemsEvery?: Duration;
      retireAfter?: number;
    },
  ): Daemon;
  pipeline(
    spec: TaskOptions & {
      gate?(ctx: Ctx): Promise<{ token?: string } | null | false>;
      steps: Array<{ name: string; run(ctx: Ctx & { token: string; step: string; results: Record<string, unknown> }): Promise<unknown> }>;
    },
  ): Daemon;
  /** A crawl whose schedule lives in the site's database: tiered dispatch, cadence from publish dates. */
  feeds<F extends Record<string, unknown> = Record<string, unknown>>(
    spec: TaskOptions & {
      /** Due feeds of one tier, oldest due first, at most `limit`. */
      due(tier: Tier, limit: number, ctx: Ctx): Promise<F[]>;
      run(feed: F, ctx: Ctx): Promise<FeedRunResult | void>;
      /** Write the next read back: plan.nextAt (epoch ms), plan.minutes, plan.tier, plan.archived. */
      save(feed: F, plan: Plan & { nextAt: number }, result: FeedRunResult, ctx: Ctx): Promise<void>;
      fail?(feed: F, failure: FeedFailure, ctx: Ctx): Promise<void>;
      key?(feed: F): string;
      host?(feed: F): string | null;
      /** The feed's current interval in minutes, for the undated ladder (default feed.minutes). */
      previous?(feed: F): number | null;
      /** Failures in a row so far (default feed.fails). */
      fails?(feed: F): number;
      shares?: Partial<Record<Tier, number>>;
      cadence?: CadenceOptions;
      perHost?: number;
      retireAfter?: number;
    },
  ): Daemon;
  start(): Daemon;
  /** Resolves once the engine is connected (at once for the local engine). */
  ready(): Promise<void>;
  stop(opts?: { graceMs?: number }): Promise<void>;
  /** For a feeds task pass the feed row: one read now (a refresh button). */
  runNow(task: string, key?: string | Record<string, unknown>): Promise<unknown>;
  refreshItems(task: string): void;
  status(): Promise<Record<string, unknown>>;
  holdHost(host: string, forMs: Duration): void;
  fetch: Fetcher;
  readonly store: Store | null;
}

export function redisStore(client: unknown, opts?: { prefix?: string }): Store;
export function jobIdFor(task: string, key: string): string;
export function withTimeout<T, F>(p: Promise<T> | T, ms: number, fallback: F): Promise<T | F>;

export interface RunEntry {
  job: string;
  kind: 'job' | 'queue' | 'pipeline';
  key: string;
  ok: boolean;
  held: boolean;
  retired: boolean;
  flagged: Flag | null;
  ms: number;
  at: number;
  error: string | null;
}

export function createDaemon(opts?: {
  name?: string;
  /** BullMQ engine: a Redis URL or a ready ioredis client. Without it the local (in-process) engine runs. */
  redis?: string | unknown;
  /** BullMQ queue name and Redis key prefix (default: name). */
  queueName?: string;
  /** Called after three missed Redis pings in a row; default logs and exits so the supervisor restarts. */
  onRedisDown?: () => void;
  /** Run records and task state. Default: Redis (beside the queue) with `redis`, memory without. */
  store?: Store;
  concurrency?: number;
  tickMs?: number;
  jitter?: number;
  fetch?: Parameters<typeof createFetcher>[0] & { perHost?: number; delayMs?: number };
  fetcher?: Fetcher;
  log?: (...args: unknown[]) => void;
  onRun?: (entry: RunEntry, result?: unknown) => unknown;
  onStall?: (s: { job: string; lastOk: number | null; lastError: string | null }) => unknown;
  /** Default flagFor for queue and feeds tasks ('30d'). */
  flagFor?: Duration | false;
  /** Default retireRecheck ('30d'). */
  retireRecheck?: Duration | false;
  now?: () => number;
}): Daemon;

export function createFetcher(opts?: {
  userAgent?: string;
  timeoutMs?: number;
  maxBytes?: number;
  maxRedirects?: number;
  robots?: boolean;
  allowPrivate?: boolean;
  gate?: ReturnType<typeof hostGate>;
  robotsTtlMs?: number;
  fetchImpl?: typeof fetch;
  lookup?: (host: string) => Promise<Array<{ address: string }>>;
  now?: () => number;
}): Fetcher;

export class FetchError extends Error {
  constructor(code: FetchError['code'], message: string, extra?: Record<string, unknown>);
  code: 'blocked' | 'robots' | 'status' | 'timeout' | 'too-large' | 'network';
  status?: number;
  url?: string;
  retryAfterMs?: number;
  /** DNS lookup error code (ENOTFOUND, EAI_AGAIN) for a 'network' error from the resolver. */
  dnsCode?: string | null;
  /** System error code under a 'network' error (ECONNREFUSED, CERT_HAS_EXPIRED, ...). */
  causeCode?: string;
}

/** 10_000: the fleet's budget for one plain HTTP fetch (and createFetcher's default timeoutMs). */
export const CRAWL_FETCH_TIMEOUT_MS: number;
/** 30_000: the fleet's navigation budget for a headless-browser render. */
export const CRAWL_BROWSER_TIMEOUT_MS: number;
/** 30 days in ms: how long a slow or broken target is left alone. */
export const FLAG_FOR_MS: number;
/** 30 days in ms: how often a retired item is rechecked. */
export const RETIRE_RECHECK_MS: number;
/** 'slow' (timeouts), 'broken' (DNS/TLS/connection errors, 5xx) or null (keep the ladder). */
export function failureClass(err: unknown): FailureClass;
/** The schedule after a failure, as a pure function. nextIn is ms (Infinity: never). */
export function flagPlan(
  err: unknown,
  opts?: { fails?: number; retireAfter?: number | null; flagFor?: number | false; retireRecheck?: number | false; ladderMs?: number; classify?: (err: unknown) => FailureClass },
): { flag: Flag | null; nextIn: number };

export function hostGate(opts?: { concurrency?: number; delayMs?: number }): {
  run<T>(host: string, fn: () => Promise<T>): Promise<T>;
  busy(): Array<{ host: string; active: number; waiting: number }>;
};
export function robotsAllows(robots: string | null | undefined, path: string, agent?: string): boolean;
export function isPublicAddress(ip: string): boolean;
export function retryAfter(value: string | null | undefined, now?: () => number): number | null;
export function ms(value: Duration): number;
export function memoryStore(): Store;
export function sqliteStore(db: unknown, opts?: { prefix?: string }): Store;
export function pgStore(query: (text: string, params: unknown[]) => Promise<unknown>, opts?: { prefix?: string }): Store;
export function statusHandler(daemon: Daemon): (req?: Request) => Promise<Response>;

export const HOUR: number;
export const DAY: number;
export const WEEK: number;
export const TIERS: readonly Tier[];
export const CADENCE_DEFAULTS: Readonly<Required<CadenceOptions>>;
export const DEFAULT_SHARES: Readonly<Record<Tier, number>>;
export function planNext(dates: Array<unknown>, opts?: CadenceOptions & { now?: number }): Plan | null;
export function planUndated(input: { changed: boolean; previous?: number | null }, opts?: CadenceOptions): Plan;
export function publishTimes(dates: Array<unknown>, now: number, opts?: { sample?: number }): number[];
export function tierFor(minutes: number, opts?: { ceiling?: number }): Tier;
export function tierSql(column?: string, opts?: { ceiling?: number }): string;
export function quotas(room: number, shares?: Partial<Record<Tier, number>>): Array<[Tier, number]>;
