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
}

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
}

export interface Item {
  key: string;
  url?: string;
  priority?: number;
  [k: string]: unknown;
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
  start(): Daemon;
  stop(opts?: { graceMs?: number }): Promise<void>;
  runNow(task: string, key?: string): Promise<unknown>;
  refreshItems(task: string): void;
  status(): Promise<Record<string, unknown>>;
  holdHost(host: string, forMs: Duration): void;
  fetch: Fetcher;
  store: Store;
}

export interface RunEntry {
  job: string;
  kind: 'job' | 'queue' | 'pipeline';
  key: string;
  ok: boolean;
  held: boolean;
  retired: boolean;
  ms: number;
  at: number;
  error: string | null;
}

export function createDaemon(opts?: {
  name?: string;
  store?: Store;
  concurrency?: number;
  tickMs?: number;
  jitter?: number;
  fetch?: Parameters<typeof createFetcher>[0] & { perHost?: number; delayMs?: number };
  fetcher?: Fetcher;
  log?: (...args: unknown[]) => void;
  onRun?: (entry: RunEntry, result?: unknown) => unknown;
  onStall?: (s: { job: string; lastOk: number | null; lastError: string | null }) => unknown;
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
  code: 'blocked' | 'robots' | 'status' | 'timeout' | 'too-large' | 'network';
  status?: number;
  url?: string;
  retryAfterMs?: number;
}

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
