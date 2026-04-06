/**
 * A log sink for LogTape and Deno KV.
 *
 * [LogTape](https://logtape.org/) is unobstrusive logging for modern JavaScript (and TypeScript).
 * [Deno KV](https://docs.deno.com/deploy/reference/deno_kv/) is a Key Value database that is built into the Deno CLI and
 * Deno Deploy. A sink is a destination for log messages. When using `logtape-kv-sink` the log messages will be stored in
 * Deno KV.
 *
 * Log messages could be large, and therefore this library stores serialized versions of the log messages as an arbitrary
 * sized blob within Deno KV via the [@kitsonk/kv-toolbox](https://kview.deno.dev/kv-toolbox) library.
 *
 * # Usage
 *
 * Basic usage is to provide an instance of `Deno.Kv` to the `getKvSink()` function as part of the configuration of
 * LogTape:
 *
 * ```ts
 * import { configure } from "@logtape/logtape";
 * import { getKvSink } from "@kitsonk/logtape-kv-sink";
 *
 * const kv = await Deno.openKv();
 *
 * await configure({
 *   sinks: {
 *     kv: getKvSink(kv),
 *   },
 *   loggers: [
 *     { category: [], sinks: ["kv"], lowestLevel: "debug" },
 *   ],
 * });
 * ```
 *
 * The instance of `Deno.Kv` and also be a `Promise<Deno.Kv>`. By default, the sink will maintain the most recent 5000 log
 * entries, purging any older entries. This can be adjusted by setting the `maxLength` option when getting the sink.
 *
 * ## Retrieving log records
 *
 * To retrieve the log records, for example if you wish to display them to a user, you can use the `listLogRecords`
 * function:
 *
 * ```ts
 * import { listLogRecords } from "@kitsonk/logtape-kv-sink";
 *
 * const kv = await Deno.openKv();
 *
 * const logRecords = listLogRecords(kv);
 * for await (const record of logRecords) {
 *   console.log(record);
 * }
 * ```
 *
 * By default, the most recent 10 records will be returned. These can be adjusted via the options passed to
 * `listLogRecords()`. The iterator returned from `listLogRecords()` contains the `.cursor` property, which can be used to
 * "page" through the records.
 *
 * @module
 */

import type { LogRecord, Sink } from "@logtape/logtape";
import { batchedAtomic } from "@kitsonk/kv-toolbox/batched_atomic";
import { query } from "@kitsonk/kv-toolbox/query";
import { deserialize, serialize } from "node:v8";

/**
 * Options for the KV sink.
 */
export interface KvSinkOptions {
  /**
   * The key prefix to use for the log entries. This defaults to `["__logtape__"]`.
   */
  prefix?: Deno.KvKey;
  /**
   * The maximum length of the log. When the log exceeds this length, the oldest entries will be pruned. This defaults
   * to `5000n`.
   */
  maxLength?: bigint | number;
}

/**
 * Options for listing log records from a Deno KV store.
 */
export interface ListLogRecordsOptions {
  /**
   * The maximum number of log entries to return. If not provided, defaults to `10`.
   */
  limit?: number;
  /**
   * The cursor to start retrieving log entries from. If not provided, starts from the beginning.
   */
  cursor?: string;
  /**
   * Whether to reverse the order of the returned key-value pairs. If not specified, the order will be ascending from
   * the start of the range as per the lexicographical ordering of the keys. If `true`, the order will be descending
   * from the end of the range.
   *
   * The default value is `true`.
   */
  reverse?: boolean;
  /**
   * The key prefix to use for the log entries. This defaults to `{ prefix: ["__logtape__"] }`.
   */
  selector?: Deno.KvListSelector;
}

const DEFAULT_PREFIX: Deno.KvKey = ["__logtape__"];
const DEFAULT_MAX_LENGTH = 5000n;

function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

/**
 * Returns a sink that writes log entries to a Deno KV store.
 *
 * @example
 *
 * ```ts
 * import { configure } from "@logtape/logtape";
 * import { getKvSink } from "@kitsonk/logtape-kv-sink";
 *
 * const kv = await Deno.openKv();
 *
 * await configure({
 *   sinks: {
 *     kv: getKvSink(kv),
 *   },
 *   loggers: [
 *     { category: [], sinks: ["kv"], lowestLevel: "debug" },
 *   ],
 * });
 * ```
 *
 * @param store - The Deno.Kv instance to use for the KV sink.
 * @param options - The options for the KV sink.
 * @returns A new KV sink.
 */
export function getKvSink(store: Deno.Kv | Promise<Deno.Kv>, options: KvSinkOptions = {}): Sink {
  const { prefix = DEFAULT_PREFIX } = options;
  const maxLength = options.maxLength !== undefined ? BigInt(options.maxLength) : DEFAULT_MAX_LENGTH;

  const uidKey: Deno.KvKey = [...prefix, "uid"];
  const lengthKey: Deno.KvKey = [...prefix, "length"];
  const entriesPrefix: Deno.KvKey = [...prefix, "entries"];
  const kvPromise = isPromiseLike(store) ? store : Promise.resolve(store);

  let isPruning = false;

  async function pruneEntries(): Promise<void> {
    if (isPruning) return;
    isPruning = true;
    try {
      const [uidResult, lengthResult] = await (await kvPromise).getMany<[Deno.KvU64, Deno.KvU64]>([uidKey, lengthKey]);
      const uid = uidResult.value?.value ?? 0n;
      const length = lengthResult.value?.value ?? 0n;
      if (length > maxLength) {
        const entriesToDelete = length - maxLength;
        const startUid = uid - length;
        const op = batchedAtomic(await kvPromise)
          .check(uidResult)
          .check(lengthResult)
          .set(lengthKey, new Deno.KvU64(length - entriesToDelete));
        for (let i = startUid; i <= startUid + entriesToDelete - 1n; i++) {
          op.deleteBlob([...entriesPrefix, i]);
        }
        await op.commit();
      }
    } catch {
      // just ignore errors during pruning, we'll try again on the next write
    } finally {
      isPruning = false;
    }
  }

  async function getUidLength(): Promise<{ uid: bigint; length: bigint }> {
    const [uidResult, lengthResult] = await (await kvPromise).getMany<[Deno.KvU64, Deno.KvU64]>([uidKey, lengthKey]);
    return {
      uid: uidResult.value?.value ?? 0n,
      length: lengthResult.value?.value ?? 0n,
    };
  }

  let isDraining = false;
  let pendingEntries: LogRecord[] = [];

  async function enqueueRecord(record: LogRecord): Promise<void> {
    pendingEntries.push(record);
    if (isDraining) return;
    isDraining = true;
    try {
      while (pendingEntries.length > 0) {
        const toCommit = pendingEntries;
        pendingEntries = [];
        const { uid, length } = await getUidLength();
        const op = batchedAtomic(await kvPromise);
        for (const [idx, record] of toCommit.entries()) {
          const id = uid + BigInt(idx);
          op.setBlob([...entriesPrefix, id], serialize(record));
        }
        op.sum(uidKey, BigInt(toCommit.length));
        op.sum(lengthKey, BigInt(toCommit.length));
        await op.commit();
        if (length + BigInt(toCommit.length) > maxLength) {
          queueMicrotask(pruneEntries);
        }
      }
    } catch {
      // just ignore errors during draining, we'll try again on the next write
    } finally {
      isDraining = false;
    }
  }

  return (record: LogRecord) => {
    enqueueRecord(record).catch(() => {
      // just ignore errors during enqueueing, we'll try again on the next write
    });
  };
}

const AsyncIterator = Object.getPrototypeOf(async function* () {}).constructor;

/**
 * An async iterator that retrieves log records from a Deno KV store.
 */
export class AsyncLogRecordIterator extends AsyncIterator implements AsyncIterableIterator<LogRecord> {
  #storePromise: Promise<Deno.Kv>;
  #limit: number;
  #listCursor?: string;
  #reverse: boolean;
  #selector: Deno.KvListSelector;
  #cursor?: string;
  #iterator?: Deno.KvListIterator<Uint8Array<ArrayBufferLike>>;

  /**
   * Returns the cursor of the current position in the iteration. This cursor can be used to resume the iteration from
   * the current position in the future by passing it to the `cursor` option of the `listLogRecords` function.
   */
  get cursor(): string {
    if (!this.#cursor) {
      throw new Error("Cannot get cursor before first iteration");
    }
    return this.#cursor;
  }

  constructor(
    store: Deno.Kv | Promise<Deno.Kv>,
    { limit = 10, cursor, reverse = true, selector = { prefix: DEFAULT_PREFIX } }: ListLogRecordsOptions,
  ) {
    super();
    this.#storePromise = isPromiseLike(store) ? store : Promise.resolve(store);
    this.#limit = limit;
    this.#listCursor = cursor;
    this.#reverse = reverse;
    this.#selector = selector;
  }

  async next(): Promise<IteratorResult<LogRecord, undefined>> {
    if (!this.#iterator) {
      const kv = await this.#storePromise;
      this.#iterator = query(kv, this.#selector, {
        cursor: this.#listCursor,
        reverse: this.#reverse,
        limit: this.#limit,
        bytes: true,
      }).get();
    }
    for await (const entry of this.#iterator) {
      this.#cursor = this.#iterator.cursor;
      if (entry.value) {
        return { value: deserialize(entry.value), done: false };
      }
    }
    return { value: undefined, done: true };
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<LogRecord> {
    return this;
  }
}

/**
 * Lists log records from a Deno KV store.
 *
 * @example
 *
 * ```ts
 * import { listLogRecords } from "@kitsonk/logtape-kv-sink";
 *
 * const kv = await Deno.openKv();
 *
 * const logRecords = listLogRecords(kv, { limit: 20 });
 * for await (const record of logRecords) {
 *   console.log(record);
 * }
 * ```
 *
 * @param store - The Deno.Kv instance to use for the KV sink.
 * @param options - Options for listing log records.
 * @returns An async iterator for the log records.
 */
export function listLogRecords(
  store: Deno.Kv | Promise<Deno.Kv>,
  options: ListLogRecordsOptions = {},
): AsyncLogRecordIterator {
  return new AsyncLogRecordIterator(store, options);
}
