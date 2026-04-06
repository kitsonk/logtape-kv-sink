import { assert } from "jsr:@std/assert@1/assert";
import { assertEquals } from "jsr:@std/assert@1/equals";
import { delay } from "jsr:@std/async@1/delay";

import type { BlobMeta } from "@kitsonk/kv-toolbox/blob";

import { getKvSink, listLogRecords } from "./main.ts";

let kv: { close(): void } | undefined;

async function setup(): Promise<Deno.Kv> {
  return kv = await Deno.openKv(":memory:");
}

function teardown(): void {
  assert(kv);
  kv.close();
}

Deno.test({
  name: "getKvSink should create a sink that can write log records to Deno.Kv",
  async fn() {
    const kv = await setup();
    try {
      const sink = getKvSink(kv);
      sink({
        timestamp: Date.now(),
        level: "info",
        category: ["test"],
        message: ["Hello, world!"],
        rawMessage: "Hello, world!",
        properties: {},
      });
      sink({
        timestamp: Date.now(),
        level: "info",
        category: ["test"],
        message: ["Hello, world!"],
        rawMessage: "Hello, world!",
        properties: {},
      });
      await delay(100); // Wait for the microtask queue to flush
      const maybeLogEntry = await kv.get<BlobMeta>(["__logtape__", "entries", 0n, "__kv_toolbox_meta__"]);
      assert(maybeLogEntry.value);
      assertEquals(maybeLogEntry.value.kind, "buffer");
      const maybeUid = await kv.get<Deno.KvU64>(["__logtape__", "uid"]);
      assert(maybeUid.value);
      assertEquals(maybeUid.value.value, 2n);
      const maybeLength = await kv.get<Deno.KvU64>(["__logtape__", "length"]);
      assert(maybeLength.value);
      assertEquals(maybeLength.value.value, 2n);
    } finally {
      teardown();
    }
  },
});

Deno.test({
  name: "getKvSink should purge old log records when maxLength is exceeded",
  async fn() {
    const kv = await setup();
    try {
      const sink = getKvSink(kv, { maxLength: 10 });
      for (let i = 0; i < 15; i++) {
        sink({
          timestamp: Date.now(),
          level: "info",
          category: ["test"],
          message: ["Hello, world!"],
          rawMessage: "Hello, world!",
          properties: {},
        });
      }
      await delay(100); // Wait for the microtask queue to flush and pruning to complete
      const maybeUid = await kv.get<Deno.KvU64>(["__logtape__", "uid"]);
      assert(maybeUid.value);
      assertEquals(maybeUid.value.value, 15n);
      const maybeLength = await kv.get<Deno.KvU64>(["__logtape__", "length"]);
      assert(maybeLength.value);
      assertEquals(maybeLength.value.value, 10n);
    } finally {
      teardown();
    }
  },
});

Deno.test({
  name: "listLogRecords should return an async iterator that can read log records from Deno.Kv",
  async fn() {
    const kv = await setup();
    try {
      const sink = getKvSink(kv);
      for (let i = 0; i < 5; i++) {
        sink({
          timestamp: Date.now(),
          level: "info",
          category: ["test"],
          message: [`Hello, world! ${i}`],
          rawMessage: `Hello, world! ${i}`,
          properties: {},
        });
      }
      await delay(100); // Wait for the microtask queue to flush
      const logRecords = await Array.fromAsync(listLogRecords(kv));
      assertEquals(logRecords.length, 5);
      let counter = 5;
      for (let i = 0; i < 5; i++) {
        counter--;
        assertEquals(logRecords[i].message[0], `Hello, world! ${counter}`);
      }
    } finally {
      teardown();
    }
  },
});
