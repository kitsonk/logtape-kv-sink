# logtape-kv-sink

A log sink for LogTape and Deno KV.

[LogTape](https://logtape.org/) is unobstrusive logging for modern JavaScript (and TypeScript).
[Deno KV](https://docs.deno.com/deploy/reference/deno_kv/) is a Key Value database that is built into the Deno CLI and
Deno Deploy. A sink is a destination for log messages. When using `logtape-kv-sink` the log messages will be stored in
Deno KV.

Log messages could be large, and therefore this library stores serialized versions of the log messages as an arbitrary
sized blob within Deno KV via the [@kitsonk/kv-toolbox](https://kview.deno.dev/kv-toolbox) library.

# Usage

Basic usage is to provide an instance of `Deno.Kv` to the `getKvSink()` function as part of the configuration of
LogTape:

```ts
import { configure } from "@logtape/logtape";
import { getKvSink } from "@kitsonk/logtape-kv-sink";

const kv = await Deno.openKv();

await configure({
  sinks: {
    kv: getKvSink(kv),
  },
  loggers: [
    { category: [], sinks: ["kv"], lowestLevel: "debug" },
  ],
});
```

The instance of `Deno.Kv` and also be a `Promise<Deno.Kv>`. By default, the sink will maintain the most recent 5000 log
entries, purging any older entries. This can be adjusted by setting the `maxLength` option when getting the sink.

## Retrieving log records

To retrieve the log records, for example if you wish to display them to a user, you can use the `listLogRecords`
function:

```ts
import { listLogRecords } from "@kitsonk/logtape-kv-sink";

const kv = await Deno.openKv();

const logRecords = listLogRecords(kv);
for await (const record of logRecords) {
  console.log(record);
}
```

Be default, the most recent 10 records will be returned. These can be adjusted via the options passed to
`listLogRecords()`. The iterator returned from `listLogRecords()` contains the `.cursor` property, which can be used to
"page" through the records.
