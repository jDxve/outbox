# outbox

[![CI](https://github.com/jDxve/outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/jDxve/outbox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A durable, per-group ordered offline queue for Flutter. Writes are persisted to SQLite before your code sees them return, then drained in order as connectivity allows — with exponential backoff, idempotent retries, and a dead letter queue for what can never succeed.

Client agnostic — supply the function that talks to your backend, and Dio, `http`, Retrofit, GraphQL, or a raw socket all work.

## Features

- **Per-group ordering** — tasks sharing a `groupKey` run in strict insertion order; unrelated tasks never block each other.
- **Dead letter queue** — permanent failures are set aside instead of retried, with an API to inspect, retry, or purge them.
- **Idempotent by construction** — every task gets a stable id at enqueue time, safe to use as a server-side dedupe key.
- **Crash-safe durability** — backed by SQLite; a task interrupted mid-send is recovered on the next launch.
- **Pluggable storage and connectivity** — swap in your own `QueueStorage` or reachability signal.

## Installation

```yaml
dependencies:
  outbox: ^0.1.0
```

On Windows and Linux, `sqflite` requires the FFI factory, set once at startup:

```dart
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

sqfliteFfiInit();
databaseFactory = databaseFactoryFfi;
```

## Quick start

```dart
final queue = await Outbox.create(
  executor: (task) async {
    final response = await dio.post(
      '/api/${task.action}',
      data: task.payload,
      options: Options(headers: {'Idempotency-Key': task.idempotencyKey}),
    );
    if (response.statusCode! >= 400 && response.statusCode! < 500) {
      throw PermanentTaskFailure('Rejected: ${response.statusCode}');
    }
  },
);

await queue.add(
  action: 'inventory/adjust',
  payload: {'sku': '12345', 'quantity': 10},
  groupKey: '12345',
);

// ...

await queue.dispose();
```

`Outbox.create` is async: it opens the database, recovers anything left in flight by a previous crash, and starts draining before returning — so a call to `add` right after can never race an uninitialized queue.

## Failure handling

| Behavior | Result |
|---|---|
| Executor throws | Task is retried per `RetryPolicy` |
| Executor throws `PermanentTaskFailure` | Task dead letters immediately, no retries consumed |

```dart
retryPolicy: const RetryPolicy(
  maxAttempts: 5,
  initialDelay: Duration(seconds: 2),
  multiplier: 2,
  maxDelay: Duration(minutes: 30),
  jitter: 0.25,
),
```

`jitter` randomizes each delay to avoid a thundering herd when many devices reconnect at once.

## Ordering

Tasks sharing a `groupKey` run in strict insertion order; a task backing off only holds back others in its own group. Use the identity of the thing being mutated — a SKU, an order id, a document id.

By default a dead lettered task also blocks its group (`haltGroupOnDeadLetter: true`), so an update is never applied to a record whose create was rejected.

## Idempotency

`task.id` is generated once at enqueue and survives retries and restarts — safe to send as `task.idempotencyKey`. The queue does not attach it for you; include it in your executor's request.

Delivery is at-least-once: if the app dies between a successful server write and the local delete, the task retries on next launch. The idempotency key is what makes that retry safe.

## Watching state

```dart
StreamBuilder<QueueSnapshot>(
  stream: queue.states,
  builder: (context, snapshot) {
    final state = snapshot.data;
    if (state == null || state.isIdle) return const SizedBox.shrink();
    return Badge(label: Text('${state.pending}'));
  },
);
```

## Dead letters

```dart
final failed = await queue.deadLetters();
await queue.retryDeadLetter(failed.first.id);
await queue.purgeDeadLetters();
```

## Connectivity

By default the queue listens to `connectivity_plus`, which reports the network interface, not server reachability. Pass your own signal if you have a real health check:

```dart
connectivity: ConnectivityWatcher(stream: myReachabilityStream),
```

## Testing

`InMemoryQueueStorage` mirrors the same semantics with no `sqflite` binding required:

```dart
final queue = await Outbox.create(
  storage: InMemoryQueueStorage(),
  executor: (task) async => sent.add(task.action),
  connectivity: ConnectivityWatcher(stream: controller.stream),
  autoStart: false,
);
```

`await queue.processQueue()` resolves once the drain (and any pass queued mid-run) completes, so tests rarely need arbitrary delays.

## Custom storage

Implement `QueueStorage` to back the queue with Drift, Isar, or an existing database. `SqliteQueueStorage.withDatabase(db)` reuses a handle you already opened; the queue only touches its own `sync_queue` table.

## Contributing

Issues and pull requests are welcome at [github.com/jDxve/outbox](https://github.com/jDxve/outbox).

## License

[MIT](LICENSE)
