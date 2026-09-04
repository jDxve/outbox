# outbox

A durable, per-group ordered offline outbox for Flutter apps that have to keep working with no signal.

There are a lot of "offline queue" packages on pub.dev. Most of them handle the easy 80%: persist a request, retry it with backoff when the network comes back. `outbox` exists for the other 20%, the part that actually breaks production apps:

- **Per-group ordering.** A create and its follow-up update must run in that order, even under retry and even across app restarts. Tasks with no relationship to each other should never wait on one another.
- **A real dead letter queue.** A task that can never succeed (a 422, a validation error) should stop retrying immediately, get set aside for a human, and — critically — stop the rest of its group from applying an update to a record that was never created.
- **Idempotency by construction.** Every task gets a stable id at enqueue time that survives crashes and restarts, so at-least-once delivery is safe to build a server-side dedupe check against.
- **Crash-safe durability.** Writes land in SQLite before your code sees them return. A task interrupted mid-send is recovered on the next launch, not lost or double-applied silently.

Client agnostic: you supply the function that talks to your backend, so Dio, http, Retrofit, GraphQL or a raw socket all work.

## Install

```yaml
dependencies:
  outbox: ^0.1.0
```

On Windows and Linux, sqflite needs the FFI factory. Set it once before creating the queue:

```dart
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

sqfliteFfiInit();
databaseFactory = databaseFactoryFfi;
```

## Usage

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
```

`create` is async on purpose. It opens the database, resets anything left in flight by a previous crash, and starts draining before it hands you the queue, so an `add` immediately after cannot land while storage is still null.

Call `dispose()` when you tear the queue down. It cancels the connectivity subscription and the retry timer; anything still pending stays on disk.

### Failures

Throwing from your executor means retry. Throwing `PermanentTaskFailure` means give up now.

That distinction matters more than it looks. A malformed payload that keeps retrying will exhaust its attempts and, worse, hold up every task behind it in its group. Map your 4xx responses to `PermanentTaskFailure` and they dead letter on the first try instead.

Retries follow `RetryPolicy`, which counts total attempts rather than retries after the first:

```dart
retryPolicy: const RetryPolicy(
  maxAttempts: 5,
  initialDelay: Duration(seconds: 2),
  multiplier: 2,
  maxDelay: Duration(minutes: 30),
  jitter: 0.25,
),
```

Jitter matters for a fleet. Twenty handhelds that lost wifi in the same dead spot will reconnect together, and without jitter they hit your API in lockstep.

### Ordering

This is the feature most competing packages skip. Tasks with the same `groupKey` run in strict insertion order, and a task that is backing off holds back later tasks in its own group only. Tasks with no group have no ordering constraint at all.

Use the identity of the thing being mutated: a SKU, an order id, a document id. Without groups a single stuck task blocks the whole queue; with groups the rest of the floor keeps working.

By default a dead lettered task also blocks its group (`haltGroupOnDeadLetter`), so an update never gets applied to a record whose create was rejected.

### Idempotency

`task.id` is generated once at enqueue and survives retries and restarts, so it is safe to send as an idempotency key. The queue does not attach it for you; put it on the request in your executor, as above.

At least once delivery is the guarantee. If the app dies between a successful write on the server and the local delete, that task is retried on the next launch. The idempotency key is what makes the second delivery harmless, so the server side check is not optional.

### Watching state

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

### Dead letters

```dart
final failed = await queue.deadLetters();
await queue.retryDeadLetter(failed.first.id);
await queue.purgeDeadLetters();
```

Give operations staff a screen for this. A task that dead letters silently is a stock adjustment nobody knows was lost.

## Connectivity

By default the queue listens to `connectivity_plus`, which reports the network interface rather than whether your server is reachable. Captive portal wifi looks online. Treat it as a hint about when to try, not a promise, and pass your own signal if you have a real reachability check:

```dart
connectivity: ConnectivityWatcher(stream: myReachabilityStream),
```

## Testing

`InMemoryQueueStorage` implements the same semantics with no sqflite binding:

```dart
final queue = await Outbox.create(
  storage: InMemoryQueueStorage(),
  executor: (task) async => sent.add(task.action),
  connectivity: ConnectivityWatcher(stream: controller.stream),
  autoStart: false,
);
```

`await queue.processQueue()` completes when the drain is done, including any pass queued while it was running, so tests do not need arbitrary delays except when exercising backoff.

## Custom storage

Implement `QueueStorage` to back the queue with drift, Isar or your existing database. `SqliteQueueStorage.withDatabase(db)` reuses a handle you already opened; the queue only touches its own `sync_queue` table.

## License

MIT
