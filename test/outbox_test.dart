import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:outbox/outbox.dart';

void main() {
  late InMemoryQueueStorage storage;
  late StreamController<bool> connectivity;
  late List<String> sent;

  setUp(() {
    storage = InMemoryQueueStorage();
    connectivity = StreamController<bool>.broadcast();
    sent = [];
  });

  tearDown(() => connectivity.close());

  // Polls instead of a fixed sleep so timing-based tests don't flake on a
  // slower or more loaded CI runner.
  Future<void> pumpUntil(
    bool Function() condition, {
    Duration timeout = const Duration(seconds: 20),
  }) async {
    final deadline = DateTime.now().add(timeout);
    while (!condition()) {
      if (DateTime.now().isAfter(deadline)) {
        fail('Condition not met within $timeout');
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }

  Future<Outbox> buildQueue({
    required TaskExecutor executor,
    RetryPolicy policy = const RetryPolicy(),
    bool autoStart = true,
  }) {
    return Outbox.create(
      storage: storage,
      executor: executor,
      retryPolicy: policy,
      connectivity: ConnectivityWatcher(stream: connectivity.stream),
      autoStart: autoStart,
    );
  }

  test('drains tasks in insertion order', () async {
    final queue = await buildQueue(
      executor: (task) async => sent.add(task.action),
    );

    await queue.add(action: 'a', payload: const {});
    await queue.add(action: 'b', payload: const {});
    await queue.add(action: 'c', payload: const {});
    await queue.processQueue();

    expect(sent, ['a', 'b', 'c']);
    expect(await queue.pendingCount(), 0);
    await queue.dispose();
  });

  test('retries transient failures then succeeds', () async {
    var attempts = 0;
    final queue = await buildQueue(
      policy: const RetryPolicy(
        initialDelay: Duration(milliseconds: 10),
        jitter: 0,
      ),
      executor: (task) async {
        attempts++;
        if (attempts < 3) throw Exception('network down');
        sent.add(task.action);
      },
    );

    await queue.add(action: 'a', payload: const {});
    await pumpUntil(() => sent.contains('a'));

    expect(attempts, 3);
    expect(sent, ['a']);
    expect(await queue.pendingCount(), 0);
    await queue.dispose();
  });

  test('dead letters after maxAttempts', () async {
    var attempts = 0;
    final queue = await buildQueue(
      policy: const RetryPolicy(
        maxAttempts: 3,
        initialDelay: Duration(milliseconds: 5),
        jitter: 0,
      ),
      executor: (_) async {
        attempts++;
        throw Exception('still down');
      },
    );

    await queue.add(action: 'a', payload: const {});
    await pumpUntil(() => attempts >= 3);

    expect(attempts, 3);
    expect(await queue.deadLetterCount(), 1);
    await queue.dispose();
  });

  test('PermanentTaskFailure dead letters on the first attempt', () async {
    var attempts = 0;
    final queue = await buildQueue(
      executor: (_) async {
        attempts++;
        throw const PermanentTaskFailure('422 unprocessable');
      },
    );

    await queue.add(action: 'a', payload: const {});
    await queue.processQueue();

    expect(attempts, 1);
    final dead = await queue.deadLetters();
    expect(dead.single.lastError, contains('422'));
    await queue.dispose();
  });

  test('a failing task blocks only its own group', () async {
    final queue = await buildQueue(
      policy: const RetryPolicy(
        initialDelay: Duration(seconds: 30),
        jitter: 0,
      ),
      executor: (task) async {
        if (task.action == 'sku-a-1') throw Exception('down');
        sent.add(task.action);
      },
      autoStart: false,
    );

    await queue.add(action: 'sku-a-1', payload: const {}, groupKey: 'a');
    await queue.add(action: 'sku-a-2', payload: const {}, groupKey: 'a');
    await queue.add(action: 'sku-b-1', payload: const {}, groupKey: 'b');
    await queue.processQueue();

    expect(sent, ['sku-b-1']);
    await queue.dispose();
  });

  test('recovers tasks interrupted mid flight', () async {
    await storage.enqueue(SyncTask(
      id: 'stuck',
      action: 'a',
      payload: const {},
      createdAt: DateTime.now(),
      status: SyncTaskStatus.processing,
    ));

    final queue = await buildQueue(
      executor: (task) async => sent.add(task.id),
      autoStart: false,
    );
    await queue.processQueue();

    expect(sent, ['stuck']);
    await queue.dispose();
  });

  test('retryDeadLetter puts a task back in play', () async {
    var shouldFail = true;
    final queue = await buildQueue(
      policy: RetryPolicy.noRetry,
      executor: (task) async {
        if (shouldFail) throw Exception('down');
        sent.add(task.action);
      },
    );

    final id = await queue.add(action: 'a', payload: const {});
    await queue.processQueue();
    expect(await queue.deadLetterCount(), 1);

    shouldFail = false;
    await queue.retryDeadLetter(id);
    await queue.processQueue();

    expect(sent, ['a']);
    expect(await queue.deadLetterCount(), 0);
    await queue.dispose();
  });

  test('backoff grows exponentially and is capped', () {
    const policy = RetryPolicy(
      initialDelay: Duration(seconds: 1),
      multiplier: 2,
      maxDelay: Duration(seconds: 10),
      jitter: 0,
    );

    expect(policy.delayAfter(1).inSeconds, 1);
    expect(policy.delayAfter(2).inSeconds, 2);
    expect(policy.delayAfter(3).inSeconds, 4);
    expect(policy.delayAfter(9).inSeconds, 10);
  });
}
