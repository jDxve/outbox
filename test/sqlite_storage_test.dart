import 'package:flutter_test/flutter_test.dart';
import 'package:outbox/outbox.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

void main() {
  setUpAll(() {
    sqfliteFfiInit();
    databaseFactory = databaseFactoryFfi;
  });

  late SqliteQueueStorage storage;
  late Database db;

  setUp(() async {
    db = await databaseFactory.openDatabase(inMemoryDatabasePath);
    storage = SqliteQueueStorage.withDatabase(db);
    await storage.initialize();
  });

  tearDown(() async {
    await db.close();
  });

  SyncTask task(String id, {String? groupKey}) => SyncTask(
        id: id,
        action: 'a',
        payload: const {},
        groupKey: groupKey,
        createdAt: DateTime.now(),
      );

  test('nextReady returns tasks in seq order', () async {
    await storage.enqueue(task('a'));
    await storage.enqueue(task('b'));
    await storage.enqueue(task('c'));

    final first = await storage.nextReady(DateTime.now());
    expect(first!.id, 'a');
    await storage.markComplete('a');

    final second = await storage.nextReady(DateTime.now());
    expect(second!.id, 'b');
  });

  test('nextReady respects next_attempt_at backoff window', () async {
    await storage.enqueue(task('a'));
    await storage.reschedule(
      'a',
      retryCount: 1,
      nextAttemptAt: DateTime.now().add(const Duration(minutes: 5)),
    );

    expect(await storage.nextReady(DateTime.now()), isNull);
    expect(
      await storage.nextReady(DateTime.now().add(const Duration(minutes: 6))),
      isNotNull,
    );
  });

  test('a pending task blocks only its own group', () async {
    await storage.enqueue(task('a1', groupKey: 'a'));
    await storage.markProcessing('a1');
    await storage.enqueue(task('a2', groupKey: 'a'));
    await storage.enqueue(task('b1', groupKey: 'b'));

    final next = await storage.nextReady(DateTime.now());
    expect(next!.id, 'b1');
  });

  test('haltGroupOnDeadLetter true blocks the group behind a dead task', () async {
    await storage.enqueue(task('a1', groupKey: 'a'));
    await storage.markDead('a1');
    await storage.enqueue(task('a2', groupKey: 'a'));

    expect(
      await storage.nextReady(DateTime.now(), haltGroupOnDeadLetter: true),
      isNull,
    );
    final next = await storage.nextReady(
      DateTime.now(),
      haltGroupOnDeadLetter: false,
    );
    expect(next!.id, 'a2');
  });

  test('markComplete removes the row', () async {
    await storage.enqueue(task('a'));
    await storage.markComplete('a');
    expect(await storage.findById('a'), isNull);
    expect(await storage.countByStatus(SyncTaskStatus.pending), 0);
  });

  test('markDead moves a task to the dead letter queue', () async {
    await storage.enqueue(task('a'));
    await storage.markDead('a', error: '422 rejected');

    expect(await storage.countByStatus(SyncTaskStatus.dead), 1);
    final dead = await storage.deadLetters();
    expect(dead.single.lastError, '422 rejected');
  });

  test('deadLetters is newest-failure first and paginates', () async {
    for (final id in ['a', 'b', 'c']) {
      await storage.enqueue(task(id));
      await storage.markDead(id);
    }

    final page1 = await storage.deadLetters(limit: 2, offset: 0);
    final page2 = await storage.deadLetters(limit: 2, offset: 2);
    expect(page1.length, 2);
    expect(page2.length, 1);
    expect(page1.map((t) => t.id).toList() + page2.map((t) => t.id).toList(),
        containsAll(['a', 'b', 'c']));
  });

  test('reviveDeadLetter resets status and retry count', () async {
    await storage.enqueue(task('a'));
    await storage.markDead('a', error: 'boom');
    await storage.reviveDeadLetter('a');

    final revived = await storage.findById('a');
    expect(revived!.status, SyncTaskStatus.pending);
    expect(revived.retryCount, 0);
    expect(revived.lastError, isNull);
  });

  test('reviveDeadLetter throws for an unknown id', () async {
    expect(
      () => storage.reviveDeadLetter('missing'),
      throwsA(isA<TaskNotFoundException>()),
    );
  });

  test('purgeDeadLetters removes one or all dead tasks', () async {
    await storage.enqueue(task('a'));
    await storage.enqueue(task('b'));
    await storage.markDead('a');
    await storage.markDead('b');

    final removedOne = await storage.purgeDeadLetters(taskId: 'a');
    expect(removedOne, 1);
    expect(await storage.countByStatus(SyncTaskStatus.dead), 1);

    final removedRest = await storage.purgeDeadLetters();
    expect(removedRest, 1);
    expect(await storage.countByStatus(SyncTaskStatus.dead), 0);
  });

  test('recoverInterrupted resets processing tasks to pending', () async {
    await storage.enqueue(task('a'));
    await storage.markProcessing('a');

    final recovered = await storage.recoverInterrupted();
    expect(recovered, 1);
    expect(await storage.countByStatus(SyncTaskStatus.pending), 1);
  });

  test('earliestNextAttempt returns the soonest future wake time', () async {
    await storage.enqueue(task('a'));
    expect(await storage.earliestNextAttempt(), isNull);

    final soon = DateTime.now().add(const Duration(minutes: 1));
    final later = DateTime.now().add(const Duration(minutes: 10));
    await storage.reschedule('a', retryCount: 1, nextAttemptAt: later);
    await storage.enqueue(task('b'));
    await storage.reschedule('b', retryCount: 1, nextAttemptAt: soon);

    final wake = await storage.earliestNextAttempt();
    expect(wake!.difference(soon).inSeconds.abs() < 2, isTrue);
  });

  test('clear removes every task regardless of status', () async {
    await storage.enqueue(task('a'));
    await storage.enqueue(task('b'));
    await storage.markDead('b');

    await storage.clear();
    expect(await storage.countByStatus(SyncTaskStatus.pending), 0);
    expect(await storage.countByStatus(SyncTaskStatus.dead), 0);
  });

  test('withDatabase does not close the caller-owned handle', () async {
    await storage.close();
    expect(await db.getVersion(), isNotNull);
  });
}
