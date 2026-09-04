import 'dart:async';

import 'package:uuid/uuid.dart';

import '../errors/queue_exceptions.dart';
import '../models/queue_snapshot.dart';
import '../models/retry_policy.dart';
import '../models/sync_task.dart';
import '../storage/queue_storage.dart';
import '../storage/sqlite_storage.dart';
import 'connectivity_watcher.dart';

/// Throw [PermanentTaskFailure] to dead-letter instead of retrying.
typedef TaskExecutor = Future<void> Function(SyncTask task);

typedef DeadLetterCallback = void Function(SyncTask task);

/// Durable, per-group ordered queue for offline mutations.
class Outbox {
  Outbox._({
    required QueueStorage storage,
    required TaskExecutor executor,
    required this.retryPolicy,
    required this.haltGroupOnDeadLetter,
    required ConnectivityWatcher watcher,
    DeadLetterCallback? onDeadLetter,
    Uuid? uuid,
  })  : _storage = storage,
        _executor = executor,
        _watcher = watcher,
        _onDeadLetter = onDeadLetter,
        _uuid = uuid ?? const Uuid();

  /// Opens storage and starts draining before returning.
  static Future<Outbox> create({
    required TaskExecutor executor,
    QueueStorage? storage,
    RetryPolicy retryPolicy = const RetryPolicy(),
    ConnectivityWatcher? connectivity,
    DeadLetterCallback? onDeadLetter,
    bool haltGroupOnDeadLetter = true,
    bool autoStart = true,
  }) async {
    final store = storage ?? SqliteQueueStorage();
    await store.initialize();

    final queue = Outbox._(
      storage: store,
      executor: executor,
      retryPolicy: retryPolicy,
      haltGroupOnDeadLetter: haltGroupOnDeadLetter,
      watcher: connectivity ?? ConnectivityWatcher(),
      onDeadLetter: onDeadLetter,
    );

    await store.recoverInterrupted();
    await queue._watcher.start();
    queue._connectivitySub = queue._watcher.onChanged.listen((online) {
      if (online) queue.processQueue();
    });

    await queue._emitSnapshot();
    if (autoStart) unawaited(queue.processQueue());
    return queue;
  }

  final QueueStorage _storage;
  final TaskExecutor _executor;
  final ConnectivityWatcher _watcher;
  final DeadLetterCallback? _onDeadLetter;
  final Uuid _uuid;

  final RetryPolicy retryPolicy;

  /// If true, a dead letter blocks the rest of its group.
  final bool haltGroupOnDeadLetter;

  final StreamController<QueueSnapshot> _states =
      StreamController<QueueSnapshot>.broadcast();
  StreamSubscription<bool>? _connectivitySub;
  Timer? _wakeTimer;
  Future<void>? _passFuture;

  bool _isProcessing = false;
  bool _rerunRequested = false;
  bool _disposed = false;
  String? _lastError;

  Stream<QueueSnapshot> get states => _states.stream;
  bool get isProcessing => _isProcessing;
  bool get isOnline => _watcher.isOnline;

  /// Returns the task id, also usable as its idempotency key.
  Future<String> add({
    required String action,
    required Map<String, dynamic> payload,
    String? groupKey,
    String? id,
  }) async {
    _assertUsable();
    final task = SyncTask(
      id: id ?? _uuid.v4(),
      action: action,
      payload: payload,
      groupKey: groupKey,
      createdAt: DateTime.now(),
    );
    await _storage.enqueue(task);
    await _emitSnapshot();
    unawaited(processQueue());
    return task.id;
  }

  /// Coalesces overlapping calls into one extra pass instead of two loops.
  Future<void> processQueue({bool force = false}) {
    if (_disposed) return Future<void>.value();
    if (!force && !_watcher.isOnline) return _emitSnapshot();

    if (_isProcessing) {
      _rerunRequested = true;
      return _passFuture ?? Future<void>.value();
    }

    _isProcessing = true;
    final pass = _runPasses();
    _passFuture = pass;
    return pass;
  }

  Future<void> _runPasses() async {
    try {
      await _emitSnapshot();
      do {
        _rerunRequested = false;
        await _drain();
      } while (_rerunRequested && !_disposed);
    } finally {
      _isProcessing = false;
      _passFuture = null;
    }
    await _scheduleWake();
    await _emitSnapshot();
  }

  Future<void> _drain() async {
    while (!_disposed) {
      final task = await _storage.nextReady(
        DateTime.now(),
        haltGroupOnDeadLetter: haltGroupOnDeadLetter,
      );
      if (task == null) return;

      await _storage.markProcessing(task.id);
      try {
        await _executor(task);
        await _storage.markComplete(task.id);
        _lastError = null;
      } on PermanentTaskFailure catch (error) {
        await _deadLetter(task, error.message);
      } catch (error) {
        await _handleFailure(task, error);
      }
      await _emitSnapshot();
    }
  }

  Future<void> _handleFailure(SyncTask task, Object error) async {
    _lastError = error.toString();
    final attemptsMade = task.retryCount + 1;

    if (!retryPolicy.canRetry(attemptsMade)) {
      await _deadLetter(task, _lastError);
      return;
    }

    await _storage.reschedule(
      task.id,
      retryCount: attemptsMade,
      nextAttemptAt: DateTime.now().add(retryPolicy.delayAfter(attemptsMade)),
      error: _lastError,
    );
  }

  Future<void> _deadLetter(SyncTask task, String? error) async {
    _lastError = error;
    await _storage.markDead(task.id, error: error);
    final stored = await _storage.findById(task.id);
    if (stored != null) _onDeadLetter?.call(stored);
  }

  Future<void> _scheduleWake() async {
    _wakeTimer?.cancel();
    if (_disposed) return;

    final wakeAt = await _storage.earliestNextAttempt();
    if (wakeAt == null) return;

    final delay = wakeAt.difference(DateTime.now());
    _wakeTimer = Timer(
      delay.isNegative ? Duration.zero : delay,
      () => processQueue(),
    );
  }

  Future<int> pendingCount() => _storage.countByStatus(SyncTaskStatus.pending);
  Future<int> deadLetterCount() => _storage.countByStatus(SyncTaskStatus.dead);

  Future<List<SyncTask>> deadLetters({int limit = 50, int offset = 0}) {
    return _storage.deadLetters(limit: limit, offset: offset);
  }

  /// Throws [TaskNotFoundException] if the id is unknown.
  Future<void> retryDeadLetter(String taskId) async {
    _assertUsable();
    await _storage.reviveDeadLetter(taskId);
    await _emitSnapshot();
    unawaited(processQueue());
  }

  Future<int> purgeDeadLetters({String? taskId}) async {
    _assertUsable();
    final removed = await _storage.purgeDeadLetters(taskId: taskId);
    await _emitSnapshot();
    return removed;
  }

  Future<void> clear() async {
    _assertUsable();
    await _storage.clear();
    await _emitSnapshot();
  }

  Future<QueueSnapshot> snapshot() async {
    return QueueSnapshot(
      pending: await _storage.countByStatus(SyncTaskStatus.pending),
      dead: await _storage.countByStatus(SyncTaskStatus.dead),
      isProcessing: _isProcessing,
      isOnline: _watcher.isOnline,
      nextAttemptAt: await _storage.earliestNextAttempt(),
      lastError: _lastError,
    );
  }

  Future<void> _emitSnapshot() async {
    if (_disposed || _states.isClosed) return;
    _states.add(await snapshot());
  }

  void _assertUsable() {
    if (_disposed) {
      throw const QueueStateError('Queue was disposed and cannot be reused.');
    }
  }

  /// In-flight task finishes; pending tasks stay on disk.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _wakeTimer?.cancel();
    _wakeTimer = null;
    await _connectivitySub?.cancel();
    _connectivitySub = null;
    await _watcher.dispose();
    await _states.close();
    await _storage.close();
  }
}
