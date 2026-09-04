import '../errors/queue_exceptions.dart';
import '../models/sync_task.dart';
import 'queue_storage.dart';

/// Non-durable [QueueStorage] for tests; no sqflite binding needed.
class InMemoryQueueStorage implements QueueStorage {
  final List<SyncTask> _tasks = [];
  final Map<String, int> _updatedAt = {};
  int _seq = 0;

  List<SyncTask> get tasks => List.unmodifiable(_tasks);

  @override
  Future<void> initialize() async {}

  @override
  Future<void> enqueue(SyncTask task) async {
    _tasks.add(task.copyWith(seq: ++_seq));
    _touch(task.id);
  }

  @override
  Future<SyncTask?> nextReady(
    DateTime now, {
    bool haltGroupOnDeadLetter = true,
  }) async {
    final blocking = {
      SyncTaskStatus.pending,
      SyncTaskStatus.processing,
      if (haltGroupOnDeadLetter) SyncTaskStatus.dead,
    };

    final candidates = _tasks
        .where((t) => t.status == SyncTaskStatus.pending && t.isReadyAt(now))
        .toList()
      ..sort((a, b) => (a.seq ?? 0).compareTo(b.seq ?? 0));

    for (final task in candidates) {
      final group = task.groupKey;
      if (group == null) return task;

      final blocked = _tasks.any((other) =>
          other.groupKey == group &&
          (other.seq ?? 0) < (task.seq ?? 0) &&
          blocking.contains(other.status));
      if (!blocked) return task;
    }
    return null;
  }

  @override
  Future<DateTime?> earliestNextAttempt() async {
    final now = DateTime.now();
    final times = _tasks
        .where((t) =>
            t.status == SyncTaskStatus.pending &&
            t.nextAttemptAt != null &&
            t.nextAttemptAt!.isAfter(now))
        .map((t) => t.nextAttemptAt!)
        .toList()
      ..sort();
    return times.isEmpty ? null : times.first;
  }

  @override
  Future<void> markProcessing(String taskId) async {
    _replace(taskId, (t) => t.copyWith(status: SyncTaskStatus.processing));
  }

  @override
  Future<void> markComplete(String taskId) async {
    _tasks.removeWhere((t) => t.id == taskId);
    _updatedAt.remove(taskId);
  }

  @override
  Future<void> reschedule(
    String taskId, {
    required int retryCount,
    required DateTime nextAttemptAt,
    String? error,
  }) async {
    _replace(
      taskId,
      (t) => t.copyWith(
        status: SyncTaskStatus.pending,
        retryCount: retryCount,
        nextAttemptAt: nextAttemptAt,
        lastError: error,
      ),
    );
  }

  @override
  Future<void> markDead(String taskId, {String? error}) async {
    _replace(
      taskId,
      (t) => t.copyWith(status: SyncTaskStatus.dead, lastError: error),
    );
  }

  @override
  Future<int> recoverInterrupted() async {
    var recovered = 0;
    for (var i = 0; i < _tasks.length; i++) {
      if (_tasks[i].status == SyncTaskStatus.processing) {
        _tasks[i] = _tasks[i].copyWith(status: SyncTaskStatus.pending);
        _touch(_tasks[i].id);
        recovered++;
      }
    }
    return recovered;
  }

  @override
  Future<int> countByStatus(SyncTaskStatus status) async {
    return _tasks.where((t) => t.status == status).length;
  }

  @override
  Future<List<SyncTask>> deadLetters({int limit = 50, int offset = 0}) async {
    final dead = _tasks.where((t) => t.status == SyncTaskStatus.dead).toList()
      ..sort((a, b) =>
          (_updatedAt[b.id] ?? 0).compareTo(_updatedAt[a.id] ?? 0));
    if (offset >= dead.length) return const [];
    return dead.skip(offset).take(limit).toList();
  }

  @override
  Future<SyncTask?> findById(String taskId) async {
    for (final task in _tasks) {
      if (task.id == taskId) return task;
    }
    return null;
  }

  @override
  Future<void> reviveDeadLetter(String taskId) async {
    final index = _tasks.indexWhere((t) => t.id == taskId);
    if (index == -1) throw TaskNotFoundException(taskId);
    _tasks[index] = SyncTask(
      seq: _tasks[index].seq,
      id: _tasks[index].id,
      action: _tasks[index].action,
      payload: _tasks[index].payload,
      groupKey: _tasks[index].groupKey,
      createdAt: _tasks[index].createdAt,
    );
    _touch(taskId);
  }

  @override
  Future<int> purgeDeadLetters({String? taskId}) async {
    final before = _tasks.length;
    _tasks.removeWhere((t) =>
        t.status == SyncTaskStatus.dead && (taskId == null || t.id == taskId));
    return before - _tasks.length;
  }

  @override
  Future<void> clear() async {
    _tasks.clear();
    _updatedAt.clear();
  }

  @override
  Future<void> close() async {}

  void _replace(String taskId, SyncTask Function(SyncTask) update) {
    final index = _tasks.indexWhere((t) => t.id == taskId);
    if (index == -1) return;
    _tasks[index] = update(_tasks[index]);
    _touch(taskId);
  }

  void _touch(String taskId) {
    _updatedAt[taskId] = DateTime.now().microsecondsSinceEpoch;
  }
}
