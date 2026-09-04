import '../models/sync_task.dart';

/// Persistence contract. Called from a single processing loop only.
abstract class QueueStorage {
  Future<void> initialize();
  Future<void> enqueue(SyncTask task);

  /// Next pending, unblocked task past its backoff window, or null.
  Future<SyncTask?> nextReady(
    DateTime now, {
    bool haltGroupOnDeadLetter = true,
  });

  Future<DateTime?> earliestNextAttempt();
  Future<void> markProcessing(String taskId);
  Future<void> markComplete(String taskId);

  Future<void> reschedule(
    String taskId, {
    required int retryCount,
    required DateTime nextAttemptAt,
    String? error,
  });

  Future<void> markDead(String taskId, {String? error});

  /// Resets tasks stuck in [SyncTaskStatus.processing] to pending.
  Future<int> recoverInterrupted();

  Future<int> countByStatus(SyncTaskStatus status);
  Future<List<SyncTask>> deadLetters({int limit = 50, int offset = 0});
  Future<SyncTask?> findById(String taskId);
  Future<void> reviveDeadLetter(String taskId);
  Future<int> purgeDeadLetters({String? taskId});
  Future<void> clear();
  Future<void> close();
}
