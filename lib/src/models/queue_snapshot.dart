/// A point in time view of the queue, emitted on [Outbox.states].
class QueueSnapshot {
  const QueueSnapshot({
    required this.pending,
    required this.dead,
    required this.isProcessing,
    required this.isOnline,
    this.nextAttemptAt,
    this.lastError,
  });

  final int pending;
  final int dead;
  final bool isProcessing;

  /// Interface state, not server reachability.
  final bool isOnline;

  final DateTime? nextAttemptAt;
  final String? lastError;

  bool get isIdle => pending == 0 && !isProcessing;

  @override
  String toString() => 'QueueSnapshot(pending: $pending, dead: $dead, '
      'isProcessing: $isProcessing, isOnline: $isOnline)';
}
