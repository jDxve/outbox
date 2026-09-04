import 'dart:convert';

enum SyncTaskStatus {
  pending,

  /// Reset to [pending] on next startup if the process died mid-send.
  processing,
  dead,
}

SyncTaskStatus syncTaskStatusFromName(String value) {
  return SyncTaskStatus.values.firstWhere(
    (status) => status.name == value,
    orElse: () => SyncTaskStatus.pending,
  );
}

/// Immutable; storage returns updated copies rather than mutating in place.
class SyncTask {
  const SyncTask({
    required this.id,
    required this.action,
    required this.payload,
    required this.createdAt,
    this.seq,
    this.groupKey,
    this.retryCount = 0,
    this.nextAttemptAt,
    this.lastError,
    this.status = SyncTaskStatus.pending,
  });

  /// FIFO order is based on this, not [createdAt] (clock-change safe).
  final int? seq;

  /// Doubles as the idempotency key, see [idempotencyKey].
  final String id;

  final String action;
  final Map<String, dynamic> payload;

  /// Same-group tasks run in strict insertion order.
  final String? groupKey;

  final DateTime createdAt;
  final int retryCount;
  final DateTime? nextAttemptAt;
  final String? lastError;
  final SyncTaskStatus status;

  String get idempotencyKey => id;

  bool isReadyAt(DateTime now) =>
      nextAttemptAt == null || !nextAttemptAt!.isAfter(now);

  SyncTask copyWith({
    int? seq,
    int? retryCount,
    DateTime? nextAttemptAt,
    String? lastError,
    SyncTaskStatus? status,
  }) {
    return SyncTask(
      id: id,
      action: action,
      payload: payload,
      createdAt: createdAt,
      seq: seq ?? this.seq,
      groupKey: groupKey,
      retryCount: retryCount ?? this.retryCount,
      nextAttemptAt: nextAttemptAt ?? this.nextAttemptAt,
      lastError: lastError ?? this.lastError,
      status: status ?? this.status,
    );
  }

  Map<String, Object?> toMap() => {
        'id': id,
        'action': action,
        'payload': jsonEncode(payload),
        'group_key': groupKey,
        'created_at': createdAt.millisecondsSinceEpoch,
        'retry_count': retryCount,
        'next_attempt_at': nextAttemptAt?.millisecondsSinceEpoch ?? 0,
        'last_error': lastError,
        'status': status.name,
      };

  factory SyncTask.fromMap(Map<String, Object?> map) {
    final nextAttempt = (map['next_attempt_at'] as int?) ?? 0;
    return SyncTask(
      seq: map['seq'] as int?,
      id: map['id'] as String,
      action: map['action'] as String,
      payload: Map<String, dynamic>.from(
        jsonDecode(map['payload'] as String) as Map,
      ),
      groupKey: map['group_key'] as String?,
      createdAt: DateTime.fromMillisecondsSinceEpoch(map['created_at'] as int),
      retryCount: (map['retry_count'] as int?) ?? 0,
      nextAttemptAt: nextAttempt == 0
          ? null
          : DateTime.fromMillisecondsSinceEpoch(nextAttempt),
      lastError: map['last_error'] as String?,
      status: syncTaskStatusFromName(map['status'] as String),
    );
  }

  @override
  String toString() =>
      'SyncTask(seq: $seq, id: $id, action: $action, status: ${status.name}, '
      'retryCount: $retryCount)';
}
