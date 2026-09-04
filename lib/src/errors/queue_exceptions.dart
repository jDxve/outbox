abstract class QueueException implements Exception {
  const QueueException(this.message);
  final String message;

  @override
  String toString() => '$runtimeType: $message';
}

/// Throw for failures that can never succeed; skips retries.
class PermanentTaskFailure extends QueueException {
  const PermanentTaskFailure(super.message);
}

class QueueStateError extends QueueException {
  const QueueStateError(super.message);
}

class TaskNotFoundException extends QueueException {
  TaskNotFoundException(this.taskId) : super('No task with id $taskId');
  final String taskId;
}
