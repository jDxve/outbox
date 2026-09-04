# Changelog

## 0.1.0

Initial release as `outbox` (prototyped under `offline_sync_queue`, `ordered_outbox` and `syncflow_engine` first).

- Durable SQLite backed FIFO queue with a monotonic sequence for ordering.
- `RetryPolicy` with exponential backoff, jitter and a delay ceiling.
- `PermanentTaskFailure` to dead letter without consuming retries.
- Per-group ordering via `groupKey`, so one stuck task does not block the queue.
- Dead letter queue with read, retry and purge.
- `QueueSnapshot` stream for pending counts and connectivity.
- Crash recovery for tasks interrupted mid send.
- `InMemoryQueueStorage` test driver and a pluggable `QueueStorage` interface.
