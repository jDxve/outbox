import 'package:path/path.dart' as p;
import 'package:sqflite/sqflite.dart';

import '../errors/queue_exceptions.dart';
import '../models/sync_task.dart';
import 'queue_storage.dart';

const String kQueueTable = 'sync_queue';

/// Durable [QueueStorage] backed by sqflite. Windows/Linux need
/// `sqflite_common_ffi`'s factory set before creating the queue.
class SqliteQueueStorage implements QueueStorage {
  SqliteQueueStorage({this.databaseName = 'outbox.db'})
      : _external = null;

  /// Reuses an open handle; [close] is a no-op since the caller owns it.
  SqliteQueueStorage.withDatabase(Database database)
      : _external = database,
        databaseName = null;

  final String? databaseName;

  final Database? _external;
  Database? _db;

  Database get _database {
    final db = _db;
    if (db == null) {
      throw const QueueStateError(
        'Storage used before initialize() finished. Build the queue with '
        'Outbox.create(), which awaits initialization for you.',
      );
    }
    return db;
  }

  @override
  Future<void> initialize() async {
    if (_db != null) return;

    final external = _external;
    if (external != null) {
      _db = external;
      await _createSchema(external);
      return;
    }

    final directory = await getDatabasesPath();
    _db = await openDatabase(
      p.join(directory, databaseName!),
      version: 1,
      onCreate: (db, _) => _createSchema(db),
      onUpgrade: (db, _, __) => _createSchema(db),
    );
  }

  Future<void> _createSchema(DatabaseExecutor db) async {
    await db.execute('''
      CREATE TABLE IF NOT EXISTS $kQueueTable (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        action TEXT NOT NULL,
        payload TEXT NOT NULL,
        group_key TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        retry_count INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        status TEXT NOT NULL DEFAULT 'pending'
      )
    ''');
    await db.execute('''
      CREATE INDEX IF NOT EXISTS idx_queue_ready
      ON $kQueueTable (status, next_attempt_at, seq)
    ''');
    await db.execute('''
      CREATE INDEX IF NOT EXISTS idx_queue_group
      ON $kQueueTable (group_key, seq)
    ''');
  }

  @override
  Future<void> enqueue(SyncTask task) async {
    final row = task.toMap();
    row['updated_at'] = DateTime.now().millisecondsSinceEpoch;
    await _database.insert(kQueueTable, row);
  }

  @override
  Future<SyncTask?> nextReady(
    DateTime now, {
    bool haltGroupOnDeadLetter = true,
  }) async {
    final blocking = <String>[
      SyncTaskStatus.pending.name,
      SyncTaskStatus.processing.name,
      if (haltGroupOnDeadLetter) SyncTaskStatus.dead.name,
    ];
    final placeholders = List.filled(blocking.length, '?').join(',');

    final rows = await _database.rawQuery('''
      SELECT t.* FROM $kQueueTable AS t
      WHERE t.status = ?
        AND t.next_attempt_at <= ?
        AND NOT EXISTS (
          SELECT 1 FROM $kQueueTable AS b
          WHERE b.group_key = t.group_key
            AND b.seq < t.seq
            AND b.status IN ($placeholders)
        )
      ORDER BY t.seq ASC
      LIMIT 1
    ''', [
      SyncTaskStatus.pending.name,
      now.millisecondsSinceEpoch,
      ...blocking,
    ]);

    if (rows.isEmpty) return null;
    return SyncTask.fromMap(rows.first);
  }

  @override
  Future<DateTime?> earliestNextAttempt() async {
    final rows = await _database.rawQuery('''
      SELECT MIN(next_attempt_at) AS wake FROM $kQueueTable
      WHERE status = ? AND next_attempt_at > ?
    ''', [SyncTaskStatus.pending.name, DateTime.now().millisecondsSinceEpoch]);

    final wake = rows.first['wake'] as int?;
    if (wake == null || wake == 0) return null;
    return DateTime.fromMillisecondsSinceEpoch(wake);
  }

  @override
  Future<void> markProcessing(String taskId) async {
    await _update(taskId, {'status': SyncTaskStatus.processing.name});
  }

  @override
  Future<void> markComplete(String taskId) async {
    await _database.delete(kQueueTable, where: 'id = ?', whereArgs: [taskId]);
  }

  @override
  Future<void> reschedule(
    String taskId, {
    required int retryCount,
    required DateTime nextAttemptAt,
    String? error,
  }) async {
    await _update(taskId, {
      'status': SyncTaskStatus.pending.name,
      'retry_count': retryCount,
      'next_attempt_at': nextAttemptAt.millisecondsSinceEpoch,
      'last_error': error,
    });
  }

  @override
  Future<void> markDead(String taskId, {String? error}) async {
    await _update(taskId, {
      'status': SyncTaskStatus.dead.name,
      'last_error': error,
    });
  }

  @override
  Future<int> recoverInterrupted() async {
    return _database.update(
      kQueueTable,
      {
        'status': SyncTaskStatus.pending.name,
        'next_attempt_at': 0,
        'updated_at': DateTime.now().millisecondsSinceEpoch,
      },
      where: 'status = ?',
      whereArgs: [SyncTaskStatus.processing.name],
    );
  }

  @override
  Future<int> countByStatus(SyncTaskStatus status) async {
    final count = Sqflite.firstIntValue(await _database.rawQuery(
      'SELECT COUNT(*) FROM $kQueueTable WHERE status = ?',
      [status.name],
    ));
    return count ?? 0;
  }

  @override
  Future<List<SyncTask>> deadLetters({int limit = 50, int offset = 0}) async {
    final rows = await _database.query(
      kQueueTable,
      where: 'status = ?',
      whereArgs: [SyncTaskStatus.dead.name],
      orderBy: 'updated_at DESC, seq DESC',
      limit: limit,
      offset: offset,
    );
    return rows.map(SyncTask.fromMap).toList();
  }

  @override
  Future<SyncTask?> findById(String taskId) async {
    final rows = await _database.query(
      kQueueTable,
      where: 'id = ?',
      whereArgs: [taskId],
      limit: 1,
    );
    if (rows.isEmpty) return null;
    return SyncTask.fromMap(rows.first);
  }

  @override
  Future<void> reviveDeadLetter(String taskId) async {
    final affected = await _update(taskId, {
      'status': SyncTaskStatus.pending.name,
      'retry_count': 0,
      'next_attempt_at': 0,
      'last_error': null,
    });
    if (affected == 0) throw TaskNotFoundException(taskId);
  }

  @override
  Future<int> purgeDeadLetters({String? taskId}) {
    return _database.delete(
      kQueueTable,
      where: taskId == null ? 'status = ?' : 'status = ? AND id = ?',
      whereArgs: [
        SyncTaskStatus.dead.name,
        if (taskId != null) taskId,
      ],
    );
  }

  @override
  Future<void> clear() async {
    await _database.delete(kQueueTable);
  }

  @override
  Future<void> close() async {
    if (_external != null) return;
    await _db?.close();
    _db = null;
  }

  Future<int> _update(String taskId, Map<String, Object?> values) {
    return _database.update(
      kQueueTable,
      {...values, 'updated_at': DateTime.now().millisecondsSinceEpoch},
      where: 'id = ?',
      whereArgs: [taskId],
    );
  }
}
