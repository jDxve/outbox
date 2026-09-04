import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:flutter/material.dart';
import 'package:outbox/outbox.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

class FakeApi {
  bool offline = false;
  final List<String> applied = [];

  Future<void> send(SyncTask task) async {
    await Future<void>.delayed(const Duration(milliseconds: 400));
    if (offline) throw Exception('No route to host');

    final quantity = task.payload['quantity'] as int;
    if (quantity < 0) {
      throw const PermanentTaskFailure('Quantity must be positive (422)');
    }
    applied.add('${task.payload['sku']} +$quantity');
  }
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  if (Platform.isWindows || Platform.isLinux) {
    sqfliteFfiInit();
    databaseFactory = databaseFactoryFfi;
  }
  final api = FakeApi();

  final queue = await Outbox.create(
    storage: SqliteQueueStorage(databaseName: 'example_queue.db'),
    executor: api.send,
    retryPolicy: const RetryPolicy(
      maxAttempts: 4,
      initialDelay: Duration(seconds: 2),
    ),
    onDeadLetter: (task) => debugPrint('Gave up on ${task.id}'),
  );

  runApp(ExampleApp(api: api, queue: queue));
}

class ExampleApp extends StatelessWidget {
  const ExampleApp({super.key, required this.api, required this.queue});

  final FakeApi api;
  final Outbox queue;

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'outbox',
      theme: ThemeData(useMaterial3: true, colorSchemeSeed: Colors.indigo),
      home: HomePage(api: api, queue: queue),
    );
  }
}

class HomePage extends StatefulWidget {
  const HomePage({super.key, required this.api, required this.queue});

  final FakeApi api;
  final Outbox queue;

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  final _random = Random();

  @override
  void dispose() {
    widget.queue.dispose();
    super.dispose();
  }

  Future<void> _adjust({required bool invalid}) async {
    await widget.queue.add(
      action: 'inventory/adjust',
      payload: {
        'sku': 'SKU-${100 + _random.nextInt(3)}',
        'quantity': invalid ? -5 : 1 + _random.nextInt(9),
      },
      groupKey: 'inventory',
    );
  }

  void _toggleOutage(bool offline) {
    setState(() => widget.api.offline = offline);
    if (!offline) widget.queue.processQueue();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Offline sync queue')),
      body: StreamBuilder<QueueSnapshot>(
        stream: widget.queue.states,
        builder: (context, snapshot) {
          final state = snapshot.data;
          return ListView(
            padding: const EdgeInsets.all(16),
            children: [
              SwitchListTile(
                title: const Text('Simulate outage'),
                subtitle: const Text('Requests fail until this is off'),
                value: widget.api.offline,
                onChanged: _toggleOutage,
              ),
              const Divider(),
              ListTile(
                title: const Text('Pending'),
                trailing: Text('${state?.pending ?? 0}'),
              ),
              ListTile(
                title: const Text('Dead letters'),
                trailing: Text('${state?.dead ?? 0}'),
              ),
              ListTile(
                title: const Text('Draining'),
                trailing: Text('${state?.isProcessing ?? false}'),
              ),
              if (state?.nextAttemptAt != null)
                ListTile(
                  title: const Text('Next retry'),
                  trailing: Text(
                    '${state!.nextAttemptAt!.difference(DateTime.now()).inSeconds}s',
                  ),
                ),
              if (state?.lastError != null)
                ListTile(
                  title: const Text('Last error'),
                  subtitle: Text(state!.lastError!),
                ),
              const SizedBox(height: 8),
              FilledButton(
                onPressed: () => _adjust(invalid: false),
                child: const Text('Queue an adjustment'),
              ),
              const SizedBox(height: 8),
              OutlinedButton(
                onPressed: () => _adjust(invalid: true),
                child: const Text('Queue an invalid one (dead letters)'),
              ),
              const SizedBox(height: 8),
              TextButton(
                onPressed: () => Navigator.of(context).push(
                  MaterialPageRoute<void>(
                    builder: (_) => DeadLetterPage(queue: widget.queue),
                  ),
                ),
                child: const Text('View dead letters'),
              ),
              const Divider(),
              Text('Applied on the server',
                  style: Theme.of(context).textTheme.titleSmall),
              for (final entry in widget.api.applied.reversed.take(10))
                ListTile(dense: true, title: Text(entry)),
            ],
          );
        },
      ),
    );
  }
}

class DeadLetterPage extends StatefulWidget {
  const DeadLetterPage({super.key, required this.queue});

  final Outbox queue;

  @override
  State<DeadLetterPage> createState() => _DeadLetterPageState();
}

class _DeadLetterPageState extends State<DeadLetterPage> {
  late Future<List<SyncTask>> _future = widget.queue.deadLetters();

  void _reload() => setState(() => _future = widget.queue.deadLetters());

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Dead letters'),
        actions: [
          IconButton(
            icon: const Icon(Icons.delete_sweep),
            onPressed: () async {
              await widget.queue.purgeDeadLetters();
              _reload();
            },
          ),
        ],
      ),
      body: FutureBuilder<List<SyncTask>>(
        future: _future,
        builder: (context, snapshot) {
          final tasks = snapshot.data ?? const <SyncTask>[];
          if (tasks.isEmpty) {
            return const Center(child: Text('Nothing here'));
          }
          return ListView.builder(
            itemCount: tasks.length,
            itemBuilder: (context, index) {
              final task = tasks[index];
              return ListTile(
                title: Text(task.action),
                subtitle: Text('${task.payload}\n${task.lastError ?? ''}'),
                isThreeLine: true,
                trailing: IconButton(
                  icon: const Icon(Icons.refresh),
                  onPressed: () async {
                    await widget.queue.retryDeadLetter(task.id);
                    _reload();
                  },
                ),
              );
            },
          );
        },
      ),
    );
  }
}
