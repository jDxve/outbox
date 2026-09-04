import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';

/// Reports interface state, not server reachability.
class ConnectivityWatcher {
  ConnectivityWatcher({Stream<bool>? stream}) : _custom = stream;

  final Stream<bool>? _custom;
  StreamSubscription<Object?>? _subscription;
  final StreamController<bool> _controller = StreamController<bool>.broadcast();

  bool _isOnline = true;
  bool _started = false;

  bool get isOnline => _isOnline;
  Stream<bool> get onChanged => _controller.stream;

  Future<void> start() async {
    if (_started) return;
    _started = true;

    final custom = _custom;
    if (custom != null) {
      _subscription = custom.listen((online) => _emit(online));
      return;
    }

    final connectivity = Connectivity();
    try {
      _isOnline = _hasConnection(await connectivity.checkConnectivity());
    } catch (_) {
      _isOnline = true;
    }

    _subscription = connectivity.onConnectivityChanged.listen(
      (results) => _emit(_hasConnection(results)),
      onError: (_) => _emit(true),
    );
  }

  bool _hasConnection(List<ConnectivityResult> results) {
    return results.isNotEmpty && !results.contains(ConnectivityResult.none);
  }

  void _emit(bool online) {
    if (online == _isOnline) return;
    _isOnline = online;
    if (!_controller.isClosed) _controller.add(online);
  }

  Future<void> dispose() async {
    await _subscription?.cancel();
    _subscription = null;
    _started = false;
    await _controller.close();
  }
}
