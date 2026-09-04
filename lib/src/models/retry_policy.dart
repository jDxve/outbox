import 'dart:math';

/// [maxAttempts] counts total attempts, not retries after the first.
class RetryPolicy {
  const RetryPolicy({
    this.maxAttempts = 5,
    this.initialDelay = const Duration(seconds: 2),
    this.multiplier = 2.0,
    this.maxDelay = const Duration(minutes: 30),
    this.jitter = 0.25,
  })  : assert(maxAttempts > 0, 'maxAttempts must be at least 1'),
        assert(multiplier >= 1.0, 'multiplier must be at least 1.0'),
        assert(jitter >= 0.0 && jitter <= 1.0, 'jitter must be 0.0 to 1.0');

  final int maxAttempts;
  final Duration initialDelay;
  final double multiplier;
  final Duration maxDelay;

  /// Random spread on each delay; avoids a reconnect stampede.
  final double jitter;

  static const RetryPolicy noRetry = RetryPolicy(maxAttempts: 1);

  /// Delay before attempt number [attemptsMade] + 1.
  Duration delayAfter(int attemptsMade, {Random? random}) {
    final exponent = max(0, attemptsMade - 1);
    final base = initialDelay.inMilliseconds * pow(multiplier, exponent);
    final capped = min(base.toDouble(), maxDelay.inMilliseconds.toDouble());
    if (jitter == 0) return Duration(milliseconds: capped.round());

    final rng = random ?? Random();
    final spread = capped * jitter;
    final offset = (rng.nextDouble() * 2 - 1) * spread;
    final result = max(0.0, capped + offset);
    return Duration(milliseconds: result.round());
  }

  bool canRetry(int attemptsMade) => attemptsMade < maxAttempts;
}
