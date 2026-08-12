using System.Diagnostics;
using Dreamine.Secs.Abstractions.Diagnostics;

namespace Dreamine.Secs.Com.Diagnostics;

internal sealed class SafeSecsDiagnosticSink : ISecsDiagnosticSink
{
    private const int QueueCapacity = 1024;
    [ThreadStatic]
    private static SafeSecsDiagnosticSink? _currentCallbackSink;
    private readonly ISecsDiagnosticSink _inner;
    private readonly object _gate = new();
    private readonly Queue<SecsDiagnosticEvent> _queue = new();
    private bool _draining;
    private long _droppedCount;

    private SafeSecsDiagnosticSink(ISecsDiagnosticSink inner) => _inner = inner;

    public static ISecsDiagnosticSink Wrap(ISecsDiagnosticSink? sink)
    {
        var resolved = sink ?? NullSecsDiagnosticSink.Instance;
        return resolved is SafeSecsDiagnosticSink or NullSecsDiagnosticSink ? resolved : new SafeSecsDiagnosticSink(resolved);
    }

    internal long DroppedCount => Interlocked.Read(ref _droppedCount);

    internal static bool IsInvoking(ISecsDiagnosticSink sink) =>
        ReferenceEquals(_currentCallbackSink, sink);

    public void Emit(SecsDiagnosticEvent diagnosticEvent)
    {
        var ownsDrain = false;
        var dropped = false;
        lock (_gate)
        {
            if (_draining)
            {
                if (_queue.Count == QueueCapacity)
                {
                    Interlocked.Increment(ref _droppedCount);
                    dropped = true;
                }
                else _queue.Enqueue(diagnosticEvent);
            }
            else
            {
                _draining = true;
                ownsDrain = true;
            }
        }

        if (dropped)
        {
            SafeTrace.Warning("SECS diagnostic queue reached its bounded capacity; one diagnostic was dropped.");
            return;
        }
        if (!ownsDrain) return;

        var current = diagnosticEvent;
        while (true)
        {
            var previousCallbackSink = _currentCallbackSink;
            _currentCallbackSink = this;
            try { _inner.Emit(current); }
            catch (Exception exception) { SafeTrace.Error("SECS diagnostic sink failed: {0}", exception); }
            finally { _currentCallbackSink = previousCallbackSink; }

            lock (_gate)
            {
                if (_queue.Count > 0) current = _queue.Dequeue();
                else
                {
                    _draining = false;
                    return;
                }
            }
        }
    }
}

internal static class SafeTrace
{
    public static void Warning(string message)
    {
        try { Trace.TraceWarning(message); }
        catch { }
    }

    public static void Error(string format, params object?[] arguments)
    {
        try { Trace.TraceError(format, arguments); }
        catch { }
    }
}
