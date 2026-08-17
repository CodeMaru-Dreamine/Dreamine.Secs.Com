namespace Dreamine.Secs.Com.Transactions;

internal sealed class PendingMonitor
{
    private readonly object _gate = new();
    private readonly CancellationTokenSource _lifetime = new();
    private bool _removed;
    private bool _monitorStarted;
    private bool _lifetimeDisposed;

    public bool TryStart(
        Func<CancellationToken, Task> monitorFactory,
        string duplicateStartMessage,
        out Task? monitor)
    {
        lock (_gate)
        {
            if (_removed) { monitor = null; return false; }
            if (_monitorStarted) throw new InvalidOperationException(duplicateStartMessage);
            _monitorStarted = true;
            monitor = monitorFactory(_lifetime.Token);
            return true;
        }
    }

    public void CancelFromOwner()
    {
        lock (_gate)
        {
            if (_removed) return;
            _removed = true;
            if (_lifetimeDisposed) return;
            _lifetime.Cancel();
            if (_monitorStarted) return;
            _lifetime.Dispose();
            _lifetimeDisposed = true;
        }
    }

    public void DisposeAfterMonitor()
    {
        lock (_gate)
        {
            _removed = true;
            if (_lifetimeDisposed) return;
            _lifetime.Dispose();
            _lifetimeDisposed = true;
        }
    }
}
