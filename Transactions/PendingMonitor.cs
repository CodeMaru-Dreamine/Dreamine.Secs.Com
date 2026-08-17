namespace Dreamine.Secs.Com.Transactions;

internal static class PendingMonitor
{
    public static bool TryStart(
        bool removed,
        ref bool monitorStarted,
        CancellationTokenSource lifetime,
        Func<CancellationToken, Task> monitorFactory,
        string duplicateStartMessage,
        out Task? monitor)
    {
        if (removed) { monitor = null; return false; }
        if (monitorStarted) throw new InvalidOperationException(duplicateStartMessage);
        monitorStarted = true;
        monitor = monitorFactory(lifetime.Token);
        return true;
    }
}
