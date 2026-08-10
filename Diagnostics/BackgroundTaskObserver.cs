using System.Diagnostics;

namespace Dreamine.Secs.Com.Diagnostics;

internal static class BackgroundTaskObserver
{
    public static void Observe(Task task, string operation)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        _ = task.ContinueWith(
            static (completed, state) => Trace.TraceError("SECS background operation '{0}' failed: {1}", state, completed.Exception),
            operation,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }
}
