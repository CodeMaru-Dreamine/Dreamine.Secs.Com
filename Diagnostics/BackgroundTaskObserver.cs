namespace Dreamine.Secs.Com.Diagnostics;

internal static class BackgroundTaskObserver
{
    public static Task Observe(Task task, string operation)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        return task.ContinueWith(
            static (completed, state) => SafeTrace.Error("SECS background operation '{0}' failed: {1}", state, completed.Exception),
            operation,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }
}
