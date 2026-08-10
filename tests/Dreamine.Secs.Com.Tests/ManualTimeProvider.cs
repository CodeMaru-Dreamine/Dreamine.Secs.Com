namespace Dreamine.Secs.Com.Tests;

internal sealed class ManualTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<ManualTimer> _timers = new();
    private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;

    public override DateTimeOffset GetUtcNow() { lock (_gate) return _utcNow; }
    public override long GetTimestamp() { lock (_gate) return _utcNow.UtcTicks; }
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    public int ScheduledTimerCount { get { lock (_gate) return _timers.Count(item => !item.IsDisposed && item.DueAt != DateTimeOffset.MaxValue); } }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ManualTimer(this, callback, state);
        lock (_gate) { _timers.Add(timer); timer.ChangeCore(dueTime, period); }
        return timer;
    }

    public void Advance(TimeSpan amount)
    {
        if (amount < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(amount));
        DateTimeOffset target;
        lock (_gate) target = _utcNow + amount;
        while (true)
        {
            ManualTimer? timer;
            lock (_gate)
            {
                timer = _timers.Where(item => !item.IsDisposed && item.DueAt <= target).OrderBy(item => item.DueAt).FirstOrDefault();
                if (timer is null) { _utcNow = target; return; }
                _utcNow = timer.DueAt;
                timer.ScheduleNext();
            }
            timer.Invoke();
        }
    }

    private sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        public DateTimeOffset DueAt { get; private set; } = DateTimeOffset.MaxValue;
        public TimeSpan Period { get; private set; } = Timeout.InfiniteTimeSpan;
        public bool IsDisposed { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._gate)
            {
                if (IsDisposed) return false;
                ChangeCore(dueTime, period); return true;
            }
        }

        public void ChangeCore(TimeSpan dueTime, TimeSpan period)
        {
            if (dueTime < Timeout.InfiniteTimeSpan) throw new ArgumentOutOfRangeException(nameof(dueTime));
            if (period < Timeout.InfiniteTimeSpan || period == TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(period));
            DueAt = dueTime == Timeout.InfiniteTimeSpan ? DateTimeOffset.MaxValue : owner._utcNow + dueTime;
            Period = period;
        }

        public void ScheduleNext() => DueAt = Period == Timeout.InfiniteTimeSpan ? DateTimeOffset.MaxValue : DueAt + Period;
        public void Invoke() { if (!IsDisposed) callback(state); }
        public void Dispose() { lock (owner._gate) { IsDisposed = true; owner._timers.Remove(this); } }
        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
    }
}
