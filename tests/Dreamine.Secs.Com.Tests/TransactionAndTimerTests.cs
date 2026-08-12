using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Hsms;
using Dreamine.Secs.Com.Transactions;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class TransactionAndTimerTests
{
    [Fact]
    public void SystemBytesGenerationIsConcurrentAndWraps()
    {
        var wrapping = new SecsSystemBytesGenerator(uint.MaxValue - 1);
        Assert.Equal(uint.MaxValue, wrapping.Next().Value);
        Assert.Equal(0u, wrapping.Next().Value);

        var generator = new SecsSystemBytesGenerator();
        var values = new uint[10_000];
        Parallel.For(0, values.Length, index => values[index] = generator.Next().Value);
        Assert.Equal(values.Length, values.Distinct().Count());
    }

    [Fact]
    public async Task PrimarySecondaryCompletesAndDetectsDuplicateAndUnknown()
    {
        await using var manager = new SecsTransactionManager();
        var primary = Primary(10);
        var pending = manager.RegisterPrimaryAsync(primary, TimeSpan.FromSeconds(45));
        var secondary = Secondary(10);
        Assert.Equal(SecsTransactionCompletionStatus.Completed, manager.TryComplete(secondary));
        Assert.Same(secondary, await pending);
        Assert.Equal(SecsTransactionCompletionStatus.Duplicate, manager.TryComplete(secondary));
        Assert.Equal(SecsTransactionCompletionStatus.UnknownSystemBytes, manager.TryComplete(Secondary(11)));
    }

    [Fact]
    public async Task OneThousandConcurrentTransactionsCompleteOutOfOrder()
    {
        await using var manager = new SecsTransactionManager();
        var waits = Enumerable.Range(1, 1_000).Select(index => manager.RegisterPrimaryAsync(Primary((uint)index), TimeSpan.FromSeconds(45))).ToArray();
        foreach (var index in Enumerable.Range(1, 1_000).Reverse()) Assert.Equal(SecsTransactionCompletionStatus.Completed, manager.TryComplete(Secondary((uint)index)));
        var results = await Task.WhenAll(waits);
        Assert.Equal(1_000, results.Length);
        Assert.Equal(0, manager.OutstandingCount);
    }

    [Fact]
    public async Task InvalidCorrelationDoesNotConsumeTransaction()
    {
        await using var manager = new SecsTransactionManager();
        var pending = manager.RegisterPrimaryAsync(Primary(20), TimeSpan.FromSeconds(45));
        var wrongStream = new SecsMessage(new SecsSessionId(1), new SecsStream(2), new SecsFunction(2), false, new SecsSystemBytes(20));
        Assert.Equal(SecsTransactionCompletionStatus.InvalidCorrelation, manager.TryComplete(wrongStream));
        Assert.Equal(1, manager.OutstandingCount);
        manager.TryComplete(Secondary(20));
        _ = await pending;
    }

    [Fact]
    public async Task LowerReplyFunctionDoesNotConsumeTransaction()
    {
        await using var manager = new SecsTransactionManager();
        var primary = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(3), true, new SecsSystemBytes(21));
        var pending = manager.RegisterPrimaryAsync(primary, TimeSpan.FromSeconds(45));
        var wrongFunction = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(2), false, new SecsSystemBytes(21));
        Assert.Equal(SecsTransactionCompletionStatus.InvalidCorrelation, manager.TryComplete(wrongFunction));
        Assert.Equal(1, manager.OutstandingCount);
        var correctFunction = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(4), false, new SecsSystemBytes(21));
        manager.TryComplete(correctFunction);
        _ = await pending;
    }

    [Fact]
    public async Task UnrelatedHigherSecondaryDoesNotConsumeTransaction()
    {
        await using var manager = new SecsTransactionManager();
        var pending = manager.RegisterPrimaryAsync(Primary(23), TimeSpan.FromSeconds(45));
        var unrelated = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(4), false, new SecsSystemBytes(23));

        Assert.Equal(SecsTransactionCompletionStatus.InvalidCorrelation, manager.TryComplete(unrelated));
        Assert.Equal(1, manager.OutstandingCount);
        Assert.Equal(SecsTransactionCompletionStatus.Completed, manager.TryComplete(Secondary(23)));
        Assert.Equal((byte)2, (await pending).Function.Value);
    }

    [Fact]
    public async Task FunctionZeroReplyCanCompleteAnOpenTransaction()
    {
        await using var manager = new SecsTransactionManager();
        var pending = manager.RegisterPrimaryAsync(Primary(22), TimeSpan.FromSeconds(45));
        var functionZero = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(0), false, new SecsSystemBytes(22));
        Assert.Equal(SecsTransactionCompletionStatus.Completed, manager.TryComplete(functionZero));
        Assert.Equal((byte)0, (await pending).Function.Value);
    }

    [Fact]
    public async Task T3ExpiresOnlyWhenVirtualTimeAdvances()
    {
        var time = new ManualTimeProvider();
        await using var manager = new SecsTransactionManager(time);
        var pending = manager.RegisterPrimaryAsync(Primary(30), TimeSpan.FromSeconds(45));
        time.Advance(TimeSpan.FromSeconds(44));
        Assert.False(pending.IsCompleted);
        time.Advance(TimeSpan.FromSeconds(1));
        var error = await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => pending);
        Assert.Equal(30u, error.SystemBytes.Value);
        Assert.Equal(0, manager.OutstandingCount);
    }

    [Fact]
    public async Task CancellationAndConnectionAbortCleanWaiters()
    {
        await using var manager = new SecsTransactionManager();
        using var cancellation = new CancellationTokenSource();
        var canceled = manager.RegisterPrimaryAsync(Primary(40), TimeSpan.FromSeconds(45), cancellation.Token);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);

        var aborted = manager.RegisterPrimaryAsync(Primary(41), TimeSpan.FromSeconds(45));
        manager.AbortAll(new IOException("connection lost"));
        await Assert.ThrowsAsync<IOException>(() => aborted);
        Assert.Equal(0, manager.OutstandingCount);
    }

    [Fact]
    public async Task IndividualAbortDoesNotFailOtherTransactions()
    {
        await using var manager = new SecsTransactionManager();
        var failed = manager.RegisterPrimaryAsync(Primary(42), TimeSpan.FromSeconds(45));
        var healthy = manager.RegisterPrimaryAsync(Primary(43), TimeSpan.FromSeconds(45));
        var sendError = new IOException("send failed");

        Assert.True(manager.TryAbort(new SecsSystemBytes(42), sendError));
        Assert.Same(sendError, await Assert.ThrowsAsync<IOException>(() => failed));
        Assert.Equal(1, manager.OutstandingCount);
        Assert.Equal(SecsTransactionCompletionStatus.Completed, manager.TryComplete(Secondary(43)));
        Assert.Equal(43u, (await healthy).SystemBytes.Value);
    }

    [Fact]
    public async Task AllocationSkipsAnOpenSystemBytesCollision()
    {
        var generator = new SecsSystemBytesGenerator(0);
        await using var manager = new SecsTransactionManager(generator: generator);
        var pending = manager.RegisterPrimaryAsync(Primary(1), TimeSpan.FromSeconds(45));
        Assert.Equal(2u, manager.AllocateSystemBytes().Value);
        manager.TryComplete(Secondary(1));
        _ = await pending;
    }

    [Fact]
    public async Task ControlTransactionCompletesAndT6UsesVirtualTime()
    {
        var time = new ManualTimeProvider();
        await using var manager = new HsmsControlTransactionManager(time);
        var request = Control(HsmsSType.SelectRequest, 50);
        var completed = manager.RegisterAsync(request, TimeSpan.FromSeconds(5));
        Assert.True(manager.TryComplete(Control(HsmsSType.SelectResponse, 50)));
        Assert.Equal(HsmsSType.SelectResponse, (await completed).SType);

        var timedOut = manager.RegisterAsync(Control(HsmsSType.LinktestRequest, 51), TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(5));
        var error = await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => timedOut);
        Assert.Equal("T6", error.TimerName);
    }

    [Fact]
    public async Task ControlResponseMustMatchRequestSessionId()
    {
        await using var manager = new HsmsControlTransactionManager();
        var request = Control(HsmsSType.SelectRequest, 55);
        var pending = manager.RegisterAsync(request, TimeSpan.FromSeconds(5));
        var wrongSession = new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.SelectResponse, new SecsSystemBytes(55), sessionId: 1));
        Assert.False(manager.TryComplete(wrongSession));
        Assert.Equal(1, manager.OutstandingCount);
        Assert.True(manager.TryComplete(Control(HsmsSType.SelectResponse, 55)));
        _ = await pending;
    }

    [Fact]
    public async Task IndividualControlAbortDoesNotFailOtherTransactions()
    {
        await using var manager = new HsmsControlTransactionManager();
        var failed = manager.RegisterAsync(Control(HsmsSType.SelectRequest, 52), TimeSpan.FromSeconds(5));
        var healthy = manager.RegisterAsync(Control(HsmsSType.LinktestRequest, 53), TimeSpan.FromSeconds(5));
        var sendError = new IOException("send failed");

        Assert.True(manager.TryAbort(new SecsSystemBytes(52), sendError));
        Assert.Same(sendError, await Assert.ThrowsAsync<IOException>(() => failed));
        Assert.True(manager.TryComplete(Control(HsmsSType.LinktestResponse, 53)));
        Assert.Equal(HsmsSType.LinktestResponse, (await healthy).SType);
    }

    [Fact]
    public async Task ThrowingDiagnosticSinkCannotFaultTimeoutMonitor()
    {
        var time = new ManualTimeProvider();
        await using var manager = new SecsTransactionManager(time, diagnostics: new ThrowingSink());
        var pending = manager.RegisterPrimaryAsync(Primary(54), TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => pending);
        Assert.Equal(0, manager.OutstandingCount);
    }

    [Fact]
    public async Task BlockingT3DiagnosticRunsAfterOwnedMonitorDrain()
    {
        var time = new ManualTimeProvider();
        using var sink = new BlockingSink();
        var manager = new SecsTransactionManager(time, diagnostics: sink);
        var pending = manager.RegisterPrimaryAsync(Primary(60), TimeSpan.FromSeconds(1));

        var expiration = Task.Run(() => time.Advance(TimeSpan.FromSeconds(1)));
        await sink.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var disposal = manager.DisposeAsync().AsTask();
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));

        sink.Release();
        await expiration.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => pending);
        Assert.Equal(0, manager.OutstandingCount);
        Assert.Equal(0, time.ScheduledTimerCount);
    }

    [Fact]
    public async Task BlockingT6DiagnosticRunsAfterOwnedMonitorDrain()
    {
        var time = new ManualTimeProvider();
        using var sink = new BlockingSink();
        var manager = new HsmsControlTransactionManager(time, sink);
        var pending = manager.RegisterAsync(Control(HsmsSType.LinktestRequest, 61), TimeSpan.FromSeconds(1));

        var expiration = Task.Run(() => time.Advance(TimeSpan.FromSeconds(1)));
        await sink.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var disposal = manager.DisposeAsync().AsTask();
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));

        sink.Release();
        await expiration.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => pending);
        Assert.Equal(0, manager.OutstandingCount);
        Assert.Equal(0, time.ScheduledTimerCount);
    }

    [Fact]
    public async Task MassT3TimeoutsHaveAtMostOneBlockedExternalDiagnosticCallback()
    {
        const int count = 256;
        var time = new ManualTimeProvider();
        using var sink = new CountingBlockingSink();
        var manager = new SecsTransactionManager(time, diagnostics: sink);
        var pending = Enumerable.Range(0, count)
            .Select(index => manager.RegisterPrimaryAsync(Primary((uint)(1_000 + index)), TimeSpan.FromSeconds(1)))
            .ToArray();

        var firstExpiration = Task.Factory.StartNew(
            () => time.Advance(TimeSpan.FromSeconds(1)),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        await sink.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var remainingExpirations = Task.Factory.StartNew(
            () => time.Advance(TimeSpan.FromSeconds(1)),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        await WaitUntilAsync(() => manager.OutstandingCount == 0, TimeSpan.FromSeconds(5));

        Assert.Equal(1, sink.MaximumConcurrentCallbacks);
        await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        sink.Release();
        await Task.WhenAll(firstExpiration, remainingExpirations).WaitAsync(TimeSpan.FromSeconds(5));
        foreach (var transaction in pending)
            await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => transaction);
    }

    [Fact]
    public async Task T3TimeoutDiagnosticCanSynchronouslyDisposeItsManager()
    {
        var time = new ManualTimeProvider();
        var sink = new CallbackSink();
        var manager = new SecsTransactionManager(time, diagnostics: sink);
        sink.Callback = () => manager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        var pending = manager.RegisterPrimaryAsync(Primary(62), TimeSpan.FromSeconds(1));

        await Task.Run(() => time.Advance(TimeSpan.FromSeconds(1))).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => pending);
        await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, manager.OutstandingCount);
    }

    [Fact]
    public async Task T6TimeoutDiagnosticCanSynchronouslyDisposeItsManager()
    {
        var time = new ManualTimeProvider();
        var sink = new CallbackSink();
        var manager = new HsmsControlTransactionManager(time, sink);
        sink.Callback = () => manager.DisposeAsync().AsTask().GetAwaiter().GetResult();
        var pending = manager.RegisterAsync(Control(HsmsSType.LinktestRequest, 63), TimeSpan.FromSeconds(1));

        await Task.Run(() => time.Advance(TimeSpan.FromSeconds(1))).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => pending);
        await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, manager.OutstandingCount);
    }

    [Fact]
    public async Task T3CallerCancellationAndTimeoutRaceHasOneTerminalOutcome()
    {
        for (var index = 0; index < 25; index++)
        {
            var time = new ManualTimeProvider();
            var manager = new SecsTransactionManager(time);
            using var cancellation = new CancellationTokenSource();
            var pending = manager.RegisterPrimaryAsync(Primary((uint)(100 + index)), TimeSpan.FromSeconds(1), cancellation.Token);

            await Task.WhenAll(
                Task.Run(cancellation.Cancel),
                Task.Run(() => time.Advance(TimeSpan.FromSeconds(1))));

            var outcome = await Record.ExceptionAsync(async () => await pending);
            Assert.True(outcome is OperationCanceledException or SecsTransactionTimeoutException, outcome?.ToString());

            await manager.DisposeAsync();
            Assert.Equal(0, manager.OutstandingCount);
            Assert.Equal(0, time.ScheduledTimerCount);
        }
    }

    [Fact]
    public async Task T6CallerCancellationAndTimeoutRaceHasOneTerminalOutcome()
    {
        for (var index = 0; index < 25; index++)
        {
            var time = new ManualTimeProvider();
            var manager = new HsmsControlTransactionManager(time);
            using var cancellation = new CancellationTokenSource();
            var pending = manager.RegisterAsync(Control(HsmsSType.LinktestRequest, (uint)(200 + index)), TimeSpan.FromSeconds(1), cancellation.Token);

            await Task.WhenAll(
                Task.Run(cancellation.Cancel),
                Task.Run(() => time.Advance(TimeSpan.FromSeconds(1))));

            var outcome = await Record.ExceptionAsync(async () => await pending);
            Assert.True(outcome is OperationCanceledException or HsmsTimerExpiredException, outcome?.ToString());

            await manager.DisposeAsync();
            Assert.Equal(0, manager.OutstandingCount);
            Assert.Equal(0, time.ScheduledTimerCount);
        }
    }

    [Fact]
    public async Task ConcurrentSecsDisposeCallsShareTheSameCompletion()
    {
        var time = new ManualTimeProvider();
        var manager = new SecsTransactionManager(time);
        var pending = manager.RegisterPrimaryAsync(Primary(300), TimeSpan.FromMinutes(1));

        var disposals = Enumerable.Range(0, 16)
            .Select(_ => Task.Run(async () => await manager.DisposeAsync()))
            .ToArray();

        await Task.WhenAll(disposals).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
        Assert.All(disposals, disposal => Assert.Equal(TaskStatus.RanToCompletion, disposal.Status));
        Assert.Equal(0, manager.OutstandingCount);
        Assert.Equal(0, time.ScheduledTimerCount);
    }

    [Fact]
    public async Task ConcurrentControlDisposeCallsShareTheSameCompletion()
    {
        var time = new ManualTimeProvider();
        var manager = new HsmsControlTransactionManager(time);
        var pending = manager.RegisterAsync(Control(HsmsSType.SelectRequest, 301), TimeSpan.FromMinutes(1));

        var disposals = Enumerable.Range(0, 16)
            .Select(_ => Task.Run(async () => await manager.DisposeAsync()))
            .ToArray();

        await Task.WhenAll(disposals).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
        Assert.All(disposals, disposal => Assert.Equal(TaskStatus.RanToCompletion, disposal.Status));
        Assert.Equal(0, manager.OutstandingCount);
        Assert.Equal(0, time.ScheduledTimerCount);
    }

    [Fact]
    public async Task T3RegistrationAndDisposeRaceCannotLeaveAnOutstandingTransaction()
    {
        for (var index = 0; index < 50; index++)
        {
            var time = new ManualTimeProvider();
            var manager = new SecsTransactionManager(time);
            using var start = new ManualResetEventSlim();
            Task<SecsMessage>? pending = null;
            Exception? registrationError = null;

            var registration = Task.Run(() =>
            {
                start.Wait();
                try { pending = manager.RegisterPrimaryAsync(Primary((uint)(400 + index)), TimeSpan.FromMinutes(1)); }
                catch (Exception exception) { registrationError = exception; }
            });
            var disposal = Task.Run(async () =>
            {
                start.Wait();
                await manager.DisposeAsync();
            });

            start.Set();
            await Task.WhenAll(registration, disposal).WaitAsync(TimeSpan.FromSeconds(5));

            if (pending is not null) await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
            else Assert.IsType<ObjectDisposedException>(registrationError);
            Assert.Equal(0, manager.OutstandingCount);
            Assert.Equal(0, time.ScheduledTimerCount);
        }
    }

    [Fact]
    public async Task T6RegistrationAndDisposeRaceCannotLeaveAnOutstandingTransaction()
    {
        for (var index = 0; index < 50; index++)
        {
            var time = new ManualTimeProvider();
            var manager = new HsmsControlTransactionManager(time);
            using var start = new ManualResetEventSlim();
            Task<HsmsControlMessage>? pending = null;
            Exception? registrationError = null;

            var registration = Task.Run(() =>
            {
                start.Wait();
                try { pending = manager.RegisterAsync(Control(HsmsSType.LinktestRequest, (uint)(500 + index)), TimeSpan.FromMinutes(1)); }
                catch (Exception exception) { registrationError = exception; }
            });
            var disposal = Task.Run(async () =>
            {
                start.Wait();
                await manager.DisposeAsync();
            });

            start.Set();
            await Task.WhenAll(registration, disposal).WaitAsync(TimeSpan.FromSeconds(5));

            if (pending is not null) await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
            else Assert.IsType<ObjectDisposedException>(registrationError);
            Assert.Equal(0, manager.OutstandingCount);
            Assert.Equal(0, time.ScheduledTimerCount);
        }
    }

    [Theory]
    [InlineData(HsmsTimerKind.T5, 10)]
    [InlineData(HsmsTimerKind.T7, 10)]
    public async Task SessionTimersUseVirtualTime(HsmsTimerKind kind, int seconds)
    {
        var time = new ManualTimeProvider();
        var scheduler = new HsmsTimerScheduler(new HsmsTimerOptions(), time);
        var task = scheduler.WaitForExpirationAsync(kind);
        time.Advance(TimeSpan.FromSeconds(seconds - 1));
        Assert.False(task.IsCompleted);
        time.Advance(TimeSpan.FromSeconds(1));
        var error = await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => task);
        Assert.Equal(kind.ToString(), error.TimerName);
    }

    [Fact]
    public async Task T8ExpiresBetweenPartialFrameBytesUsingVirtualTime()
    {
        var time = new ManualTimeProvider();
        var stream = new OneByteThenBlockStream();
        var read = new HsmsFrameCodec().ReadAsync(stream, TimeSpan.FromSeconds(5), time);
        await stream.SecondReadStarted.Task;
        time.Advance(TimeSpan.FromSeconds(5));
        var error = await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => read);
        Assert.Equal("T8", error.TimerName);
    }

    private static SecsMessage Primary(uint systemBytes) => new(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), true, new SecsSystemBytes(systemBytes));
    private static SecsMessage Secondary(uint systemBytes) => new(new SecsSessionId(1), new SecsStream(1), new SecsFunction(2), false, new SecsSystemBytes(systemBytes));
    private static HsmsControlMessage Control(HsmsSType type, uint systemBytes) => new(HsmsHeader.CreateControl(type, new SecsSystemBytes(systemBytes)));

    private sealed class OneByteThenBlockStream : Stream
    {
        private int _reads;
        public TaskCompletionSource SecondReadStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _reads) == 1) { buffer.Span[0] = 0; return 1; }
            SecondReadStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class ThrowingSink : Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink
    {
        public void Emit(Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticEvent diagnosticEvent) => throw new InvalidOperationException("sink failure");
    }

    private sealed class BlockingSink : Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink, IDisposable
    {
        private readonly ManualResetEventSlim _release = new();

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Emit(Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticKind.Timeout) return;
            Entered.TrySetResult();
            if (!_release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("The test did not release the diagnostic sink.");
        }

        public void Release() => _release.Set();
        public void Dispose() => _release.Dispose();
    }

    private sealed class CallbackSink : Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink
    {
        public Action? Callback { get; set; }

        public void Emit(Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind == Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticKind.Timeout)
                Callback?.Invoke();
        }
    }

    private sealed class CountingBlockingSink : Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink, IDisposable
    {
        private readonly ManualResetEventSlim _release = new();
        private int _active;
        private int _maximum;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int MaximumConcurrentCallbacks => Volatile.Read(ref _maximum);

        public void Emit(Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticKind.Timeout) return;
            var active = Interlocked.Increment(ref _active);
            UpdateMaximum(active);
            Entered.TrySetResult();
            try
            {
                if (!_release.Wait(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("The test did not release the diagnostic sink.");
            }
            finally { Interlocked.Decrement(ref _active); }
        }

        public void Release() => _release.Set();
        public void Dispose() => _release.Dispose();

        private void UpdateMaximum(int candidate)
        {
            var current = Volatile.Read(ref _maximum);
            while (candidate > current)
            {
                var observed = Interlocked.CompareExchange(ref _maximum, candidate, current);
                if (observed == current) return;
                current = observed;
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);
        while (!condition())
        {
            cancellation.Token.ThrowIfCancellationRequested();
            await Task.Yield();
        }
    }
}
