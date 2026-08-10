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
}
