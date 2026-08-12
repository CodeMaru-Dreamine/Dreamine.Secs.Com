using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Codecs;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class HsmsDiagnosticReentrancyTests
{
    [Fact]
    public async Task ConnectionAttemptDiagnosticCanSynchronouslyDisconnect()
    {
        var sink = new CallbackSink(SecsDiagnosticKind.ConnectionAttempt);
        var session = CreateSession(ReservePort(), SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: sink);
        sink.Callback = () => session.DisconnectAsync().GetAwaiter().GetResult();

        var connect = session.ConnectAsync();
        await sink.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Null(sink.Error);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect.WaitAsync(TimeSpan.FromSeconds(5)));
        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ConnectionClosedDiagnosticCanSynchronouslyDisconnect()
    {
        var sink = new CallbackSink(SecsDiagnosticKind.ConnectionClosed);
        await using var passive = CreateSession(ReservePort(), SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: sink);
        await using var active = CreateSession(GetPort(passive), SecsConnectionMode.Active, SecsRole.Host);
        sink.Callback = () => passive.DisconnectAsync().GetAwaiter().GetResult();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);

        await active.DisconnectAsync(timeout.Token);
        await sink.Completed.Task.WaitAsync(timeout.Token);

        Assert.Null(sink.Error);
        Assert.Equal(ConnectionState.Disconnected, passive.State);
    }

    [Fact]
    public async Task FrameSentDiagnosticCanSynchronouslySendAnotherMessage()
    {
        var port = ReservePort();
        var sink = new ArmedFrameSentSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: sink);
        sink.Session = active;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        var received = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        passive.MessageReceived += (_, _) =>
        {
            if (Interlocked.Increment(ref count) == 2) received.TrySetResult(2);
        };
        sink.Arm(new SecsMessage(
            new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false,
            new SecsSystemBytes(0x10203041)));

        await active.SendAsync(new SecsMessage(
            new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false,
            new SecsSystemBytes(0x10203040)), timeout.Token).WaitAsync(timeout.Token);
        await sink.Completed.Task.WaitAsync(timeout.Token);
        await received.Task.WaitAsync(timeout.Token);

        Assert.Null(sink.Error);
    }

    [Fact]
    public async Task ReconnectDiagnosticCanSynchronouslyDispose()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var sink = new ReconnectDisposeSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: time, diagnostics: sink);
        sink.Session = active;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            await active.SelectAsync(timeout.Token);
            await passive.DisconnectAsync(timeout.Token);
            await WaitUntilAsync(() => time.ScheduledTimerCount > 0, timeout.Token);

            time.Advance(TimeSpan.FromSeconds(10));
            await sink.Completed.Task.WaitAsync(timeout.Token);

            Assert.Null(sink.Error);
            await active.DisposeAsync().AsTask().WaitAsync(timeout.Token);
        }
        finally
        {
            try { await active.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task PassiveRemoteCloseReentersActualTcpAcceptWithoutAdvancingT5()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        await using var passive = CreateSession(
            port,
            SecsConnectionMode.Passive,
            SecsRole.Equipment,
            timeProvider: time);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var firstConnect = passive.ConnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
        using (var firstPeer = new TcpClient())
        {
            await firstPeer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
            await firstConnect.WaitAsync(timeout.Token);
        }

        await WaitUntilAsync(() => IsPassiveListenerActive(passive), timeout.Token);
        using var secondPeer = new TcpClient();
        await secondPeer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Connected, timeout.Token);

        Assert.Equal(ConnectionState.Connected, passive.State);
    }

    [Fact]
    public async Task PassiveReconnectAttemptDiagnosticRunsAfterWorkerPublicationAndRunGateRelease()
    {
        var port = ReservePort();
        var diagnostics = new ReconnectBoundarySink();
        var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: diagnostics);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        diagnostics.Session = passive;
        diagnostics.RunGate = GetRunGate(passive);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            diagnostics.Arm();

            await active.DisconnectAsync(timeout.Token);
            await diagnostics.Completed.Task.WaitAsync(timeout.Token);

            Assert.True(diagnostics.RunGateAvailable);
            Assert.Null(diagnostics.Error);
            await WaitUntilAsync(() => passive.State == ConnectionState.Disconnected, timeout.Token);
            await WaitUntilAsync(() => GetOwnedWorkerCount(passive) == 0, timeout.Token);
        }
        finally
        {
            try { await passive.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task PrebufferedReceiveDiagnosticCanSynchronouslyDisconnectAfterConnectPublication()
    {
        var port = ReservePort();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        var diagnostics = new BoundedDisconnectSink(SecsDiagnosticKind.FrameReceived);
        var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: diagnostics);
        diagnostics.Session = active;
        var frame = new HsmsFrameCodec().Encode(new HsmsControlMessage(
            HsmsHeader.CreateControl(HsmsSType.LinktestRequest, new SecsSystemBytes(0x10203040))));
        var peerReady = new TaskCompletionSource<TcpClient>(TaskCreationOptions.RunContinuationsAsynchronously);
        var peerThread = new Thread(() =>
        {
            try
            {
                var peer = listener.AcceptTcpClient();
                peer.GetStream().Write(frame);
                peerReady.TrySetResult(peer);
            }
            catch (Exception exception) { peerReady.TrySetException(exception); }
        }) { IsBackground = true };
        peerThread.Start();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var connect = active.ConnectAsync(timeout.Token);
            using var peer = await peerReady.Task.WaitAsync(timeout.Token);
            await diagnostics.Completed.Task.WaitAsync(timeout.Token);
            await connect.WaitAsync(timeout.Token);

            Assert.Null(diagnostics.Error);
            Assert.Equal(ConnectionState.Disconnected, active.State);
        }
        finally
        {
            listener.Stop();
            try { await active.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task FirstConnectionT7DiagnosticCanSynchronouslyDisposeWithoutSelfDrainingWorker()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var sink = new CallbackSink(SecsDiagnosticKind.Timeout);
        var session = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            timeProvider: time,
            diagnostics: sink);
        sink.Callback = () => session.DisposeAsync().AsTask().GetAwaiter().GetResult();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        listener.Start();
        var accepted = listener.AcceptTcpClientAsync(timeout.Token).AsTask();

        try
        {
            await session.ConnectAsync(timeout.Token);
            using var peer = await accepted.WaitAsync(timeout.Token);
            await WaitUntilAsync(() => time.ScheduledTimerCount > 0, timeout.Token);

            time.Advance(TimeSpan.FromSeconds(10));
            await sink.Completed.Task.WaitAsync(timeout.Token);

            Assert.Null(sink.Error);
            await WaitUntilAsync(() => GetOwnedWorkerCount(session) == 0, timeout.Token);
            await WaitUntilAsync(() => GetLifetimeCleanupStarted(session) == 1, timeout.Token);
            Assert.Equal(0, GetOutstandingCount(session, "_transactions"));
            Assert.Equal(0, GetOutstandingCount(session, "_controlTransactions"));
            await session.DisposeAsync().AsTask().WaitAsync(timeout.Token);
        }
        finally
        {
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task QueuedDataSendCannotCrossInboundDeselectTransition()
    {
        var port = ReservePort();
        var diagnostics = new ControlTransitionSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: diagnostics);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        diagnostics.Arm();

        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, _) => received.TrySetResult();
        var sendGate = GetSendGate(active);
        await sendGate.WaitAsync(timeout.Token);
        Task send;
        Task deselect;
        try
        {
            send = active.SendAsync(new SecsMessage(
                new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false,
                active.AllocateSystemBytes(), new SecsAsciiItem("ORDERED")), timeout.Token);
            Assert.False(send.IsCompleted);

            deselect = passive.DeselectAsync(timeout.Token);
            await diagnostics.FrameReceived.Task.WaitAsync(timeout.Token);
            var crossed = await CompletesWithinAsync(diagnostics.LeftSelected.Task, TimeSpan.FromMilliseconds(250));
            Assert.False(crossed);
            Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
        }
        finally { sendGate.Release(); }

        await send.WaitAsync(timeout.Token);
        await received.Task.WaitAsync(timeout.Token);
        await deselect.WaitAsync(timeout.Token);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, active.HsmsState);
    }

    [Fact]
    public async Task InboundDeselectQueuedFirstInvalidatesCapturedDataSend()
    {
        var port = ReservePort();
        var diagnostics = new ControlTransitionSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: diagnostics);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        diagnostics.Arm();

        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, _) => received.TrySetResult();
        var sendGate = GetSendGate(active);
        await sendGate.WaitAsync(timeout.Token);
        Task deselect;
        Task send;
        try
        {
            deselect = passive.DeselectAsync(timeout.Token);
            await diagnostics.FrameReceived.Task.WaitAsync(timeout.Token);
            for (var index = 0; index < 100; index++) await Task.Yield();
            send = active.SendAsync(new SecsMessage(
                new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false,
                active.AllocateSystemBytes(), new SecsAsciiItem("STALE")), timeout.Token);
            Assert.False(send.IsCompleted);
        }
        finally { sendGate.Release(); }

        var error = await Record.ExceptionAsync(() => send);
        Assert.True(error is HsmsStateException or IOException, error?.ToString());
        await deselect.WaitAsync(timeout.Token);
        Assert.False(received.Task.IsCompleted);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, active.HsmsState);
    }

    [Fact]
    public async Task LocalSeparateQueuedFirstInvalidatesCapturedDataSend()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, _) => received.TrySetResult();
        var sendGate = GetSendGate(active);
        await sendGate.WaitAsync(timeout.Token);
        Task separate;
        Task send;
        try
        {
            separate = active.SeparateAsync(timeout.Token);
            Assert.False(separate.IsCompleted);
            send = active.SendAsync(new SecsMessage(
                new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false,
                active.AllocateSystemBytes(), new SecsAsciiItem("STALE")), timeout.Token);
            Assert.False(send.IsCompleted);
        }
        finally { sendGate.Release(); }

        await separate.WaitAsync(timeout.Token);
        var error = await Record.ExceptionAsync(() => send);
        Assert.True(error is HsmsStateException or IOException, error?.ToString());
        Assert.False(received.Task.IsCompleted);
        Assert.Equal(ConnectionState.Disconnected, active.State);
    }

    [Fact]
    public async Task LocalSeparateStateDiagnosticRunsAfterSendSerializationRelease()
    {
        var port = ReservePort();
        var diagnostics = new SendGateProbeSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: diagnostics);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        diagnostics.Arm(GetSendGate(active));

        await active.SeparateAsync(timeout.Token);
        await diagnostics.Completed.Task.WaitAsync(timeout.Token);

        Assert.True(diagnostics.AcquiredSendGate);
    }

    [Fact]
    public async Task FirstDisposeDiagnosticCanSynchronouslyCallDisposeAgain()
    {
        var port = ReservePort();
        var sink = new CallbackSink(SecsDiagnosticKind.ConnectionClosed);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: sink);
        sink.Callback = () => session.DisposeAsync().AsTask().GetAwaiter().GetResult();
        using var listener = new TcpListener(IPAddress.Loopback, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        listener.Start();
        var accepted = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
        await session.ConnectAsync(timeout.Token);
        using var peer = await accepted.WaitAsync(timeout.Token);

        var disposal = Task.Run(() => session.DisposeAsync().AsTask().GetAwaiter().GetResult());
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));
        await sink.Completed.Task.WaitAsync(timeout.Token);

        Assert.Null(sink.Error);
        await WaitUntilAsync(() => GetOwnedWorkerCount(session) == 0, timeout.Token);
        await WaitUntilAsync(() => GetLifetimeCleanupStarted(session) == 1, timeout.Token);
        Assert.Equal(0, GetOutstandingCount(session, "_transactions"));
        Assert.Equal(0, GetOutstandingCount(session, "_controlTransactions"));
        await session.DisposeAsync().AsTask().WaitAsync(timeout.Token);
    }

    [Fact]
    public async Task FailedLocalSeparateStopsRunWithoutAutoReconnect()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var diagnostics = new CountingSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            autoReconnect: true,
            timeProvider: time,
            diagnostics: diagnostics);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        var attemptsBefore = diagnostics.Count(SecsDiagnosticKind.ConnectionAttempt);

        var sendGate = GetSendGate(active);
        await sendGate.WaitAsync(timeout.Token);
        Task separate;
        try
        {
            separate = active.SeparateAsync(timeout.Token);
            Assert.False(separate.IsCompleted);
            await passive.DisconnectAsync(timeout.Token);
            await WaitUntilAsync(
                () => active.State is ConnectionState.Disconnected or ConnectionState.Faulted,
                timeout.Token);
        }
        finally { sendGate.Release(); }

        var error = await Assert.ThrowsAsync<IOException>(() => separate);
        Assert.Contains("connection changed", error.Message, StringComparison.OrdinalIgnoreCase);
        await WaitUntilAsync(() => active.State == ConnectionState.Disconnected, timeout.Token);
        Assert.Equal(0, time.ScheduledTimerCount);
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(attemptsBefore, diagnostics.Count(SecsDiagnosticKind.ConnectionAttempt));
    }

    [Fact]
    public async Task PassiveReconnectBindFailureIsTerminalAndExplicitConnectRecovers()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var diagnostics = new PassiveBindFailureSink();
        var passive = CreateSession(
            port,
            SecsConnectionMode.Passive,
            SecsRole.Equipment,
            timeProvider: time,
            diagnostics: diagnostics);
        diagnostics.Session = passive;
        using var collision = new TcpListener(IPAddress.Loopback, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var firstConnect = passive.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
            using (var firstPeer = new TcpClient())
            {
                await firstPeer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
                await firstConnect.WaitAsync(timeout.Token);
                collision.Start(1);
            }

            await diagnostics.FirstReconnectFailure.Task.WaitAsync(timeout.Token);
            await WaitUntilAsync(() => IsReconnectCompleted(passive), timeout.Token);

            Assert.Equal(ConnectionState.Faulted, passive.State);
            Assert.Equal(1, diagnostics.ReconnectFailureCount);
            Assert.Equal(2, diagnostics.ConnectionAttemptCount);
            Assert.Equal(0, time.ScheduledTimerCount);

            collision.Stop();
            var explicitConnect = passive.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
            using var secondPeer = new TcpClient();
            await secondPeer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
            await explicitConnect.WaitAsync(timeout.Token);
            Assert.Equal(ConnectionState.Connected, passive.State);
        }
        finally
        {
            collision.Stop();
            try { await passive.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task InitialPassiveBindFailureIsTerminalAndExplicitConnectRecovers()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var diagnostics = new CountingSink();
        var passive = CreateSession(
            port,
            SecsConnectionMode.Passive,
            SecsRole.Equipment,
            timeProvider: time,
            diagnostics: diagnostics);
        using var collision = new TcpListener(IPAddress.Loopback, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        collision.Start(1);
        try
        {
            await Assert.ThrowsAsync<SocketException>(() => passive.ConnectAsync(timeout.Token));
            Assert.Equal(ConnectionState.Faulted, passive.State);
            Assert.Equal(1, diagnostics.Count(SecsDiagnosticKind.ConnectionAttempt));
            Assert.Equal(0, time.ScheduledTimerCount);

            collision.Stop();
            var explicitConnect = passive.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
            using var peer = new TcpClient();
            await peer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
            await explicitConnect.WaitAsync(timeout.Token);
            Assert.Equal(ConnectionState.Connected, passive.State);
        }
        finally
        {
            collision.Stop();
            try { await passive.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task DeselectStartsExactlyOneNewT7Generation()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, timeProvider: time);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        var run = GetConnectedRun(passive);
        var before = GetT7Generation(run);

        await active.DeselectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.HsmsState == HsmsConnectionState.ConnectedNotSelected, timeout.Token);

        Assert.Equal(before + 1, GetT7Generation(run));
        Assert.Equal(1, time.ScheduledTimerCount);
    }

    [Fact]
    public async Task CanceledT7GenerationCannotCloseSelectedConnection()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        var run = GetConnectedRun(passive);
        var epoch = GetConnectionEpoch(run);
        var t7Generation = GetT7Generation(run);
        await active.SelectAsync(timeout.Token);

        var ended = await InvokeExpectedFailureAsync(passive, run, epoch, t7Generation);

        Assert.False(ended);
        Assert.Equal(ConnectionState.Connected, passive.State);
        Assert.Equal(HsmsConnectionState.Selected, passive.HsmsState);
    }

    [Fact]
    public async Task StaleT6EpochCannotCloseReconnectedRun()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: time);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        var run = GetConnectedRun(active);
        var oldEpoch = GetConnectionEpoch(run);

        await passive.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
        await WaitUntilAsync(() => time.ScheduledTimerCount > 0, timeout.Token);
        var passiveReconnect = passive.ConnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
        time.Advance(TimeSpan.FromSeconds(10));
        await Task.WhenAll(passiveReconnect, WaitUntilAsync(() => active.State == ConnectionState.Connected, timeout.Token));
        Assert.True(GetConnectionEpoch(run) > oldEpoch);

        var ended = await InvokeExpectedFailureAsync(active, run, oldEpoch, null);

        Assert.False(ended);
        Assert.Equal(ConnectionState.Connected, active.State);
    }

    [Fact]
    public void CooperativeDiagnosticBoundaryIsFifoBoundedAndCountsOverflow()
    {
        const int nestedCount = 2_048;
        var sink = new ReentrantFloodSink(nestedCount);
        var type = typeof(HsmsSession).Assembly.GetType("Dreamine.Secs.Com.Diagnostics.SafeSecsDiagnosticSink", throwOnError: true)!;
        var wrap = type.GetMethod("Wrap", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!;
        var boundary = Assert.IsAssignableFrom<ISecsDiagnosticSink>(wrap.Invoke(null, [sink]));
        sink.Boundary = boundary;

        boundary.Emit(new(SecsDiagnosticKind.ConnectionAttempt, "root"));

        var dropped = Assert.IsType<long>(type
            .GetProperty("DroppedCount", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(boundary));
        Assert.True(dropped > 0);
        Assert.Equal(nestedCount + 1, sink.Messages.Count + dropped);
        Assert.Equal("root", sink.Messages[0]);
        for (var index = 1; index < sink.Messages.Count; index++)
            Assert.Equal($"nested-{index - 1}", sink.Messages[index]);
    }

    private static HsmsSession CreateSession(
        int port,
        SecsConnectionMode mode,
        SecsRole role,
        bool autoReconnect = false,
        TimeProvider? timeProvider = null,
        ISecsDiagnosticSink? diagnostics = null) =>
        new(new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Mode = mode,
            Role = role,
            SessionId = new SecsSessionId(1),
            AutoReconnect = autoReconnect
        }, timeProvider, diagnostics);

    private static int GetPort(HsmsSession session) =>
        Assert.IsType<HsmsSessionOptions>(typeof(HsmsSession)
            .GetField("_options", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session)).Port;

    private static object GetConnectedRun(HsmsSession session) =>
        typeof(HsmsSession)
            .GetField("_connectedRun", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session)!;

    private static long GetConnectionEpoch(object run) =>
        Assert.IsType<long>(run.GetType().GetProperty("ConnectionEpoch")!.GetValue(run));

    private static long GetT7Generation(object run) =>
        Assert.IsType<long>(run.GetType()
            .GetField("_t7Generation", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(run));

    private static int GetOwnedWorkerCount(HsmsSession session)
    {
        var type = typeof(HsmsSession);
        var gate = type.GetField("_workerGate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session)!;
        var workers = type.GetField("_workers", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session)!;
        lock (gate)
            return Assert.IsType<int>(workers.GetType().GetProperty("Count")!.GetValue(workers));
    }

    private static int GetLifetimeCleanupStarted(HsmsSession session) =>
        Assert.IsType<int>(typeof(HsmsSession)
            .GetField("_lifetimeCleanupStarted", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session));

    private static bool IsPassiveListenerActive(HsmsSession session) =>
        typeof(HsmsSession)
            .GetField("_listener", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session) is TcpListener;

    private static int GetOutstandingCount(HsmsSession session, string managerField)
    {
        var manager = typeof(HsmsSession)
            .GetField(managerField, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session)!;
        return Assert.IsType<int>(manager.GetType().GetProperty("OutstandingCount")!.GetValue(manager));
    }

    private static SemaphoreSlim GetSendGate(HsmsSession session) =>
        Assert.IsType<SemaphoreSlim>(typeof(HsmsSession)
            .GetField("_sendGate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session));

    private static object GetRunGate(HsmsSession session) =>
        typeof(HsmsSession)
            .GetField("_runGate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session)!;

    private static bool IsReconnectCompleted(HsmsSession session)
    {
        var run = typeof(HsmsSession)
            .GetField("_currentRun", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session);
        if (run is null) return true;
        return run.GetType().GetProperty("ReconnectTask")!.GetValue(run) is Task { IsCompleted: true };
    }

    private static async Task<bool> CompletesWithinAsync(Task task, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);
        try { await task.WaitAsync(cancellation.Token); return true; }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { return false; }
    }

    private static async Task<bool> InvokeExpectedFailureAsync(
        HsmsSession session,
        object run,
        long epoch,
        long? t7Generation)
    {
        var method = typeof(HsmsSession)
            .GetMethods(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            .Single(candidate => candidate.Name == "FailConnectionAsync" && candidate.GetParameters().Length == 4);
        var task = Assert.IsType<Task<bool>>(method.Invoke(session, [
            run,
            epoch,
            new HsmsTimerExpiredException(t7Generation is null ? "T6" : "T7", TimeSpan.FromSeconds(10)),
            t7Generation
        ]));
        return await task;
    }

    private static async Task ConnectPairAsync(HsmsSession passive, HsmsSession active, CancellationToken cancellationToken)
    {
        var passiveConnect = passive.ConnectAsync(cancellationToken);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, cancellationToken);
        await active.ConnectAsync(cancellationToken);
        await passiveConnect;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
        }
    }

    private static int ReservePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class CallbackSink(SecsDiagnosticKind kind) : ISecsDiagnosticSink
    {
        private int _entered;
        public Action? Callback { get; set; }
        public Exception? Error { get; private set; }
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != kind || Interlocked.Exchange(ref _entered, 1) != 0) return;
            try { Callback!(); }
            catch (Exception exception) { Error = exception; }
            finally { Completed.TrySetResult(); }
        }
    }

    private sealed class ArmedFrameSentSink : ISecsDiagnosticSink
    {
        private int _armed;
        private SecsMessage? _second;
        public HsmsSession? Session { get; set; }
        public Exception? Error { get; private set; }
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm(SecsMessage second)
        {
            _second = second;
            Volatile.Write(ref _armed, 1);
        }

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.FrameSent || Interlocked.Exchange(ref _armed, 0) != 1) return;
            try { Session!.SendAsync(_second!).GetAwaiter().GetResult(); }
            catch (Exception exception) { Error = exception; }
            finally { Completed.TrySetResult(); }
        }
    }

    private sealed class ReconnectDisposeSink : ISecsDiagnosticSink
    {
        private int _entered;
        public HsmsSession? Session { get; set; }
        public Exception? Error { get; private set; }
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.ConnectionClosed ||
                !diagnosticEvent.Message.StartsWith("Reconnect failed:", StringComparison.Ordinal) ||
                Interlocked.Exchange(ref _entered, 1) != 0) return;
            try { Session!.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
            catch (Exception exception) { Error = exception; }
            finally { Completed.TrySetResult(); }
        }
    }

    private sealed class ReconnectBoundarySink : ISecsDiagnosticSink
    {
        private int _armed;
        public HsmsSession? Session { get; set; }
        public object? RunGate { get; set; }
        public bool RunGateAvailable { get; private set; }
        public Exception? Error { get; private set; }
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.ConnectionAttempt ||
                Interlocked.Exchange(ref _armed, 0) != 1) return;
            try
            {
                RunGateAvailable = Task.Run(() =>
                {
                    lock (RunGate!) return true;
                }).Wait(TimeSpan.FromMilliseconds(500));
                Session!.DisconnectAsync().GetAwaiter().GetResult();
            }
            catch (Exception exception) { Error = exception; }
            finally { Completed.TrySetResult(); }
        }
    }

    private sealed class BoundedDisconnectSink(SecsDiagnosticKind kind) : ISecsDiagnosticSink
    {
        private int _entered;
        public HsmsSession? Session { get; set; }
        public Exception? Error { get; private set; }
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != kind || Interlocked.Exchange(ref _entered, 1) != 0) return;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            try { Session!.DisconnectAsync(timeout.Token).GetAwaiter().GetResult(); }
            catch (Exception exception) { Error = exception; }
            finally { Completed.TrySetResult(); }
        }
    }

    private sealed class ControlTransitionSink : ISecsDiagnosticSink
    {
        private int _armed;
        public TaskCompletionSource FrameReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource LeftSelected { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (Volatile.Read(ref _armed) == 0) return;
            if (diagnosticEvent.Kind == SecsDiagnosticKind.FrameReceived) FrameReceived.TrySetResult();
            if (diagnosticEvent.Kind == SecsDiagnosticKind.StateChanged &&
                diagnosticEvent.State == HsmsConnectionState.ConnectedNotSelected)
                LeftSelected.TrySetResult();
        }
    }

    private sealed class SendGateProbeSink : ISecsDiagnosticSink
    {
        private int _armed;
        private SemaphoreSlim? _sendGate;
        public bool AcquiredSendGate { get; private set; }
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm(SemaphoreSlim sendGate)
        {
            _sendGate = sendGate;
            Volatile.Write(ref _armed, 1);
        }

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.StateChanged ||
                diagnosticEvent.State != HsmsConnectionState.ConnectedNotSelected ||
                Interlocked.Exchange(ref _armed, 0) != 1) return;
            AcquiredSendGate = _sendGate!.Wait(0);
            if (AcquiredSendGate) _sendGate.Release();
            Completed.TrySetResult();
        }
    }

    private sealed class CountingSink : ISecsDiagnosticSink
    {
        private readonly object _gate = new();
        private readonly List<SecsDiagnosticEvent> _events = new();

        public int Count(SecsDiagnosticKind kind)
        {
            lock (_gate) return _events.Count(item => item.Kind == kind);
        }

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            lock (_gate) _events.Add(diagnosticEvent);
        }
    }

    private sealed class PassiveBindFailureSink : ISecsDiagnosticSink
    {
        private int _connectionAttempts;
        private int _reconnectFailures;
        public HsmsSession? Session { get; set; }
        public int ConnectionAttemptCount => Volatile.Read(ref _connectionAttempts);
        public int ReconnectFailureCount => Volatile.Read(ref _reconnectFailures);
        public TaskCompletionSource FirstReconnectFailure { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind == SecsDiagnosticKind.ConnectionAttempt)
                Interlocked.Increment(ref _connectionAttempts);
            if (diagnosticEvent.Kind != SecsDiagnosticKind.ConnectionClosed ||
                !diagnosticEvent.Message.StartsWith("Reconnect failed:", StringComparison.Ordinal)) return;
            var count = Interlocked.Increment(ref _reconnectFailures);
            FirstReconnectFailure.TrySetResult();
            if (count == 2) Session!.DisconnectAsync().GetAwaiter().GetResult();
        }
    }

    private sealed class ReentrantFloodSink(int nestedCount) : ISecsDiagnosticSink
    {
        private int _flooded;
        public ISecsDiagnosticSink? Boundary { get; set; }
        public List<string> Messages { get; } = new();

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            Messages.Add(diagnosticEvent.Message);
            if (Interlocked.Exchange(ref _flooded, 1) != 0) return;
            for (var index = 0; index < nestedCount; index++)
                Boundary!.Emit(new(SecsDiagnosticKind.StateChanged, $"nested-{index}"));
        }
    }
}
