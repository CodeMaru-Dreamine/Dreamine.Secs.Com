using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TraceIsolationCollection
{
    public const string Name = "Process-wide Trace listener isolation";
}

[Collection(TraceIsolationCollection.Name)]
public sealed class Gate1LifecycleRegressionTests
{
    [Fact]
    public void ThrowingTraceListenerCannotEscapeOrWedgeDiagnosticDrain()
    {
        var inner = new ThrowOnceSink();
        var boundary = WrapDiagnosticSink(inner);
        using var listener = new ThrowingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            boundary.Emit(new(SecsDiagnosticKind.ConnectionAttempt, "first"));
            boundary.Emit(new(SecsDiagnosticKind.ConnectionClosed, "second"));
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        Assert.Equal(["first", "second"], inner.Messages);
    }

    [Fact]
    public void ThrowingTraceListenerCannotEscapeDiagnosticOverflowReporting()
    {
        var inner = new OverflowSink();
        var boundary = WrapDiagnosticSink(inner);
        inner.Boundary = boundary;
        using var listener = new ThrowingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            boundary.Emit(new(SecsDiagnosticKind.ConnectionAttempt, "root"));
            boundary.Emit(new(SecsDiagnosticKind.ConnectionClosed, "after"));
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        Assert.Contains("after", inner.Messages);
    }

    [Fact]
    public async Task ThrowingTraceListenerCannotFaultBackgroundFailureObservation()
    {
        var type = typeof(HsmsSession).Assembly.GetType("Dreamine.Secs.Com.Diagnostics.BackgroundTaskObserver", throwOnError: true)!;
        var observe = type.GetMethod("Observe", BindingFlags.Public | BindingFlags.Static)!;
        using var listener = new ThrowingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            var observation = Assert.IsAssignableFrom<Task>(observe.Invoke(null, [
                Task.FromException(new InvalidOperationException("background failure")),
                "test operation"
            ]));
            await observation.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(TaskStatus.RanToCompletion, observation.Status);
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }
    }

    [Fact]
    public async Task CancellationDuringConnectionAttemptDiagnosticDoesNotLeaveConnectingState()
    {
        var sink = new BlockingDiagnosticSink(SecsDiagnosticKind.ConnectionAttempt);
        var session = CreateSession(ReservePort(), SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: sink);
        using var callerCancellation = new CancellationTokenSource();
        try
        {
            var connect = Task.Run(() => session.ConnectAsync(callerCancellation.Token));
            await sink.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            callerCancellation.Cancel();
            sink.Release.Set();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.DoesNotMatch("Listening|Connecting", session.State.ToString());
        }
        finally
        {
            sink.Release.Set();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task ConcurrentConnectWaitsForTheOwnedInFlightAttempt()
    {
        var port = ReservePort();
        var sink = new BlockingDiagnosticSink(SecsDiagnosticKind.ConnectionAttempt);
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var accepted = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            var first = Task.Run(() => session.ConnectAsync(timeout.Token));
            await sink.Entered.Task.WaitAsync(timeout.Token);

            var second = session.ConnectAsync(timeout.Token);
            for (var index = 0; index < 100; index++) await Task.Yield();
            Assert.False(second.IsCompleted);

            sink.Release.Set();
            using var peer = await accepted;
            await Task.WhenAll(first, second).WaitAsync(timeout.Token);
            Assert.Equal(ConnectionState.Connected, session.State);
        }
        finally
        {
            sink.Release.Set();
            listener.Stop();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task StopPendingPassiveConnectThenImmediateConnectCreatesFreshAttempt()
    {
        var port = ReservePort();
        var session = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var first = session.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => session.State == ConnectionState.Listening, timeout.Token);

            var stop = session.DisconnectAsync(timeout.Token);
            var second = session.ConnectAsync(timeout.Token);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(timeout.Token));
            await stop.WaitAsync(timeout.Token);
            if (second.IsCompleted) await second;
            await WaitUntilAsync(() => HasOwnedConnectAttempt(session), timeout.Token);

            using var peer = new TcpClient();
            await peer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
            await second.WaitAsync(timeout.Token);

            Assert.Equal(ConnectionState.Connected, session.State);
        }
        finally
        {
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task StoppedAttemptCannotTearDownNewConnectionAfterItsDiagnosticReturns()
    {
        var port = ReservePort();
        var sink = new BlockingDiagnosticSink(SecsDiagnosticKind.ConnectionAttempt);
        var session = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var first = Task.Run(() => session.ConnectAsync(timeout.Token));
            await sink.Entered.Task.WaitAsync(timeout.Token);

            var stop = session.DisconnectAsync(timeout.Token);
            await WaitUntilAsync(() => session.State == ConnectionState.Disconnected, timeout.Token);
            var second = session.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => session.State == ConnectionState.Listening, timeout.Token);

            using var peer = new TcpClient();
            await peer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
            await second.WaitAsync(timeout.Token);
            Assert.Equal(ConnectionState.Connected, session.State);

            sink.Release.Set();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(timeout.Token));
            await stop.WaitAsync(timeout.Token);

            Assert.Equal(ConnectionState.Connected, session.State);
            var codec = new HsmsFrameCodec();
            var systemBytes = new SecsSystemBytes(0x01020304);
            await codec.WriteAsync(peer.GetStream(), new HsmsControlMessage(
                HsmsHeader.CreateControl(HsmsSType.SelectRequest, systemBytes)), timeout.Token);
            var response = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(
                peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
            Assert.Equal(HsmsSType.SelectResponse, response.SType);
            Assert.Equal(systemBytes, response.Header.SystemBytes);
            Assert.Equal(HsmsConnectionState.Selected, session.HsmsState);
        }
        finally
        {
            sink.Release.Set();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task ConnectWaitingForScheduledReconnectIsCanceledByExplicitDisconnect()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var sink = new ArmableBlockingDiagnosticSink(SecsDiagnosticKind.ConnectionAttempt);
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            autoReconnect: true,
            timeProvider: time,
            diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        await passive.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
        await WaitUntilAsync(() => time.ScheduledTimerCount > 0, timeout.Token);

        sink.Arm();
        var expiration = Task.Run(() => time.Advance(TimeSpan.FromSeconds(10)));
        await sink.Entered.Task.WaitAsync(timeout.Token);
        var waitingConnect = active.ConnectAsync(timeout.Token);
        for (var index = 0; index < 100; index++) await Task.Yield();
        Assert.False(waitingConnect.IsCompleted);

        var disconnect = active.DisconnectAsync(timeout.Token);
        sink.Release.Set();
        await expiration.WaitAsync(timeout.Token);
        await disconnect.WaitAsync(timeout.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitingConnect.WaitAsync(timeout.Token));
        Assert.Equal(ConnectionState.Disconnected, active.State);
    }

    [Fact]
    public async Task StaleReceiveLoopCannotProcessOrReplyOnReconnectedEpoch()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var sink = new BlockingDiagnosticSink(SecsDiagnosticKind.FrameReceived);
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(2);
        var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: time, diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var firstAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            await active.ConnectAsync(timeout.Token);
            using var firstPeer = await firstAccept;
            var run = GetConnectedRun(active);
            var oldEpoch = GetConnectionEpoch(run);
            var oldReceiveLoop = GetReceiveLoop(run);

            var selectRequest = new HsmsFrameCodec().Encode(new HsmsControlMessage(
                HsmsHeader.CreateControl(HsmsSType.SelectRequest, new SecsSystemBytes(0x10203040))));
            await firstPeer.GetStream().WriteAsync(selectRequest, timeout.Token);
            await sink.Entered.Task.WaitAsync(timeout.Token);

            time.Advance(TimeSpan.FromSeconds(10));
            await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
            await WaitUntilAsync(() => time.ScheduledTimerCount > 0, timeout.Token);

            var secondAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            time.Advance(TimeSpan.FromSeconds(10));
            using var secondPeer = await secondAccept;
            await WaitUntilAsync(() => active.State == ConnectionState.Connected, timeout.Token);
            Assert.True(GetConnectionEpoch(run) > oldEpoch);

            sink.Release.Set();
            await oldReceiveLoop.WaitAsync(timeout.Token);

            Assert.Equal(ConnectionState.Connected, active.State);
            Assert.Equal(HsmsConnectionState.ConnectedNotSelected, active.HsmsState);
            Assert.False(secondPeer.Client.Poll(200_000, SelectMode.SelectRead));
        }
        finally
        {
            sink.Release.Set();
            listener.Stop();
            try { await active.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task ReconnectWaitsForBlockedOldControlResponseMutation()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(2);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            var firstAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            await session.ConnectAsync(timeout.Token);
            using var firstPeer = await firstAccept;
            var codec = new HsmsFrameCodec();
            var selection = session.SelectAsync(timeout.Token);
            var selectRequest = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(
                firstPeer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));

            using var blockedPending = new MonitorGateBlocker(GetPendingGate(
                session, "_controlTransactions", selectRequest.Header.SystemBytes.Value));
            await codec.WriteAsync(firstPeer.GetStream(), new HsmsControlMessage(HsmsHeader.CreateControl(
                HsmsSType.SelectResponse,
                selectRequest.Header.SystemBytes,
                headerByte3: (byte)HsmsSelectStatus.Success,
                sessionId: selectRequest.Header.SessionId)), timeout.Token);
            await WaitUntilAsync(() => GetOutstandingCount(session, "_controlTransactions") == 0, timeout.Token);

            var runGateAvailableDuringMutation = CanEnterRunGate(session);
            var stop = Task.Run(() => session.DisconnectAsync(timeout.Token));
            blockedPending.Release();
            await selection.WaitAsync(timeout.Token);
            await stop.WaitAsync(timeout.Token);
            var secondAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            var second = session.ConnectAsync(timeout.Token);
            using var secondPeer = await secondAccept;
            await second.WaitAsync(timeout.Token);

            Assert.False(runGateAvailableDuringMutation);
            Assert.Equal(ConnectionState.Connected, session.State);
            Assert.Equal(HsmsConnectionState.ConnectedNotSelected, session.HsmsState);
        }
        finally
        {
            listener.Stop();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task ReconnectWaitsForBlockedOldDataTransactionMutation()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(2);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            var firstAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            await session.ConnectAsync(timeout.Token);
            using var firstPeer = await firstAccept;
            await SelectWithRawPeerAsync(session, firstPeer, timeout.Token);

            var primaryMessage = new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                true,
                session.AllocateSystemBytes());
            var primary = session.SendPrimaryAsync(primaryMessage, timeout.Token);
            var codec = new HsmsFrameCodec();
            _ = Assert.IsType<HsmsDataMessage>(await codec.ReadAsync(
                firstPeer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));

            using var blockedPending = new MonitorGateBlocker(GetPendingGate(
                session, "_transactions", primaryMessage.SystemBytes.Value));
            await codec.WriteAsync(firstPeer.GetStream(), new HsmsDataMessage(new SecsMessage(
                primaryMessage.SessionId,
                primaryMessage.Stream,
                new SecsFunction(2),
                false,
                primaryMessage.SystemBytes)), timeout.Token);
            await WaitUntilAsync(() => GetOutstandingCount(session, "_transactions") == 0, timeout.Token);

            var runGateAvailableDuringMutation = CanEnterRunGate(session);
            var stop = Task.Run(() => session.DisconnectAsync(timeout.Token));
            blockedPending.Release();
            var secondary = await primary.WaitAsync(timeout.Token);
            await stop.WaitAsync(timeout.Token);
            var secondAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            var second = session.ConnectAsync(timeout.Token);
            using var secondPeer = await secondAccept;
            await second.WaitAsync(timeout.Token);

            Assert.Equal(new SecsFunction(2), secondary.Function);
            Assert.False(runGateAvailableDuringMutation);
            Assert.Equal(ConnectionState.Connected, session.State);
            Assert.Equal(HsmsConnectionState.ConnectedNotSelected, session.HsmsState);
        }
        finally
        {
            listener.Stop();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task SeparateStateMutationUsesTheRunEpochFence()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            await session.ConnectAsync(timeout.Token);
            using var peer = await accept;
            await SelectWithRawPeerAsync(session, peer, timeout.Token);

            using var blockedState = new MonitorGateBlocker(GetStateMachineGate(session));
            var separate = Task.Run(() => session.SeparateAsync(timeout.Token));
            await WaitUntilAsync(() => !CanEnterRunGate(session), timeout.Token);

            blockedState.Release();
            var request = Assert.IsType<HsmsControlMessage>(await new HsmsFrameCodec().ReadAsync(
                peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
            Assert.Equal(HsmsSType.SeparateRequest, request.SType);
            await separate.WaitAsync(timeout.Token);
            Assert.Equal(ConnectionState.Disconnected, session.State);
        }
        finally
        {
            listener.Stop();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task QueuedOutboundFromStaleEpochIsRejectedBeforeWriting()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        var run = GetConnectedRun(active);
        var originalEpoch = GetConnectionEpoch(run);
        var sendGate = GetSendGate(active);
        await sendGate.WaitAsync(timeout.Token);
        Task send;
        try
        {
            send = active.SendAsync(new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                false,
                active.AllocateSystemBytes()), timeout.Token);
            SetConnectionEpoch(run, originalEpoch + 1);
        }
        finally { sendGate.Release(); }

        try
        {
            await Assert.ThrowsAsync<IOException>(() => send.WaitAsync(timeout.Token));
        }
        finally { SetConnectionEpoch(run, originalEpoch); }
    }

    [Fact]
    public async Task T6StartsAfterWireWriteBeforeFrameSentDiagnosticReturns()
    {
        var time = new ManualTimeProvider();
        var sink = new ArmableBlockingDiagnosticSink(SecsDiagnosticKind.FrameSent);
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, timeProvider: time, diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            await session.ConnectAsync(timeout.Token);
            using var peer = await accept;
            await SelectWithRawPeerAsync(session, peer, timeout.Token);
            Assert.Equal(0, time.ScheduledTimerCount);

            sink.Arm();
            var request = Task.Run(() => session.LinktestAsync(timeout.Token));
            var received = Assert.IsType<HsmsControlMessage>(await new HsmsFrameCodec().ReadAsync(
                peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
            Assert.Equal(HsmsSType.LinktestRequest, received.SType);
            await sink.Entered.Task.WaitAsync(timeout.Token);

            var scheduledBeforeRelease = time.ScheduledTimerCount;
            if (scheduledBeforeRelease != 0) time.Advance(TimeSpan.FromSeconds(5));
            sink.Release.Set();
            if (scheduledBeforeRelease == 0)
            {
                await WaitUntilAsync(() => time.ScheduledTimerCount != 0, timeout.Token);
                time.Advance(TimeSpan.FromSeconds(5));
            }

            await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => request.WaitAsync(timeout.Token));
            Assert.Equal(1, scheduledBeforeRelease);
        }
        finally
        {
            sink.Release.Set();
            listener.Stop();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task T3StartsAfterWireWriteBeforeFrameSentDiagnosticReturns()
    {
        var time = new ManualTimeProvider();
        var sink = new ArmableBlockingDiagnosticSink(SecsDiagnosticKind.FrameSent);
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        var session = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, timeProvider: time, diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            await session.ConnectAsync(timeout.Token);
            using var peer = await accept;
            await SelectWithRawPeerAsync(session, peer, timeout.Token);
            Assert.Equal(0, time.ScheduledTimerCount);

            sink.Arm();
            var primary = new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                true,
                session.AllocateSystemBytes());
            var request = Task.Run(() => session.SendPrimaryAsync(primary, timeout.Token));
            var received = Assert.IsType<HsmsDataMessage>(await new HsmsFrameCodec().ReadAsync(
                peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
            Assert.Equal(primary.SystemBytes, received.SecsMessage.SystemBytes);
            await sink.Entered.Task.WaitAsync(timeout.Token);

            var scheduledBeforeRelease = time.ScheduledTimerCount;
            if (scheduledBeforeRelease != 0) time.Advance(TimeSpan.FromSeconds(45));
            sink.Release.Set();
            if (scheduledBeforeRelease == 0)
            {
                await WaitUntilAsync(() => time.ScheduledTimerCount != 0, timeout.Token);
                time.Advance(TimeSpan.FromSeconds(45));
            }

            await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => request.WaitAsync(timeout.Token));
            Assert.Equal(1, scheduledBeforeRelease);
        }
        finally
        {
            sink.Release.Set();
            listener.Stop();
            try { await session.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task TaskRunFromMessageCallbackDoesNotInheritReentrantDisposeOwnership()
    {
        var port = ReservePort();
        var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var completed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            await active.SelectAsync(timeout.Token);
            passive.MessageReceived += (_, _) =>
            {
                Exception? failure = null;
                try
                {
                    Task.Run(async () => await passive.DisposeAsync()).GetAwaiter().GetResult();
                }
                catch (Exception exception) { failure = exception; }
                completed.TrySetResult(failure);
            };

            await active.SendAsync(new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                false,
                active.AllocateSystemBytes()), timeout.Token);

            var failure = await completed.Task.WaitAsync(timeout.Token);
            var aggregate = Assert.IsType<AggregateException>(failure);
            Assert.Contains(aggregate.InnerExceptions, exception => exception is TimeoutException);
        }
        finally
        {
            try { await passive.DisposeAsync(); }
            catch { }
        }
    }

    private static ISecsDiagnosticSink WrapDiagnosticSink(ISecsDiagnosticSink sink)
    {
        var type = typeof(HsmsSession).Assembly.GetType("Dreamine.Secs.Com.Diagnostics.SafeSecsDiagnosticSink", throwOnError: true)!;
        var wrap = type.GetMethod("Wrap", BindingFlags.Public | BindingFlags.Static)!;
        return Assert.IsAssignableFrom<ISecsDiagnosticSink>(wrap.Invoke(null, [sink]));
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

    private static object GetConnectedRun(HsmsSession session) =>
        typeof(HsmsSession)
            .GetField("_connectedRun", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session)!;

    private static bool CanEnterRunGate(HsmsSession session)
    {
        var gate = typeof(HsmsSession)
            .GetField("_runGate", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session)!;
        if (!Monitor.TryEnter(gate)) return false;
        Monitor.Exit(gate);
        return true;
    }

    private static object GetStateMachineGate(HsmsSession session)
    {
        var stateMachine = typeof(HsmsSession)
            .GetField("_stateMachine", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session)!;
        return stateMachine.GetType()
            .GetField("_gate", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(stateMachine)!;
    }

    private static int GetOutstandingCount(HsmsSession session, string managerField)
    {
        var manager = typeof(HsmsSession)
            .GetField(managerField, BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session)!;
        return Assert.IsType<int>(manager.GetType().GetProperty("OutstandingCount")!.GetValue(manager));
    }

    private static object GetPendingGate(HsmsSession session, string managerField, uint systemBytes)
    {
        var manager = typeof(HsmsSession)
            .GetField(managerField, BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session)!;
        var pending = Assert.IsAssignableFrom<IEnumerable>(manager.GetType()
            .GetField("_pending", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(manager));
        foreach (var entry in pending)
        {
            var entryType = entry!.GetType();
            if (Assert.IsType<uint>(entryType.GetProperty("Key")!.GetValue(entry)) != systemBytes) continue;
            var value = entryType.GetProperty("Value")!.GetValue(entry)!;
            var monitor = value.GetType().GetField("_monitor", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(value)!;
            return monitor.GetType().GetField("_gate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(monitor)!;
        }
        throw new InvalidOperationException($"Pending transaction 0x{systemBytes:X8} was not found.");
    }

    private static long GetConnectionEpoch(object run) =>
        Assert.IsType<long>(run.GetType().GetProperty("ConnectionEpoch")!.GetValue(run));

    private static Task GetReceiveLoop(object run) =>
        Assert.IsAssignableFrom<Task>(run.GetType().GetProperty("ReceiveLoop")!.GetValue(run));

    private static SemaphoreSlim GetSendGate(HsmsSession session) =>
        Assert.IsType<SemaphoreSlim>(typeof(HsmsSession)
            .GetField("_sendGate", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session));

    private static bool HasOwnedConnectAttempt(HsmsSession session)
    {
        var run = typeof(HsmsSession)
            .GetField("_currentRun", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session);
        return run?.GetType().GetProperty("ConnectAttempt")!.GetValue(run) is not null;
    }

    private static void SetConnectionEpoch(object run, long value) =>
        run.GetType().GetProperty("ConnectionEpoch")!.SetValue(run, value);

    private static async Task SelectWithRawPeerAsync(
        HsmsSession session,
        TcpClient peer,
        CancellationToken cancellationToken)
    {
        var codec = new HsmsFrameCodec();
        var selection = session.SelectAsync(cancellationToken);
        var request = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(
            peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken));
        Assert.Equal(HsmsSType.SelectRequest, request.SType);
        await codec.WriteAsync(peer.GetStream(), new HsmsControlMessage(HsmsHeader.CreateControl(
            HsmsSType.SelectResponse,
            request.Header.SystemBytes,
            headerByte3: (byte)HsmsSelectStatus.Success,
            sessionId: request.Header.SessionId)), cancellationToken);
        await selection;
    }

    private static int ReservePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class BlockingDiagnosticSink(SecsDiagnosticKind target) : ISecsDiagnosticSink
    {
        private int _entered;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Release { get; } = new();

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != target || Interlocked.Exchange(ref _entered, 1) != 0) return;
            Entered.TrySetResult();
            Release.Wait();
        }
    }

    private sealed class ArmableBlockingDiagnosticSink(SecsDiagnosticKind target) : ISecsDiagnosticSink
    {
        private int _armed;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Release { get; } = new();

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != target || Interlocked.Exchange(ref _armed, 0) != 1) return;
            Entered.TrySetResult();
            Release.Wait();
        }
    }

    private sealed class MonitorGateBlocker : IDisposable
    {
        private readonly ManualResetEventSlim _acquired = new();
        private readonly ManualResetEventSlim _release = new();
        private readonly Thread _thread;
        private int _released;

        public MonitorGateBlocker(object gate)
        {
            _thread = new Thread(() =>
            {
                Monitor.Enter(gate);
                try
                {
                    _acquired.Set();
                    _release.Wait();
                }
                finally { Monitor.Exit(gate); }
            }) { IsBackground = true };
            _thread.Start();
            if (!_acquired.Wait(TimeSpan.FromSeconds(5)))
                throw new TimeoutException("The private pending gate was not acquired.");
        }

        public void Release()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0) _release.Set();
        }

        public void Dispose()
        {
            Release();
            if (!_thread.Join(TimeSpan.FromSeconds(5)))
                throw new TimeoutException("The private pending-gate thread did not stop.");
            _acquired.Dispose();
            _release.Dispose();
        }
    }

    private sealed class ThrowOnceSink : ISecsDiagnosticSink
    {
        private int _calls;
        public List<string> Messages { get; } = [];

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            Messages.Add(diagnosticEvent.Message);
            if (Interlocked.Increment(ref _calls) == 1) throw new InvalidOperationException("diagnostic failure");
        }
    }

    private sealed class OverflowSink : ISecsDiagnosticSink
    {
        private int _flooded;
        public ISecsDiagnosticSink? Boundary { get; set; }
        public List<string> Messages { get; } = [];

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            Messages.Add(diagnosticEvent.Message);
            if (Interlocked.Exchange(ref _flooded, 1) != 0) return;
            for (var index = 0; index < 2_048; index++)
                Boundary!.Emit(new(SecsDiagnosticKind.StateChanged, $"nested-{index}"));
        }
    }

    private sealed class ThrowingTraceListener : TraceListener
    {
        public override void Write(string? message) => throw new InvalidOperationException("trace failure");
        public override void WriteLine(string? message) => throw new InvalidOperationException("trace failure");
    }
}
