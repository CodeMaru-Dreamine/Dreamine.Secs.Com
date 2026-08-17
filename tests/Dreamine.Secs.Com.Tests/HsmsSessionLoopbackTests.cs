using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

[Collection(NetworkIsolationCollection.Name)]
public sealed class HsmsSessionLoopbackTests
{
    [Fact]
    public async Task ActivePassiveSelectDataTransactionLinktestDeselectAndSeparate()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await ConnectPairAsync(passive, active, timeout.Token);
        Assert.Equal(ConnectionState.Connected, active.State);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, passive.HsmsState);

        await active.SelectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.HsmsState == HsmsConnectionState.Selected, timeout.Token);
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);

        await active.LinktestAsync(timeout.Token);

        var received = new TaskCompletionSource<SecsMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, message) => received.TrySetResult(message);
        var noReply = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false, active.AllocateSystemBytes(), new SecsAsciiItem("PING"));
        await active.SendAsync(noReply, timeout.Token);
        Assert.Equal("PING", Assert.IsType<SecsAsciiItem>((await received.Task.WaitAsync(timeout.Token)).Item).Value);

        var responseSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, primary) =>
        {
            if (!primary.ReplyExpected) return;
            var secondary = new SecsMessage(primary.SessionId, primary.Stream, new SecsFunction(2), false, primary.SystemBytes, new SecsAsciiItem("PONG"));
            _ = passive.SendAsync(secondary, timeout.Token).ContinueWith(
                task => { if (task.IsFaulted) responseSent.TrySetException(task.Exception!.InnerExceptions); else if (task.IsCanceled) responseSent.TrySetCanceled(); else responseSent.TrySetResult(); },
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        };
        var primaryWithReply = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), true, active.AllocateSystemBytes(), new SecsAsciiItem("REQ"));
        var secondaryResponse = await active.SendPrimaryAsync(primaryWithReply, timeout.Token);
        await responseSent.Task.WaitAsync(timeout.Token);
        Assert.Equal("PONG", Assert.IsType<SecsAsciiItem>(secondaryResponse.Item).Value);

        await active.DeselectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.HsmsState == HsmsConnectionState.ConnectedNotSelected, timeout.Token);
        await Assert.ThrowsAsync<HsmsStateException>(() => active.SendAsync(noReply, timeout.Token));

        await active.SelectAsync(timeout.Token);
        await active.SeparateAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Disconnected, timeout.Token);
        Assert.Equal(ConnectionState.Disconnected, active.State);
    }

    [Fact]
    public async Task OutboundDataMustMatchConfiguredSingleSessionId()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        var wrongSession = new SecsMessage(new SecsSessionId(2), new SecsStream(1), new SecsFunction(1), false, active.AllocateSystemBytes());
        await Assert.ThrowsAsync<ArgumentException>(() => active.SendAsync(wrongSession, timeout.Token));
    }

    [Fact]
    public async Task SimultaneousSelectWorksAcrossTcpLoopback()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Host);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Equipment);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await Task.WhenAll(active.SelectAsync(timeout.Token), passive.SelectAsync(timeout.Token));
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
        Assert.Equal(HsmsConnectionState.Selected, passive.HsmsState);
        await Task.WhenAll(active.SelectAsync(timeout.Token), passive.SelectAsync(timeout.Token));
    }

    [Fact]
    public async Task PassiveConnectObservesUserCancellation()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        using var cancellation = new CancellationTokenSource();
        var connect = passive.ConnectAsync(cancellation.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, CancellationToken.None);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        Assert.Equal(ConnectionState.Faulted, passive.State);
    }

    [Fact]
    public async Task DisconnectCancelsPendingPassiveAccept()
    {
        var port = ReservePort();
        var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        var connect = passive.ConnectAsync();
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, CancellationToken.None);

        try
        {
            await passive.DisconnectAsync().WaitAsync(TimeSpan.FromSeconds(2));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
            Assert.Equal(ConnectionState.Disconnected, passive.State);
        }
        finally
        {
            await passive.DisposeAsync();
        }
    }

    [Fact]
    public async Task CanceledCompletedClientIsDisposedBeforeSessionPublication()
    {
        var passive = CreateSession(ReservePort(), SecsConnectionMode.Passive, SecsRole.Equipment);
        var connect = passive.ConnectAsync();
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, CancellationToken.None);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var peer = new TcpClient();
        var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
        await peer.ConnectAsync((IPEndPoint)listener.LocalEndpoint, timeout.Token);
        using var completedClient = await accept;
        try
        {
            var sessionType = typeof(HsmsSession);
            var run = sessionType.GetField("_currentRun", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(passive);
            var publish = sessionType.GetMethod("TryPublishConnectedClient", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();

            var published = Assert.IsType<bool>(publish.Invoke(passive, new object[]
            {
                run!, completedClient, completedClient.GetStream(), canceled.Token
            }));

            Assert.False(published);
            Assert.Null(sessionType.GetField("_client", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(passive));
            var buffer = new byte[1];
            Assert.Equal(0, await peer.GetStream().ReadAsync(buffer, timeout.Token));
        }
        finally
        {
            listener.Stop();
            await passive.DisposeAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        }
    }

    [Fact]
    public async Task ActiveAutoReconnectWaitsForT5ThenReconnects()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: activeTime);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        await passive.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
        var passiveReconnect = passive.ConnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
        await WaitUntilAsync(() => activeTime.ScheduledTimerCount >= 1, timeout.Token);
        activeTime.Advance(TimeSpan.FromSeconds(9));
        Assert.NotEqual(ConnectionState.Connected, active.State);
        activeTime.Advance(TimeSpan.FromSeconds(1));
        await passiveReconnect;
        await WaitUntilAsync(() => active.State == ConnectionState.Connected, timeout.Token);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, active.HsmsState);
    }

    [Fact]
    public async Task ActiveAutoReconnectCompletesOneHundredCyclesWithVirtualT5()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: activeTime);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        for (var cycle = 0; cycle < 100; cycle++)
        {
            await passive.DisconnectAsync(timeout.Token);
            await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);

            var passiveReconnect = passive.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
            await WaitUntilAsync(() => activeTime.ScheduledTimerCount >= 1, timeout.Token);
            activeTime.Advance(TimeSpan.FromSeconds(10));

            await passiveReconnect;
            await WaitUntilAsync(() => active.State == ConnectionState.Connected, timeout.Token);
            await active.SelectAsync(timeout.Token);
            Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
        }
    }

    [Fact]
    public async Task FailedInitialActiveAttemptAlsoRetriesAfterT5()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: activeTime);
        await Assert.ThrowsAnyAsync<SocketException>(() => active.ConnectAsync());
        await WaitUntilAsync(() => activeTime.ScheduledTimerCount >= 1, CancellationToken.None);

        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var passiveConnect = passive.ConnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
        activeTime.Advance(TimeSpan.FromSeconds(10));
        await passiveConnect;
        await WaitUntilAsync(() => active.State == ConnectionState.Connected, timeout.Token);
    }

    [Fact]
    public async Task ExplicitDisconnectCancelsScheduledAutoReconnect()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        var diagnostics = new RecordingSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            autoReconnect: true,
            timeProvider: activeTime,
            diagnostics: diagnostics);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        await passive.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
        await WaitUntilAsync(() => activeTime.ScheduledTimerCount >= 1, timeout.Token);
        var attemptCount = diagnostics.Events.Count(item => item.Kind == SecsDiagnosticKind.ConnectionAttempt);

        await active.DisconnectAsync(timeout.Token);
        activeTime.Advance(TimeSpan.FromSeconds(10));
        for (var index = 0; index < 100; index++) await Task.Yield();

        Assert.Equal(attemptCount, diagnostics.Events.Count(item => item.Kind == SecsDiagnosticKind.ConnectionAttempt));
        Assert.Equal(ConnectionState.Disconnected, active.State);
    }

    [Fact]
    public async Task FailedT5ReconnectDiagnosticCanSynchronouslyDisconnectSession()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        var sink = new ReentrantDisconnectSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        var active = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            autoReconnect: true,
            timeProvider: time,
            diagnostics: sink);
        sink.Session = active;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            await active.SelectAsync(timeout.Token);
            await passive.DisconnectAsync(timeout.Token);
            await WaitUntilAsync(() => time.ScheduledTimerCount >= 1, timeout.Token);

            time.Advance(TimeSpan.FromSeconds(10));
            await sink.Completed.Task.WaitAsync(timeout.Token);

            Assert.Null(sink.Error);
            Assert.Equal(ConnectionState.Disconnected, active.State);
            await active.DisposeAsync().AsTask().WaitAsync(timeout.Token);
        }
        finally
        {
            try { await active.DisposeAsync(); }
            catch { }
        }
    }

    [Fact]
    public async Task CanceledDisconnectWaitStillCompletesOwnedCleanup()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);

        var lifecycleGate = GetLifecycleGate(active);
        await lifecycleGate.WaitAsync(timeout.Token);
        try
        {
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active.DisconnectAsync(canceled.Token));
            Assert.Equal(ConnectionState.Connected, active.State);
        }
        finally { lifecycleGate.Release(); }

        await WaitUntilAsync(() => active.State == ConnectionState.Disconnected, timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Disconnected, timeout.Token);
    }

    [Fact]
    public async Task ReconnectAfterCanceledDisconnectCreatesFreshTcpConnection()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(2);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var firstAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
        await active.ConnectAsync(timeout.Token);
        using var firstPeer = await firstAccept;

        var lifecycleGate = GetLifecycleGate(active);
        await lifecycleGate.WaitAsync(timeout.Token);
        Task reconnect;
        Task<TcpClient> secondAccept;
        try
        {
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active.DisconnectAsync(canceled.Token));
            secondAccept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
            reconnect = active.ConnectAsync(timeout.Token);
        }
        finally { lifecycleGate.Release(); }

        await reconnect;
        using var secondPeer = await secondAccept;
        Assert.Equal(ConnectionState.Connected, active.State);
        listener.Stop();
    }

    [Fact]
    public async Task PublicConnectDoesNotBypassPendingT5Reconnect()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, autoReconnect: true, timeProvider: activeTime);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        await passive.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => active.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
        await WaitUntilAsync(() => activeTime.ScheduledTimerCount >= 1, timeout.Token);

        var passiveReconnect = passive.ConnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
        var publicConnect = active.ConnectAsync(timeout.Token);
        for (var index = 0; index < 100; index++) await Task.Yield();
        Assert.False(publicConnect.IsCompleted);

        activeTime.Advance(TimeSpan.FromSeconds(9));
        Assert.False(publicConnect.IsCompleted);
        activeTime.Advance(TimeSpan.FromSeconds(1));
        await Task.WhenAll(passiveReconnect, publicConnect);
        Assert.Equal(ConnectionState.Connected, active.State);
    }

    [Fact]
    public async Task QueuedPublicConnectRechecksReconnectScheduledBeforeLifecycleRelease()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        using var sink = new BlockingConnectionClosedSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            autoReconnect: true,
            timeProvider: activeTime,
            diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            await active.SelectAsync(timeout.Token);

            await passive.DisconnectAsync(timeout.Token);
            await sink.Entered.Task.WaitAsync(timeout.Token);
            var passiveReconnect = passive.ConnectAsync(timeout.Token);
            await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
            var queuedConnect = active.ConnectAsync(timeout.Token);

            sink.Release.Set();
            await WaitUntilAsync(() => activeTime.ScheduledTimerCount >= 1, timeout.Token);
            for (var index = 0; index < 100; index++) await Task.Yield();
            Assert.False(queuedConnect.IsCompleted);

            activeTime.Advance(TimeSpan.FromSeconds(9));
            Assert.False(queuedConnect.IsCompleted);
            activeTime.Advance(TimeSpan.FromSeconds(1));
            await Task.WhenAll(passiveReconnect, queuedConnect);
            Assert.Equal(ConnectionState.Connected, active.State);
        }
        finally { sink.Release.Set(); }
    }

    [Fact]
    public async Task T3FailsOnlyTheTransactionAndKeepsSelectedConnection()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, timeProvider: activeTime);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, message) => { if (message.ReplyExpected) received.TrySetResult(); };
        var primary = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), true, active.AllocateSystemBytes());
        var response = active.SendPrimaryAsync(primary, timeout.Token);
        await received.Task.WaitAsync(timeout.Token);
        activeTime.Advance(TimeSpan.FromSeconds(45));
        await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => response);
        Assert.Equal(ConnectionState.Connected, active.State);
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
    }

    [Fact]
    public async Task T6ClosesConnectionWhenControlResponseDoesNotArrive()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        var accept = listener.AcceptTcpClientAsync();
        var time = new ManualTimeProvider();
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, timeProvider: time);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await active.ConnectAsync(timeout.Token);
        using var peer = await accept.WaitAsync(timeout.Token);
        var select = active.SelectAsync(timeout.Token);
        await WaitUntilAsync(() => time.ScheduledTimerCount >= 2, timeout.Token);
        time.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<HsmsTimerExpiredException>(() => select);
        Assert.Equal(ConnectionState.Faulted, active.State);
        Assert.Equal(HsmsConnectionState.NotConnected, active.HsmsState);
        listener.Stop();
    }

    [Fact]
    public async Task T7ClosesTcpWhenSessionRemainsNotSelected()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, timeProvider: activeTime);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        activeTime.Advance(TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => active.State == ConnectionState.Faulted, timeout.Token);
        Assert.Equal(HsmsConnectionState.NotConnected, active.HsmsState);
        await WaitUntilAsync(() => passive.State == ConnectionState.Disconnected, timeout.Token);
    }

    [Fact]
    public async Task T8CommunicationFailureClosesPartialFrameConnection()
    {
        var port = ReservePort();
        var time = new ManualTimeProvider();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, timeProvider: time);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var passiveConnect = passive.ConnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, timeout.Token);
        using var peer = new TcpClient();
        await peer.ConnectAsync(IPAddress.Loopback, port, timeout.Token);
        await passiveConnect;
        await peer.GetStream().WriteAsync(new byte[] { 0 }, timeout.Token);
        await WaitUntilAsync(() => time.ScheduledTimerCount >= 2, timeout.Token);
        time.Advance(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => passive.State == ConnectionState.Faulted, timeout.Token);
        Assert.Equal(HsmsConnectionState.NotConnected, passive.HsmsState);
    }

    [Fact]
    public async Task MessageHandlerExceptionIsIsolatedFromReceiveLoop()
    {
        var port = ReservePort();
        var diagnostics = new RecordingSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: diagnostics);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        var delivered = new TaskCompletionSource<SecsMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += static (_, _) => throw new InvalidOperationException("consumer failure");
        passive.MessageReceived += (_, message) => delivered.TrySetResult(message);
        var message = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false, active.AllocateSystemBytes());
        await active.SendAsync(message, timeout.Token);

        Assert.Equal(message.SystemBytes, (await delivered.Task.WaitAsync(timeout.Token)).SystemBytes);
        await active.LinktestAsync(timeout.Token);
        Assert.Equal(HsmsConnectionState.Selected, passive.HsmsState);
        Assert.Contains(diagnostics.Events, item => item.Kind == SecsDiagnosticKind.ApplicationError);
    }

    [Fact]
    public async Task UnmatchedControlResponseIsRejectedWithoutChangingState()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
        await active.ConnectAsync(timeout.Token);
        using var peer = await accept;
        var codec = new HsmsFrameCodec();

        var select = active.SelectAsync(timeout.Token);
        var selectRequest = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
        await codec.WriteAsync(peer.GetStream(), new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.SelectResponse, selectRequest.Header.SystemBytes)), timeout.Token);
        await select;

        var unknownSystemBytes = new SecsSystemBytes(0xfeed_beef);
        await codec.WriteAsync(peer.GetStream(), new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.DeselectResponse, unknownSystemBytes)), timeout.Token);
        var reject = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
        Assert.Equal(HsmsSType.RejectRequest, reject.SType);
        Assert.Equal((byte)HsmsRejectReason.TransactionNotOpen, reject.Header.HeaderByte3);
        Assert.Equal(unknownSystemBytes, reject.Header.SystemBytes);
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
        listener.Stop();
    }

    [Fact]
    public async Task FailedDeselectResponseKeepsSelectedState()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();
        await active.ConnectAsync(timeout.Token);
        using var peer = await accept;
        var codec = new HsmsFrameCodec();

        var select = active.SelectAsync(timeout.Token);
        var selectRequest = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
        await codec.WriteAsync(peer.GetStream(), new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.SelectResponse, selectRequest.Header.SystemBytes)), timeout.Token);
        await select;

        var deselect = active.DeselectAsync(timeout.Token);
        var deselectRequest = Assert.IsType<HsmsControlMessage>(await codec.ReadAsync(peer.GetStream(), TimeSpan.FromSeconds(5), TimeProvider.System, timeout.Token));
        await codec.WriteAsync(peer.GetStream(), new HsmsControlMessage(HsmsHeader.CreateControl(
            HsmsSType.DeselectResponse, deselectRequest.Header.SystemBytes, headerByte3: (byte)HsmsDeselectStatus.Busy)), timeout.Token);
        await Assert.ThrowsAsync<SecsProtocolException>(() => deselect);
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
        listener.Stop();
    }

    [Fact]
    public async Task DisposeCancelsPendingPassiveAcceptAndIsIdempotent()
    {
        var port = ReservePort();
        var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        var connect = passive.ConnectAsync();
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, CancellationToken.None);

        await passive.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        await passive.DisposeAsync();
        Assert.Equal(ConnectionState.Disconnected, passive.State);
    }

    [Fact]
    public async Task SecondConnectPausedThenDisconnectedEndsWithCancellationNotDisposedToken()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        var lifecycle = GetLifecycleGate(active);
        await lifecycle.WaitAsync(timeout.Token);
        try
        {
            var secondConnect = active.ConnectAsync(timeout.Token);
            var disconnect = active.DisconnectAsync(timeout.Token);
            lifecycle.Release();

            var failure = await Record.ExceptionAsync(() => secondConnect.WaitAsync(timeout.Token));
            Assert.IsAssignableFrom<OperationCanceledException>(failure);
            Assert.IsNotType<ObjectDisposedException>(failure);
            await disconnect.WaitAsync(timeout.Token);
        }
        finally
        {
            if (lifecycle.CurrentCount == 0) lifecycle.Release();
        }
    }

    [Fact]
    public async Task ConcurrentDisposeCallsAwaitTheSameShutdown()
    {
        var port = ReservePort();
        var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        var connect = passive.ConnectAsync();
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, CancellationToken.None);

        var disposals = Enumerable.Range(0, 10)
            .Select(_ => passive.DisposeAsync().AsTask())
            .ToArray();

        Assert.All(disposals, disposal => Assert.Same(disposals[0], disposal));
        await Task.WhenAll(disposals).WaitAsync(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        Assert.Equal(ConnectionState.Disconnected, passive.State);
    }

    [Fact]
    public async Task ReentrantDiagnosticDisposeObservesPublishedShutdownTask()
    {
        var sink = new ReentrantDisposeSink();
        var session = CreateSession(ReservePort(), SecsConnectionMode.Passive, SecsRole.Equipment, diagnostics: sink);
        sink.Session = session;

        var disposal = session.DisposeAsync().AsTask();
        await disposal;
        await sink.ReentrantDisposeObserved.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.NotNull(sink.ReentrantDisposal);
        Assert.Same(disposal, sink.ReentrantDisposal);
    }

    [Fact]
    public async Task TimedOutDisposeIsSharedAndDoesNotHideShutdownFailure()
    {
        var port = ReservePort();
        var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var release = new ManualResetEventSlim();
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            await active.SelectAsync(timeout.Token);
            passive.MessageReceived += (_, _) =>
            {
                handlerEntered.TrySetResult();
                release.Wait();
            };
            await active.SendAsync(new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                false,
                active.AllocateSystemBytes()), timeout.Token);
            await handlerEntered.Task.WaitAsync(timeout.Token);

            var first = passive.DisposeAsync().AsTask();
            var second = passive.DisposeAsync().AsTask();
            Assert.Same(first, second);
            var firstError = await Assert.ThrowsAsync<AggregateException>(() => first);
            var secondError = await Assert.ThrowsAsync<AggregateException>(() => second);
            Assert.Same(firstError, secondError);
            Assert.Contains(firstError.InnerExceptions, exception => exception is TimeoutException);
        }
        finally { release.Set(); }
    }

    [Fact]
    public async Task BlockingT3DiagnosticIsOutsideSessionOwnedWorkerShutdown()
    {
        var port = ReservePort();
        var activeTime = new ManualTimeProvider();
        using var sink = new BlockingTimeoutSink();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, timeProvider: activeTime, diagnostics: sink);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task? expiration = null;
        try
        {
            await ConnectPairAsync(passive, active, timeout.Token);
            await active.SelectAsync(timeout.Token);
            var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            passive.MessageReceived += (_, message) =>
            {
                if (message.ReplyExpected) received.TrySetResult();
            };
            var primary = new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                true,
                active.AllocateSystemBytes());
            var response = active.SendPrimaryAsync(primary, timeout.Token);
            await received.Task.WaitAsync(timeout.Token);

            expiration = Task.Run(() => activeTime.Advance(TimeSpan.FromSeconds(45)));
            await sink.Entered.Task.WaitAsync(timeout.Token);

            var first = active.DisposeAsync().AsTask();
            var second = active.DisposeAsync().AsTask();
            Assert.Same(first, second);
            await first.WaitAsync(TimeSpan.FromSeconds(5));
            await second.WaitAsync(TimeSpan.FromSeconds(5));
            await Assert.ThrowsAsync<SecsTransactionTimeoutException>(() => response);
        }
        finally
        {
            sink.Release.Set();
            if (expiration is not null) await expiration.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    [Fact]
    public async Task ConcurrentSendsRemainCompleteAndUnmixed()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        const int count = 200;
        var received = new System.Collections.Concurrent.ConcurrentDictionary<uint, string>();
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        passive.MessageReceived += (_, message) =>
        {
            received.TryAdd(message.SystemBytes.Value, Assert.IsType<SecsAsciiItem>(message.Item).Value);
            if (received.Count == count) allReceived.TrySetResult();
        };

        var sends = Enumerable.Range(1, count).Select(index =>
        {
            var message = new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false, new SecsSystemBytes((uint)index), new SecsAsciiItem($"PAYLOAD-{index:D4}"));
            return active.SendAsync(message, timeout.Token);
        });
        await Task.WhenAll(sends);
        await allReceived.Task.WaitAsync(timeout.Token);
        Assert.Equal(count, received.Count);
        foreach (var index in Enumerable.Range(1, count)) Assert.Equal($"PAYLOAD-{index:D4}", received[(uint)index]);
    }

    [Fact]
    public async Task PassiveHostAcceptsOneHundredReconnectCycles()
    {
        var port = ReservePort();
        await using var passiveHost = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Host);
        await using var activeEquipment = CreateSession(port, SecsConnectionMode.Active, SecsRole.Equipment);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        for (var cycle = 0; cycle < 100; cycle++)
        {
            await ConnectPairAsync(passiveHost, activeEquipment, timeout.Token);
            await activeEquipment.SelectAsync(timeout.Token);
            await activeEquipment.LinktestAsync(timeout.Token);
            await activeEquipment.DisconnectAsync(timeout.Token);
            await WaitUntilAsync(() => passiveHost.State == ConnectionState.Disconnected, timeout.Token);
            Assert.Equal(HsmsConnectionState.NotConnected, passiveHost.HsmsState);
        }
    }

    private static HsmsSession CreateSession(int port, SecsConnectionMode mode, SecsRole role, bool autoReconnect = false, TimeProvider? timeProvider = null, ISecsDiagnosticSink? diagnostics = null) =>
        new(new HsmsSessionOptions { Host = "127.0.0.1", Port = port, Mode = mode, Role = role, SessionId = new SecsSessionId(1), AutoReconnect = autoReconnect }, timeProvider, diagnostics);

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

    private static SemaphoreSlim GetLifecycleGate(HsmsSession session) =>
        Assert.IsType<SemaphoreSlim>(typeof(HsmsSession)
            .GetField("_lifecycleGate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(session));

    private sealed class RecordingSink : ISecsDiagnosticSink
    {
        private readonly object _gate = new();
        private readonly List<SecsDiagnosticEvent> _events = new();
        public IReadOnlyList<SecsDiagnosticEvent> Events { get { lock (_gate) return _events.ToArray(); } }
        public void Emit(SecsDiagnosticEvent diagnosticEvent) { lock (_gate) _events.Add(diagnosticEvent); }
    }

    private sealed class ReentrantDisposeSink : ISecsDiagnosticSink
    {
        private int _entered;
        public HsmsSession? Session { get; set; }
        public Task? ReentrantDisposal { get; private set; }
        public TaskCompletionSource ReentrantDisposeObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.ConnectionClosed || Interlocked.Exchange(ref _entered, 1) != 0) return;
            ReentrantDisposal = Session!.DisposeAsync().AsTask();
            ReentrantDisposeObserved.TrySetResult();
        }
    }

    private sealed class ReentrantDisconnectSink : ISecsDiagnosticSink
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
            try { Session!.DisconnectAsync().GetAwaiter().GetResult(); }
            catch (Exception exception) { Error = exception; }
            finally { Completed.TrySetResult(); }
        }
    }

    private sealed class BlockingConnectionClosedSink : ISecsDiagnosticSink, IDisposable
    {
        private int _entered;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Release { get; } = new();

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.ConnectionClosed || Interlocked.Exchange(ref _entered, 1) != 0) return;
            Entered.TrySetResult();
            Release.Wait();
        }

        public void Dispose() => Release.Dispose();
    }

    private sealed class BlockingTimeoutSink : ISecsDiagnosticSink, IDisposable
    {
        private int _entered;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Release { get; } = new();

        public void Emit(SecsDiagnosticEvent diagnosticEvent)
        {
            if (diagnosticEvent.Kind != SecsDiagnosticKind.Timeout || Interlocked.Exchange(ref _entered, 1) != 0) return;
            Entered.TrySetResult();
            Release.Wait();
        }

        public void Dispose() => Release.Dispose();
    }
}
