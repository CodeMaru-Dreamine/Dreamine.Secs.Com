using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Com.Hsms;
using Dreamine.Secs.Com.Transactions;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class HsmsWireObservationTests
{
    [Fact]
    public async Task NonminimalInboundEncodingPreservesExactBytesAndActualDiagnosticLength()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var diagnostics = new RecordingSink();
        var session = CreateSession(port, Wire(), diagnostics: diagnostics);
        await using var observations = session.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();

        try
        {
            await session.ConnectAsync(timeout.Token);
            using var peer = await accept;
            await SelectWithRawPeerAsync(session, peer, timeout.Token);

            var received = new TaskCompletionSource<SecsMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            session.MessageReceived += (_, message) => received.TrySetResult(message);
            var canonicalMessage = new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                false,
                new SecsSystemBytes(0x10203040),
                new SecsAsciiItem("A"));
            var canonical = new HsmsFrameCodec().Encode(new HsmsDataMessage(canonicalMessage));
            var nonminimal = UseTwoLengthBytesForSingleByteItem(canonical);

            await peer.GetStream().WriteAsync(nonminimal, timeout.Token);
            await peer.GetStream().FlushAsync(timeout.Token);

            var decoded = await received.Task.WaitAsync(timeout.Token);
            Assert.Equal("A", Assert.IsType<SecsAsciiItem>(decoded.Item).Value);
            var observed = await ReadUntilAsync(
                observations,
                item => item.Direction == HsmsWireDirection.Inbound && item.CapturedBytes.Span.SequenceEqual(nonminimal),
                timeout.Token);

            Assert.Equal(nonminimal, observed.CapturedBytes.ToArray());
            Assert.Equal(nonminimal.Length, observed.ActualByteCount);
            Assert.Equal(nonminimal.Length - HsmsFrameCodec.LengthPrefixSize, observed.DeclaredFrameLength);
            Assert.False(observed.IsCaptureTruncated);
            Assert.Equal(nonminimal.Length - 1, new HsmsFrameCodec().Encode(new HsmsDataMessage(decoded)).Length);
            Assert.Contains(diagnostics.Events, item => item.Kind == SecsDiagnosticKind.FrameReceived && item.FrameLength == nonminimal.Length);
        }
        finally
        {
            listener.Stop();
            await session.DisposeAsync();
        }
    }

    [Fact]
    public async Task OutboundObservationMatchesTheExactFrameReadByThePeer()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var diagnostics = new RecordingSink();
        var session = CreateSession(port, Wire(), diagnostics: diagnostics);
        await using var observations = session.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();

        try
        {
            await session.ConnectAsync(timeout.Token);
            using var peer = await accept;
            await SelectWithRawPeerAsync(session, peer, timeout.Token);

            var message = new SecsMessage(
                new SecsSessionId(1),
                new SecsStream(1),
                new SecsFunction(1),
                false,
                new SecsSystemBytes(0x55667788),
                new SecsAsciiItem("EXACT"));
            var send = session.SendAsync(message, timeout.Token);
            var peerFrame = await ReadRawFrameAsync(peer.GetStream(), timeout.Token);
            await send;
            var observed = await ReadUntilAsync(
                observations,
                item => item.Direction == HsmsWireDirection.Outbound && item.CapturedBytes.Span.SequenceEqual(peerFrame),
                timeout.Token);

            Assert.Equal(peerFrame, observed.CapturedBytes.ToArray());
            Assert.Equal(peerFrame.Length, observed.ActualByteCount);
            Assert.Equal(BinaryPrimitives.ReadInt32BigEndian(peerFrame), observed.DeclaredFrameLength);
            Assert.True(observed.SequenceNumber > 0);
            Assert.True(observed.ConnectionEpoch > 0);
            Assert.Equal(TimeSpan.Zero, observed.ObservedAtUtc.Offset);
            Assert.Contains(diagnostics.Events, item => item.Kind == SecsDiagnosticKind.FrameSent && item.FrameLength == peerFrame.Length);
        }
        finally
        {
            listener.Stop();
            await session.DisposeAsync();
        }
    }

    [Fact]
    public async Task CompleteInvalidFrameIsObservedBeforeDecodeClosesTheConnection()
    {
        var port = ReservePort();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start(1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var diagnostics = new RecordingSink();
        var session = CreateSession(port, Wire(), diagnostics: diagnostics);
        await using var observations = session.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        var accept = listener.AcceptTcpClientAsync(timeout.Token).AsTask();

        try
        {
            await session.ConnectAsync(timeout.Token);
            using var peer = await accept;
            var invalid = new HsmsFrameCodec().Encode(new HsmsControlMessage(
                HsmsHeader.CreateControl(HsmsSType.SelectRequest, new SecsSystemBytes(7))));
            invalid[8] = 1;

            await peer.GetStream().WriteAsync(invalid, timeout.Token);
            await peer.GetStream().FlushAsync(timeout.Token);

            var observed = await ReadUntilAsync(
                observations,
                item => item.Direction == HsmsWireDirection.Inbound,
                timeout.Token);
            Assert.Equal(invalid, observed.CapturedBytes.ToArray());
            Assert.Contains(diagnostics.Events, item => item.Kind == SecsDiagnosticKind.FrameReceived && item.FrameLength == invalid.Length);
            await WaitUntilAsync(() => session.State is ConnectionState.Faulted or ConnectionState.Disconnected, timeout.Token);
        }
        finally
        {
            listener.Stop();
            await session.DisposeAsync();
        }
    }

    [Fact]
    public async Task FullQueueDropsWithSequenceGapWithoutBlockingProtocol()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, new HsmsWireObservationOptions { QueueCapacity = 1, MaximumCapturedBytes = 14 });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        Assert.True(active.DroppedWireObservationCount > 0);

        await using var observations = active.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        Assert.True(await observations.MoveNextAsync().AsTask().WaitAsync(timeout.Token));
        var first = observations.Current;

        await active.LinktestAsync(timeout.Token);

        Assert.True(await observations.MoveNextAsync().AsTask().WaitAsync(timeout.Token));
        var next = observations.Current;
        Assert.True(next.SequenceNumber > first.SequenceNumber + 1);
        Assert.True(active.DroppedWireObservationCount > 0);
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
    }

    [Fact]
    public async Task RetainedObservationSnapshotIsNotReusedByLaterFrames()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, Wire(queueCapacity: 128));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var observations = active.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);

        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        Assert.True(await observations.MoveNextAsync().AsTask().WaitAsync(timeout.Token));
        var retained = observations.Current.CapturedBytes;
        var expected = retained.ToArray();

        for (var index = 0; index < 10; index++) await active.LinktestAsync(timeout.Token);

        Assert.Equal(expected, retained.ToArray());
    }

    [Fact]
    public async Task CaptureLimitRetainsOnlyExactPrefixAndMarksTruncation()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, new HsmsWireObservationOptions { QueueCapacity = 128, MaximumCapturedBytes = 14 });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var observations = active.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        var message = new SecsMessage(
            new SecsSessionId(1),
            new SecsStream(1),
            new SecsFunction(1),
            false,
            active.AllocateSystemBytes(),
            new SecsAsciiItem("CAPTURE-LIMIT"));
        var expected = new HsmsFrameCodec().Encode(new HsmsDataMessage(message));

        await active.SendAsync(message, timeout.Token);
        var observed = await ReadUntilAsync(
            observations,
            item => item.Direction == HsmsWireDirection.Outbound && item.ActualByteCount == expected.Length,
            timeout.Token);

        Assert.True(observed.IsCaptureTruncated);
        Assert.Equal(14, observed.CapturedBytes.Length);
        Assert.Equal(expected.AsSpan(0, 14).ToArray(), observed.CapturedBytes.ToArray());
    }

    [Fact]
    public async Task CapturePolicyIsAppliedBeforePayloadCopy()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, new HsmsWireObservationOptions
        {
            QueueCapacity = 32,
            MaximumCapturedBytes = 1024 * 1024 + 64,
            DefaultCaptureMode = HsmsWireCaptureMode.HeaderOnly,
            CaptureRules =
            [
                new HsmsWireCaptureRule(6, 11, HsmsWireDirection.Outbound, HsmsWireCaptureMode.Excluded),
                new HsmsWireCaptureRule(6, 12, HsmsWireDirection.Outbound, HsmsWireCaptureMode.FullFrame, 1024)
            ]
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var observations = active.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        await active.SendAsync(new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false,
            active.AllocateSystemBytes(), new SecsBinaryItem(new byte[1024 * 1024])), timeout.Token);
        var headerOnly = await ReadUntilAsync(observations,
            item => item.Direction == HsmsWireDirection.Outbound && item.Header?.Stream == 1, timeout.Token);
        Assert.Equal(14, headerOnly.CapturedBytes.Length);
        Assert.Equal((byte)1, headerOnly.Header?.Function);

        await active.SendAsync(new SecsMessage(new SecsSessionId(1), new SecsStream(6), new SecsFunction(11), false,
            active.AllocateSystemBytes(), new SecsAsciiItem("excluded")), timeout.Token);
        var excluded = await ReadUntilAsync(observations,
            item => item.Direction == HsmsWireDirection.Outbound && item.Header?.Stream == 6 && item.Header?.Function == 11,
            timeout.Token);
        Assert.Empty(excluded.CapturedBytes.ToArray());
        Assert.Equal((byte)11, excluded.Header?.Function);

        await active.SendAsync(new SecsMessage(new SecsSessionId(1), new SecsStream(6), new SecsFunction(12), false,
            active.AllocateSystemBytes(), new SecsAsciiItem("full")), timeout.Token);
        var full = await ReadUntilAsync(observations,
            item => item.Direction == HsmsWireDirection.Outbound && item.Header?.Stream == 6 && item.Header?.Function == 12,
            timeout.Token);
        Assert.False(full.IsCaptureTruncated);
        Assert.True(full.CapturedBytes.Length > 14);
    }

    [Fact]
    public async Task FailedConsumerDoesNotAffectProtocolAndAReplacementCanResume()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, Wire(queueCapacity: 64));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in active.ReadWireObservationsAsync(timeout.Token))
                throw new InvalidOperationException("consumer failure");
        });

        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(() => consumer);

        await active.LinktestAsync(timeout.Token);
        await using var replacement = active.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        Assert.True(await replacement.MoveNextAsync().AsTask().WaitAsync(timeout.Token));
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
    }

    [Fact]
    public async Task DisposeCompletesAnIdleObservationEnumeration()
    {
        var session = CreateSession(ReservePort(), Wire());
        await using var observations = session.ReadWireObservationsAsync().GetAsyncEnumerator();
        var pendingMove = observations.MoveNextAsync().AsTask();

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.False(await pendingMove.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task ReconnectUsesANewEpochWhileSequenceRemainsMonotonic()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, Wire(queueCapacity: 128));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var observations = active.ReadWireObservationsAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);

        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        Assert.True(await observations.MoveNextAsync().AsTask().WaitAsync(timeout.Token));
        var first = observations.Current;

        await active.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => passive.State == ConnectionState.Disconnected, timeout.Token);
        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);

        var secondEpoch = await ReadUntilAsync(observations, item => item.ConnectionEpoch > first.ConnectionEpoch, timeout.Token);
        Assert.True(secondEpoch.SequenceNumber > first.SequenceNumber);
        Assert.True(secondEpoch.ConnectionEpoch > first.ConnectionEpoch);
    }

    [Fact]
    public async Task DefaultDisabledDoesNotCreateAChannelOrObservationPayload()
    {
        var session = CreateSession(ReservePort());
        try
        {
            var channel = typeof(HsmsSession).GetField("_wireObservationChannel", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(channel);
            Assert.Null(channel!.GetValue(session));
            Assert.False(session.IsWireObservationEnabled);
            Assert.Equal(0, session.DroppedWireObservationCount);
            await using var observations = session.ReadWireObservationsAsync().GetAsyncEnumerator();
            Assert.False(await observations.MoveNextAsync());
        }
        finally
        {
            await session.DisposeAsync();
        }
    }

    [Fact]
    public async Task DefaultDisabledRemainsAllocationPathFreeDuringProtocolTraffic()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port);
        await using var active = CreateSession(port, wireObservation: null);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await ConnectPairAsync(passive, active, timeout.Token);
        await active.SelectAsync(timeout.Token);
        await active.LinktestAsync(timeout.Token);

        var channel = typeof(HsmsSession).GetField("_wireObservationChannel", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Assert.Null(channel.GetValue(active));
        Assert.Equal(0, active.DroppedWireObservationCount);
        Assert.False(active.IsWireObservationEnabled);
    }

    [Fact]
    public void WireCapabilityIsAdditiveAndExistingPublicSignaturesRemainAvailable()
    {
        Assert.True(typeof(ISecsConnection).IsAssignableFrom(typeof(HsmsSession)));
        Assert.True(typeof(IHsmsWireObservationSource).IsAssignableFrom(typeof(HsmsSession)));
        Assert.NotNull(typeof(HsmsSession).GetConstructor(new[]
        {
            typeof(HsmsSessionOptions), typeof(TimeProvider), typeof(ISecsDiagnosticSink)
        }));
        Assert.NotNull(typeof(SecsDiagnosticEvent).GetConstructor(new[]
        {
            typeof(SecsDiagnosticKind), typeof(string), typeof(HsmsConnectionState?), typeof(int?)
        }));
    }

    [Fact]
    public async Task ProviderPreservesWireObservationOptInDuringOptionMapping()
    {
        var provider = new DreamineSecsCommunicationProvider(_ => new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = ReservePort(),
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host,
            WireObservation = Wire()
        });
        var connection = provider.CreateConnection(new Dreamine.Secs.Abstractions.Options.SecsConnectionOptions
        {
            ProviderKey = provider.Key,
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host
        });

        try
        {
            var source = Assert.IsAssignableFrom<IHsmsWireObservationSource>(connection);
            Assert.True(source.IsWireObservationEnabled);
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task WorkerFailureStillDisposesManagersAndRootLifetime()
    {
        var session = CreateSession(ReservePort());
        var worker = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var track = typeof(HsmsSession).GetMethod("TrackWorker", BindingFlags.Instance | BindingFlags.NonPublic)!;
        track.Invoke(session, new object[] { worker.Task, "injected worker" });
        var lifetime = Assert.IsType<CancellationTokenSource>(typeof(HsmsSession)
            .GetField("_disposeCancellation", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session));
        var transactions = Assert.IsType<SecsTransactionManager>(typeof(HsmsSession)
            .GetField("_transactions", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session));

        var disposal = session.DisposeAsync().AsTask();
        await WaitUntilAsync(() => GetDisposed(session), CancellationToken.None);
        worker.TrySetException(new InvalidOperationException("injected worker failure"));
        var error = await Assert.ThrowsAsync<AggregateException>(() => disposal.WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Single(error.Flatten().InnerExceptions, item => item.Message == "injected worker failure");
        Assert.Throws<ObjectDisposedException>(() => transactions.AllocateSystemBytes());
        Assert.Throws<ObjectDisposedException>(() => _ = lifetime.Token);
    }

    [Fact]
    public async Task TimedOutPublicDisposeStillRunsDeferredCleanupTail()
    {
        var session = CreateSession(ReservePort(), Wire());
        var worker = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        typeof(HsmsSession).GetMethod("TrackWorker", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(session, new object[] { worker.Task, "blocked worker" });
        var lifetime = Assert.IsType<CancellationTokenSource>(typeof(HsmsSession)
            .GetField("_disposeCancellation", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session));
        var transactions = Assert.IsType<SecsTransactionManager>(typeof(HsmsSession)
            .GetField("_transactions", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(session));
        await using var observations = session.ReadWireObservationsAsync().GetAsyncEnumerator();
        var pendingObservation = observations.MoveNextAsync().AsTask();

        var error = await Assert.ThrowsAsync<AggregateException>(
            () => session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Contains(error.Flatten().InnerExceptions, item => item is TimeoutException);
        worker.TrySetResult();

        Assert.False(await pendingObservation.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Throws<ObjectDisposedException>(() => transactions.AllocateSystemBytes());
        Assert.Throws<ObjectDisposedException>(() => _ = lifetime.Token);
    }

    private static HsmsSession CreateSession(
        int port,
        HsmsWireObservationOptions? wireObservation = null,
        ISecsDiagnosticSink? diagnostics = null) =>
        new(new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host,
            SessionId = new SecsSessionId(1),
            WireObservation = wireObservation
        }, diagnostics: diagnostics);

    private static HsmsSession CreateSession(int port) =>
        new(new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Mode = SecsConnectionMode.Passive,
            Role = SecsRole.Equipment,
            SessionId = new SecsSessionId(1)
        });

    private static HsmsWireObservationOptions Wire(int queueCapacity = 32, int maximumCapturedBytes = 4_096) => new()
    {
        QueueCapacity = queueCapacity,
        MaximumCapturedBytes = maximumCapturedBytes
    };

    private static async Task ConnectPairAsync(HsmsSession passive, HsmsSession active, CancellationToken cancellationToken)
    {
        var passiveConnect = passive.ConnectAsync(cancellationToken);
        await WaitUntilAsync(() => passive.State == ConnectionState.Listening, cancellationToken);
        await active.ConnectAsync(cancellationToken);
        await passiveConnect;
    }

    private static async Task SelectWithRawPeerAsync(HsmsSession session, TcpClient peer, CancellationToken cancellationToken)
    {
        var selection = session.SelectAsync(cancellationToken);
        var requestBytes = await ReadRawFrameAsync(peer.GetStream(), cancellationToken);
        var request = Assert.IsType<HsmsControlMessage>(new HsmsFrameCodec().Decode(requestBytes));
        Assert.Equal(HsmsSType.SelectRequest, request.SType);
        var response = new HsmsControlMessage(HsmsHeader.CreateControl(
            HsmsSType.SelectResponse,
            request.Header.SystemBytes,
            headerByte3: (byte)HsmsSelectStatus.Success,
            sessionId: request.Header.SessionId));
        await new HsmsFrameCodec().WriteAsync(peer.GetStream(), response, cancellationToken);
        await selection;
    }

    private static async Task<byte[]> ReadRawFrameAsync(Stream stream, CancellationToken cancellationToken)
    {
        var prefix = new byte[HsmsFrameCodec.LengthPrefixSize];
        await stream.ReadExactlyAsync(prefix, cancellationToken);
        var declared = BinaryPrimitives.ReadInt32BigEndian(prefix);
        Assert.True(declared >= HsmsFrameCodec.HeaderLength);
        var frame = new byte[HsmsFrameCodec.LengthPrefixSize + declared];
        prefix.CopyTo(frame, 0);
        await stream.ReadExactlyAsync(frame.AsMemory(HsmsFrameCodec.LengthPrefixSize), cancellationToken);
        return frame;
    }

    private static byte[] UseTwoLengthBytesForSingleByteItem(byte[] canonical)
    {
        var result = new byte[canonical.Length + 1];
        canonical.AsSpan(0, HsmsFrameCodec.LengthPrefixSize + HsmsFrameCodec.HeaderLength).CopyTo(result);
        BinaryPrimitives.WriteInt32BigEndian(result, BinaryPrimitives.ReadInt32BigEndian(canonical) + 1);
        var itemOffset = HsmsFrameCodec.LengthPrefixSize + HsmsFrameCodec.HeaderLength;
        result[itemOffset] = (byte)((canonical[itemOffset] & 0xfc) | 0x02);
        result[itemOffset + 1] = 0;
        canonical.AsSpan(itemOffset + 1).CopyTo(result.AsSpan(itemOffset + 2));
        return result;
    }

    private static async Task<HsmsWireObservation> ReadUntilAsync(
        IAsyncEnumerator<HsmsWireObservation> observations,
        Func<HsmsWireObservation, bool> predicate,
        CancellationToken cancellationToken)
    {
        while (await observations.MoveNextAsync().AsTask().WaitAsync(cancellationToken))
            if (predicate(observations.Current)) return observations.Current;
        throw new InvalidOperationException("The observation stream completed before the expected frame was read.");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
        }
    }

    private static bool GetDisposed(HsmsSession session) =>
        (int)typeof(HsmsSession).GetField("_disposed", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(session)! != 0;

    private static int ReservePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class RecordingSink : ISecsDiagnosticSink
    {
        private readonly object _gate = new();
        private readonly List<SecsDiagnosticEvent> _events = new();
        public IReadOnlyList<SecsDiagnosticEvent> Events { get { lock (_gate) return _events.ToArray(); } }
        public void Emit(SecsDiagnosticEvent diagnosticEvent) { lock (_gate) _events.Add(diagnosticEvent); }
    }
}
