using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Codecs;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class HsmsRuntimeLimitTests
{
    [Fact]
    public async Task StreamReadRejectsDeclaredTextAboveMessageLimitBeforeReadingFrameBody()
    {
        var codec = CreateCodec(maximumFrameLength: 1_024, maximumMessageLength: 4);
        var prefixOnly = new byte[HsmsFrameCodec.LengthPrefixSize];
        BinaryPrimitives.WriteInt32BigEndian(prefixOnly, HsmsFrameCodec.HeaderLength + 5);
        await using var stream = new MemoryStream(prefixOnly);

        var error = await Assert.ThrowsAsync<SecsDecodeException>(
            () => codec.ReadAsync(stream, TimeSpan.FromSeconds(1), TimeProvider.System));

        Assert.Equal(SecsValidationCode.SizeLimitExceeded, error.Code);
        Assert.Equal(prefixOnly.Length, stream.Position);
    }

    [Fact]
    public void IncrementalDecoderRejectsMessageLimitBeforeGrowingItsBuffer()
    {
        var codec = CreateCodec(maximumFrameLength: 1_024 * 1_024, maximumMessageLength: 16);
        var decoder = new HsmsStreamDecoder(codec);
        var prefix = new byte[HsmsFrameCodec.LengthPrefixSize];
        BinaryPrimitives.WriteInt32BigEndian(prefix, HsmsFrameCodec.HeaderLength + 17);

        var error = Assert.Throws<SecsDecodeException>(() => decoder.Append(prefix));

        Assert.Equal(SecsValidationCode.SizeLimitExceeded, error.Code);
        Assert.Equal(prefix.Length, decoder.BufferedByteCount);
        var buffer = Assert.IsType<byte[]>(typeof(HsmsStreamDecoder)
            .GetField("_buffer", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(decoder));
        Assert.Equal(256, buffer.Length);
    }

    [Fact]
    public async Task SessionAppliesMessageDepthAndListLimitsToOutboundTraffic()
    {
        var port = ReservePort();
        await using var passive = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment);
        await using var active = CreateSession(
            port,
            SecsConnectionMode.Active,
            SecsRole.Host,
            maximumFrameLength: 1_024,
            maximumMessageLength: 6,
            maximumNestingDepth: 0,
            maximumListItemCount: 1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await Task.WhenAll(passive.ConnectAsync(timeout.Token), active.ConnectAsync(timeout.Token));
        await active.SelectAsync(timeout.Token);

        var tooLarge = Message(active, new SecsAsciiItem("12345"));
        var tooDeep = Message(active, new SecsListItem(new SecsBinaryItem()));
        var tooMany = Message(active, new SecsListItem(new SecsBinaryItem(), new SecsBinaryItem()));

        Assert.Equal(
            SecsValidationCode.SizeLimitExceeded,
            (await Assert.ThrowsAsync<SecsDecodeException>(() => active.SendAsync(tooLarge, timeout.Token))).Code);
        Assert.Equal(
            SecsValidationCode.DepthLimitExceeded,
            (await Assert.ThrowsAsync<SecsDecodeException>(() => active.SendAsync(tooDeep, timeout.Token))).Code);
        Assert.Equal(
            SecsValidationCode.SizeLimitExceeded,
            (await Assert.ThrowsAsync<SecsDecodeException>(() => active.SendAsync(tooMany, timeout.Token))).Code);
        Assert.Equal(HsmsConnectionState.Selected, active.HsmsState);
    }

    [Fact]
    public void ProviderPreservesMessageLimitDuringOptionMapping()
    {
        var provider = new DreamineSecsCommunicationProvider(_ => new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = 5001,
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host,
            MaximumFrameLength = 100,
            MaximumMessageLength = 91
        });

        Assert.Throws<ArgumentOutOfRangeException>(() => provider.CreateConnection(ConnectionOptions(provider)));
    }

    [Theory]
    [InlineData(-1, 1)]
    [InlineData(1, -1)]
    public void ProviderPreservesStructureLimitsDuringOptionMapping(int maximumNestingDepth, int maximumListItemCount)
    {
        var provider = new DreamineSecsCommunicationProvider(_ => new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = 5001,
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host,
            MaximumNestingDepth = maximumNestingDepth,
            MaximumListItemCount = maximumListItemCount
        });

        Assert.Throws<ArgumentOutOfRangeException>(() => provider.CreateConnection(ConnectionOptions(provider)));
    }

    private static HsmsFrameCodec CreateCodec(int maximumFrameLength, int maximumMessageLength) => new(
        new HsmsFrameCodecOptions { MaximumFrameLength = maximumFrameLength },
        new SecsItemCodec(new SecsItemCodecOptions { MaximumMessageLength = maximumMessageLength }));

    private static HsmsSession CreateSession(
        int port,
        SecsConnectionMode mode,
        SecsRole role,
        int maximumFrameLength = 16 * 1024 * 1024,
        int? maximumMessageLength = null,
        int maximumNestingDepth = 64,
        int maximumListItemCount = 65_535) => new(new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Mode = mode,
            Role = role,
            SessionId = new SecsSessionId(1),
            MaximumFrameLength = maximumFrameLength,
            MaximumMessageLength = maximumMessageLength,
            MaximumNestingDepth = maximumNestingDepth,
            MaximumListItemCount = maximumListItemCount
        });

    private static SecsMessage Message(HsmsSession session, SecsItem item) => new(
        new SecsSessionId(1),
        new SecsStream(1),
        new SecsFunction(1),
        false,
        session.AllocateSystemBytes(),
        item);

    private static SecsConnectionOptions ConnectionOptions(DreamineSecsCommunicationProvider provider) => new()
    {
        ProviderKey = provider.Key,
        Mode = SecsConnectionMode.Active,
        Role = SecsRole.Host
    };

    private static int ReservePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
