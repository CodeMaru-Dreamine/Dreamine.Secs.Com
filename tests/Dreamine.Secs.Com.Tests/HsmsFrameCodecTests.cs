using System.Buffers.Binary;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Codecs;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class HsmsFrameCodecTests
{
    private readonly HsmsFrameCodec _codec = new();

    public static TheoryData<HsmsSType> ControlTypes => new()
    {
        HsmsSType.SelectRequest, HsmsSType.SelectResponse,
        HsmsSType.DeselectRequest, HsmsSType.DeselectResponse,
        HsmsSType.LinktestRequest, HsmsSType.LinktestResponse,
        HsmsSType.RejectRequest, HsmsSType.SeparateRequest
    };

    [Fact]
    public void DataMessageGoldenHeaderAndItemRoundTrip()
    {
        var source = CreateData();
        var frame = _codec.Encode(source);
        Assert.Equal(new byte[]
        {
            0, 0, 0, 13,
            0, 3, 0x81, 1, 0, 0, 1, 2, 3, 4,
            0x21, 1, 0xaa
        }, frame);
        var decoded = Assert.IsType<HsmsDataMessage>(_codec.Decode(frame));
        Assert.Equal((ushort)3, decoded.SecsMessage.SessionId.Value);
        Assert.True(decoded.SecsMessage.ReplyExpected);
        Assert.Equal(new byte[] { 0xaa }, Assert.IsType<SecsBinaryItem>(decoded.SecsMessage.Item).Values.ToArray());
    }

    [Theory]
    [MemberData(nameof(ControlTypes))]
    public void EverySupportedControlMessageRoundTrips(HsmsSType sType)
    {
        var source = new HsmsControlMessage(HsmsHeader.CreateControl(
            sType,
            new SecsSystemBytes(0x01020304),
            headerByte2: sType == HsmsSType.RejectRequest ? (byte)HsmsSType.SelectRequest : (byte)0,
            headerByte3: sType == HsmsSType.RejectRequest ? (byte)HsmsRejectReason.UnsupportedSType : (byte)0));
        var decoded = Assert.IsType<HsmsControlMessage>(_codec.Decode(_codec.Encode(source)));
        Assert.Equal(sType, decoded.SType);
        Assert.Equal(0x01020304u, decoded.Header.SystemBytes.Value);
    }

    [Fact]
    public void SelectRequestHeaderMatchesGoldenBytes()
    {
        var frame = _codec.Encode(new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.SelectRequest, new SecsSystemBytes(0x01020304))));
        Assert.Equal(new byte[] { 0, 0, 0, 10, 0xff, 0xff, 0, 0, 0, 1, 1, 2, 3, 4 }, frame);
    }

    [Fact]
    public void IncrementalDecoderHandlesEverySplitPoint()
    {
        var frame = _codec.Encode(CreateData());
        for (var split = 0; split <= frame.Length; split++)
        {
            var decoder = new HsmsStreamDecoder(_codec);
            var firstMessages = decoder.Append(frame.AsSpan(0, split));
            var messages = split == frame.Length ? firstMessages : decoder.Append(frame.AsSpan(split));
            if (split != frame.Length) Assert.Empty(firstMessages);
            Assert.Single(messages);
            Assert.IsType<HsmsDataMessage>(messages[0]);
            decoder.Complete();
        }
    }

    [Fact]
    public void IncrementalDecoderReturnsConcatenatedFrames()
    {
        var first = _codec.Encode(CreateData());
        var second = _codec.Encode(new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.LinktestRequest, new SecsSystemBytes(9))));
        var decoder = new HsmsStreamDecoder(_codec);
        var messages = decoder.Append(first.Concat(second).ToArray());
        Assert.Equal(2, messages.Count);
        Assert.IsType<HsmsDataMessage>(messages[0]);
        Assert.Equal(HsmsSType.LinktestRequest, Assert.IsType<HsmsControlMessage>(messages[1]).SType);
    }

    [Fact]
    public void IncrementalDecoderHandlesChunkLargerThanSingleFrameLimit()
    {
        var codec = new HsmsFrameCodec(new HsmsFrameCodecOptions { MaximumFrameLength = 10 }, new SecsItemCodec());
        var frame = codec.Encode(new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.LinktestRequest, new SecsSystemBytes(9))));
        var chunk = Enumerable.Range(0, 100).SelectMany(_ => frame).ToArray();
        var messages = new HsmsStreamDecoder(codec).Append(chunk);
        Assert.Equal(100, messages.Count);
    }

    [Fact]
    public void IncompleteFrameIsReportedAtInputCompletion()
    {
        var decoder = new HsmsStreamDecoder(_codec);
        Assert.Empty(decoder.Append(_codec.Encode(CreateData()).AsSpan(0, 8)));
        var exception = Assert.Throws<SecsDecodeException>(decoder.Complete);
        Assert.Equal(SecsValidationCode.Truncated, exception.Code);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(9)]
    public void LengthBelowHeaderIsRejected(int length)
    {
        var frame = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(frame, length);
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(frame));
        Assert.Equal(SecsValidationCode.InvalidLength, exception.Code);
        Assert.Null(exception.HsmsHeader);
    }

    [Fact]
    public void TruncatedHeaderAndFrameAreRejected()
    {
        var frame = new byte[13];
        BinaryPrimitives.WriteInt32BigEndian(frame, 10);
        var partialHeader = Assert.Throws<SecsDecodeException>(() => _codec.Decode(frame));
        Assert.Equal(SecsValidationCode.Truncated, partialHeader.Code);
        Assert.Null(partialHeader.HsmsHeader);
        var partialPrefix = Assert.Throws<SecsDecodeException>(() => _codec.Decode(new byte[] { 0, 0, 0 }));
        Assert.Equal(SecsValidationCode.Truncated, partialPrefix.Code);
        Assert.Null(partialPrefix.HsmsHeader);
    }

    [Fact]
    public void TrailingFrameBytesAreRejected()
    {
        var frame = _codec.Encode(CreateData()).Concat(new byte[] { 0 }).ToArray();
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(frame));
        Assert.Equal(SecsValidationCode.TrailingData, exception.Code);
        Assert.Equal(HsmsHeader.CreateData(CreateData().SecsMessage), exception.HsmsHeader);
    }

    [Theory]
    [InlineData(4, 1, SecsValidationCode.UnsupportedProtocolType)]
    [InlineData(5, 8, SecsValidationCode.UnsupportedProtocolType)]
    public void UnsupportedPTypeAndSTypeAreRejected(int headerOffset, byte value, SecsValidationCode expected)
    {
        var frame = _codec.Encode(new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.SelectRequest, new SecsSystemBytes(1))));
        frame[HsmsFrameCodec.LengthPrefixSize + headerOffset] = value;
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(frame));
        Assert.Equal(expected, exception.Code);
        var header = Assert.IsType<HsmsHeader>(exception.HsmsHeader);
        Assert.Equal((ushort)ushort.MaxValue, header.SessionId);
        Assert.Equal(new SecsSystemBytes(1), header.SystemBytes);
        Assert.Equal(frame[8], header.PType);
        Assert.Equal(frame[9], header.SType);
    }

    [Theory]
    [InlineData(1, (byte)HsmsSType.SelectRequest)]
    [InlineData(0, 8)]
    public void UnsupportedOutboundPTypeAndSTypeAreRejected(byte pType, byte sType)
    {
        var header = new HsmsHeader(
            ushort.MaxValue,
            0,
            0,
            pType,
            sType,
            new SecsSystemBytes(1));

        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Encode(new HsmsControlMessage(header)));
        Assert.Equal(SecsValidationCode.UnsupportedProtocolType, exception.Code);
        Assert.Equal(header, exception.HsmsHeader);
    }

    [Theory]
    [InlineData(1, (byte)HsmsSType.SelectRequest)]
    [InlineData(0, 8)]
    public async Task UnsupportedOutboundProtocolHeaderIsRejectedBeforeAnyBytesAreWritten(byte pType, byte sType)
    {
        var header = new HsmsHeader(
            ushort.MaxValue,
            0,
            0,
            pType,
            sType,
            new SecsSystemBytes(0x10203040));
        await using var destination = new MemoryStream();

        var exception = await Assert.ThrowsAsync<SecsDecodeException>(
            () => _codec.WriteAsync(destination, new HsmsControlMessage(header)));

        Assert.Equal(SecsValidationCode.UnsupportedProtocolType, exception.Code);
        Assert.Equal(header, exception.HsmsHeader);
        Assert.Equal(0, destination.Length);
    }

    [Fact]
    public void ControlTextIsRejected()
    {
        var frame = _codec.Encode(new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.SelectRequest, new SecsSystemBytes(1))));
        Array.Resize(ref frame, frame.Length + 1);
        BinaryPrimitives.WriteInt32BigEndian(frame, 11);
        Assert.Equal(SecsValidationCode.InvalidLength, Assert.Throws<SecsDecodeException>(() => _codec.Decode(frame)).Code);
    }

    [Theory]
    [InlineData(HsmsSType.SelectRequest, 2, 1)]
    [InlineData(HsmsSType.DeselectRequest, 3, 1)]
    [InlineData(HsmsSType.LinktestRequest, 2, 1)]
    [InlineData(HsmsSType.SeparateRequest, 3, 1)]
    public void ReservedControlHeaderBytesAreRejected(HsmsSType type, int headerByteIndex, byte value)
    {
        var frame = _codec.Encode(new HsmsControlMessage(HsmsHeader.CreateControl(type, new SecsSystemBytes(1))));
        frame[HsmsFrameCodec.LengthPrefixSize + headerByteIndex] = value;
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(frame));
        Assert.Equal(SecsValidationCode.InvalidMessage, exception.Code);
        var header = Assert.IsType<HsmsHeader>(exception.HsmsHeader);
        Assert.Equal(frame[HsmsFrameCodec.LengthPrefixSize + 2], header.HeaderByte2);
        Assert.Equal(frame[HsmsFrameCodec.LengthPrefixSize + 3], header.HeaderByte3);
    }

    [Fact]
    public void LinktestRequiresFfffSessionAndRejectRequiresReason()
    {
        var linktest = new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.LinktestRequest, new SecsSystemBytes(1), sessionId: 1));
        Assert.Equal(SecsValidationCode.InvalidMessage, Assert.Throws<SecsDecodeException>(() => _codec.Encode(linktest)).Code);
        var reject = new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.RejectRequest, new SecsSystemBytes(1)));
        Assert.Equal(SecsValidationCode.InvalidMessage, Assert.Throws<SecsDecodeException>(() => _codec.Encode(reject)).Code);
    }

    [Fact]
    public void MaximumFrameLengthIsEnforcedBeforeAllocation()
    {
        var codec = new HsmsFrameCodec(new HsmsFrameCodecOptions { MaximumFrameLength = 10 }, new SecsItemCodec());
        Assert.Throws<SecsDecodeException>(() => codec.Encode(CreateData()));
        var decoder = new HsmsStreamDecoder(codec);
        Assert.Equal(SecsValidationCode.SizeLimitExceeded, Assert.Throws<SecsDecodeException>(() => decoder.Append(new byte[] { 0, 0, 0, 11 })).Code);
    }

    [Fact]
    public async Task StreamReadAndWriteHandlePartialReads()
    {
        var message = CreateData();
        await using var stream = new MemoryStream();
        await _codec.WriteAsync(stream, message);
        stream.Position = 0;
        var decoded = await _codec.ReadAsync(new OneByteReadStream(stream), TimeSpan.FromSeconds(5), TimeProvider.System);
        Assert.IsType<HsmsDataMessage>(decoded);
    }

    [Fact]
    public async Task StreamTruncationAfterCompleteHeaderPreservesTypedContext()
    {
        var message = CreateData();
        var completeFrame = _codec.Encode(message);
        var truncatedFrame = completeFrame[..(HsmsFrameCodec.LengthPrefixSize + HsmsFrameCodec.HeaderLength + 1)];
        await using var stream = new MemoryStream(truncatedFrame);

        var exception = await Assert.ThrowsAsync<SecsDecodeException>(
            () => _codec.ReadAsync(stream, TimeSpan.FromSeconds(5), TimeProvider.System));

        Assert.Equal(SecsValidationCode.Truncated, exception.Code);
        Assert.Equal(HsmsHeader.CreateData(message.SecsMessage), exception.HsmsHeader);
    }

    private static HsmsDataMessage CreateData() => new(new SecsMessage(
        new SecsSessionId(3), new SecsStream(1), new SecsFunction(1), true,
        new SecsSystemBytes(0x01020304), new SecsBinaryItem(0xaa)));

    private sealed class OneByteReadStream(Stream inner) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => inner.Position = value; }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, Math.Min(1, count));
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
        public override void SetLength(long value) => inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
    }
}
