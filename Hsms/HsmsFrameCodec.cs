using System.Buffers.Binary;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Codecs;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>E37-0413 §8의 길이 접두사와 10바이트 헤더를 변환합니다.</para> \endif \if EN <para>Converts the E37-0413 §8 length prefix and ten-byte header.</para> \endif</summary>
public sealed class HsmsFrameCodec
{
    /// <summary>\if KO HSMS 길이 접두사 크기입니다. \endif \if EN The HSMS length-prefix size. \endif</summary>
    public const int LengthPrefixSize = 4;
    /// <summary>\if KO HSMS 헤더 크기입니다. \endif \if EN The HSMS header size. \endif</summary>
    public const int HeaderLength = 10;

    private readonly HsmsFrameCodecOptions _options;
    private readonly SecsItemCodec _itemCodec;

    /// <summary>\if KO 기본 제한으로 codec을 만듭니다. \endif \if EN Creates a codec with default limits. \endif</summary>
    public HsmsFrameCodec() : this(new HsmsFrameCodecOptions(), new SecsItemCodec()) { }

    /// <summary>\if KO 지정한 frame 및 item 설정으로 codec을 만듭니다. \endif \if EN Creates a codec with specified frame and item settings. \endif</summary>
    /// <param name="options">\if KO frame 설정입니다. \endif \if EN Frame settings. \endif</param><param name="itemCodec">\if KO SECS-II item codec입니다. \endif \if EN SECS-II item codec. \endif</param>
    public HsmsFrameCodec(HsmsFrameCodecOptions options, SecsItemCodec itemCodec)
    {
        ArgumentNullException.ThrowIfNull(options); ArgumentNullException.ThrowIfNull(itemCodec);
        options.Validate(); _options = options; _itemCodec = itemCodec;
    }

    /// <summary>\if KO 최대 frame 길이를 가져옵니다. \endif \if EN Gets the maximum frame length. \endif</summary>
    public int MaximumFrameLength => _options.MaximumFrameLength;

    internal int MaximumMessageLength => _itemCodec.MaximumMessageLength;

    /// <summary>\if KO 메시지를 접두사를 포함한 완전한 frame으로 인코딩합니다. \endif \if EN Encodes a message as a complete frame including its prefix. \endif</summary>
    /// <param name="message">\if KO 메시지입니다. \endif \if EN Message. \endif</param><returns>\if KO frame byte입니다. \endif \if EN Frame bytes. \endif</returns>
    public byte[] Encode(HsmsMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        ValidateProtocolHeader(message.Header);
        if (message is HsmsControlMessage outboundControl) ValidateControlHeader(outboundControl.Header);
        var item = message switch
        {
            HsmsDataMessage data => data.SecsMessage.Item,
            HsmsControlMessage => null,
            _ => throw new ArgumentException($"Unsupported HSMS message type {message.GetType().FullName}.", nameof(message))
        };
        var textLength = item is null ? 0 : _itemCodec.GetEncodedLength(item);
        var frameLength = checked(HeaderLength + textLength);
        ValidateFrameLength(frameLength, 0, message.Header);
        var text = item is null ? Array.Empty<byte>() : _itemCodec.EncodeValidated(item, textLength);
        var totalLength = GetTotalLength(frameLength, 0, message.Header);
        var frame = new byte[totalLength];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)frameLength);
        WriteHeader(frame.AsSpan(LengthPrefixSize, HeaderLength), message.Header);
        text.CopyTo(frame, LengthPrefixSize + HeaderLength);
        return frame;
    }

    /// <summary>\if KO 접두사를 포함한 완전한 단일 frame을 디코딩합니다. \endif \if EN Decodes one complete frame including its prefix. \endif</summary>
    /// <param name="frame">\if KO frame입니다. \endif \if EN Frame. \endif</param><returns>\if KO 메시지입니다. \endif \if EN Message. \endif</returns>
    public HsmsMessage Decode(ReadOnlyMemory<byte> frame)
    {
        if (frame.Length < LengthPrefixSize) throw Error(SecsValidationCode.Truncated, "HSMS length prefix is truncated.", 0);
        var declared = BinaryPrimitives.ReadUInt32BigEndian(frame.Span);
        HsmsHeader? headerContext = declared >= HeaderLength && frame.Length >= LengthPrefixSize + HeaderLength
            ? ReadHeader(frame.Span.Slice(LengthPrefixSize, HeaderLength))
            : null;
        if (declared > int.MaxValue) throw Error(SecsValidationCode.InvalidLength, "HSMS length exceeds the supported integer range.", 0, headerContext);
        var frameLength = (int)declared;
        ValidateFrameLength(frameLength, 0, headerContext);
        ValidateMessageLength(frameLength, 0, headerContext);
        var totalLength = GetTotalLength(frameLength, 0, headerContext);
        if (frame.Length != totalLength)
        {
            var code = frame.Length < totalLength ? SecsValidationCode.Truncated : SecsValidationCode.TrailingData;
            throw Error(code, $"Declared HSMS length is {frameLength}, actual is {frame.Length - LengthPrefixSize}.", LengthPrefixSize, headerContext);
        }
        var header = headerContext!.Value;
        var text = frame.Slice(LengthPrefixSize + HeaderLength);
        return CreateMessage(header, text);
    }

    /// <summary>\if KO Stream에 frame을 비동기 기록합니다. \endif \if EN Asynchronously writes a frame to a stream. \endif</summary>
    /// <param name="stream">\if KO 대상 stream입니다. \endif \if EN Destination stream. \endif</param><param name="message">\if KO 메시지입니다. \endif \if EN Message. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task WriteAsync(Stream stream, HsmsMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var frame = Encode(message);
        await WriteEncodedAsync(stream, frame, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task WriteEncodedAsync(Stream stream, ReadOnlyMemory<byte> frame, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        await stream.WriteAsync(frame, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>\if KO T8을 적용해 Stream에서 한 frame을 읽습니다. 첫 바이트 전 EOF는 null입니다. \endif \if EN Reads one frame with T8 enforcement; EOF before the first byte returns null. \endif</summary>
    /// <param name="stream">\if KO 원본 stream입니다. \endif \if EN Source stream. \endif</param><param name="t8">\if KO 바이트 간 제한 시간입니다. \endif \if EN Inter-character timeout. \endif</param><param name="timeProvider">\if KO 시간 공급자입니다. \endif \if EN Time provider. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param><returns>\if KO 메시지 또는 EOF의 null입니다. \endif \if EN Message or null at EOF. \endif</returns>
    public async Task<HsmsMessage?> ReadAsync(Stream stream, TimeSpan t8, TimeProvider timeProvider, CancellationToken cancellationToken = default)
    {
        var frame = await ReadRawFrameAsync(stream, t8, timeProvider, cancellationToken).ConfigureAwait(false);
        return frame is null ? null : Decode(frame);
    }

    internal async Task<byte[]?> ReadRawFrameAsync(Stream stream, TimeSpan t8, TimeProvider timeProvider, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream); ArgumentNullException.ThrowIfNull(timeProvider);
        if (t8 <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(t8));
        var prefix = new byte[LengthPrefixSize];
        var prefixRead = await ReadExactAsync(stream, prefix, false, t8, timeProvider, cancellationToken).ConfigureAwait(false);
        if (!prefixRead) return null;
        var declared = BinaryPrimitives.ReadUInt32BigEndian(prefix);
        if (declared > int.MaxValue) throw Error(SecsValidationCode.InvalidLength, "HSMS length exceeds the supported integer range.", 0);
        var frameLength = (int)declared;
        ValidateFrameLength(frameLength, 0);
        ValidateMessageLength(frameLength, 0);
        var totalLength = GetTotalLength(frameLength, 0);
        var frame = new byte[totalLength];
        prefix.CopyTo(frame, 0);
        try
        {
            _ = await ReadExactAsync(stream, frame.AsMemory(LengthPrefixSize), true, t8, timeProvider, cancellationToken).ConfigureAwait(false);
        }
        catch (SecsDecodeException exception) when (exception.HsmsHeader is null && exception.Offset >= HeaderLength)
        {
            var header = ReadHeader(frame.AsSpan(LengthPrefixSize, HeaderLength));
            throw Error(exception.Code, exception.Message, exception.Offset, header);
        }
        return frame;
    }

    private HsmsMessage CreateMessage(HsmsHeader header, ReadOnlyMemory<byte> text)
    {
        ValidateProtocolHeader(header);
        if (!header.IsData)
        {
            if (!text.IsEmpty) throw Error(SecsValidationCode.InvalidLength, "Control messages cannot contain message text.", LengthPrefixSize + HeaderLength, header);
            ValidateControlHeader(header);
            return new HsmsControlMessage(header);
        }
        if (header.SessionId > SecsSessionId.MaximumValue) throw Error(SecsValidationCode.OutOfRange, "Data Session ID exceeds 32767.", 4, header);
        SecsItem? item;
        try { item = text.IsEmpty ? null : _itemCodec.Decode(text); }
        catch (SecsDecodeException exception)
        {
            throw Error(exception.Code, exception.Message, exception.Offset, header);
        }
        SecsMessage secs;
        try
        {
            secs = new SecsMessage(new SecsSessionId(header.SessionId), new SecsStream(header.Stream), new SecsFunction(header.Function), header.ReplyExpected, header.SystemBytes, item);
        }
        catch (ArgumentException exception)
        {
            throw Error(SecsValidationCode.InvalidMessage, exception.Message, 4, header);
        }
        return new HsmsDataMessage(secs);
    }

    private void ValidateFrameLength(int frameLength, int offset, HsmsHeader? header = null)
    {
        if (frameLength < HeaderLength) throw Error(SecsValidationCode.InvalidLength, $"HSMS length must be at least {HeaderLength}.", offset, header);
        if (frameLength > _options.MaximumFrameLength) throw Error(SecsValidationCode.SizeLimitExceeded, $"HSMS length exceeds {_options.MaximumFrameLength}.", offset, header);
    }

    private void ValidateMessageLength(int frameLength, int offset, HsmsHeader? header = null)
    {
        var messageLength = frameLength - HeaderLength;
        if (messageLength > _itemCodec.MaximumMessageLength)
            throw Error(SecsValidationCode.SizeLimitExceeded, $"HSMS message text exceeds {_itemCodec.MaximumMessageLength} bytes.", offset, header);
    }

    private static int GetTotalLength(int frameLength, int offset, HsmsHeader? header = null)
    {
        if (frameLength > int.MaxValue - LengthPrefixSize) throw Error(SecsValidationCode.InvalidLength, "HSMS frame plus length prefix exceeds the supported integer range.", offset, header);
        return LengthPrefixSize + frameLength;
    }

    private static bool IsSupportedSType(byte value) => value is 0 or 1 or 2 or 3 or 4 or 5 or 6 or 7 or 9;

    private static void ValidateProtocolHeader(HsmsHeader header)
    {
        if (header.PType != 0) throw Error(SecsValidationCode.UnsupportedProtocolType, $"Unsupported PType {header.PType}.", 8, header);
        if (!IsSupportedSType(header.SType)) throw Error(SecsValidationCode.UnsupportedProtocolType, $"Unsupported SType {header.SType}.", 9, header);
    }

    private static void ValidateControlHeader(HsmsHeader header)
    {
        var type = (HsmsSType)header.SType;
        if (type != HsmsSType.RejectRequest && header.HeaderByte2 != 0)
            throw Error(SecsValidationCode.InvalidMessage, $"{type} requires Header Byte 2 to be zero.", 6, header);
        if (type is HsmsSType.SelectRequest or HsmsSType.DeselectRequest or HsmsSType.LinktestRequest or HsmsSType.LinktestResponse or HsmsSType.SeparateRequest)
        {
            if (header.HeaderByte3 != 0)
                throw Error(SecsValidationCode.InvalidMessage, $"{type} requires Header Byte 3 to be zero.", 7, header);
        }
        if ((type is HsmsSType.LinktestRequest or HsmsSType.LinktestResponse) && header.SessionId != ushort.MaxValue)
            throw Error(SecsValidationCode.InvalidMessage, $"{type} requires Session ID 0xFFFF.", 4, header);
        if (type == HsmsSType.RejectRequest && header.HeaderByte3 == 0)
            throw Error(SecsValidationCode.InvalidMessage, "RejectRequest requires a non-zero reason code.", 7, header);
    }

    private static void WriteHeader(Span<byte> destination, HsmsHeader header)
    {
        BinaryPrimitives.WriteUInt16BigEndian(destination, header.SessionId);
        destination[2] = header.HeaderByte2; destination[3] = header.HeaderByte3; destination[4] = header.PType; destination[5] = header.SType;
        BinaryPrimitives.WriteUInt32BigEndian(destination[6..], header.SystemBytes.Value);
    }

    private static HsmsHeader ReadHeader(ReadOnlySpan<byte> source) => new(
        BinaryPrimitives.ReadUInt16BigEndian(source), source[2], source[3], source[4], source[5], new SecsSystemBytes(BinaryPrimitives.ReadUInt32BigEndian(source[6..])));

    private static async Task<bool> ReadExactAsync(Stream stream, Memory<byte> destination, bool firstByteAlreadyRead, TimeSpan t8, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < destination.Length)
        {
            var enforceT8 = firstByteAlreadyRead || offset > 0;
            var read = enforceT8
                ? await ReadWithTimeoutAsync(stream, destination[offset..], t8, timeProvider, cancellationToken).ConfigureAwait(false)
                : await stream.ReadAsync(destination[offset..], cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                if (offset == 0 && !firstByteAlreadyRead) return false;
                throw Error(SecsValidationCode.Truncated, "The stream ended inside an HSMS frame.", offset);
            }
            offset += read;
        }
        return true;
    }

    private static async Task<int> ReadWithTimeoutAsync(Stream stream, Memory<byte> destination, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        using var timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var readTask = stream.ReadAsync(destination, timeoutCancellation.Token).AsTask();
        var delayTask = Task.Delay(timeout, timeProvider, timeoutCancellation.Token);
        var completed = await Task.WhenAny(readTask, delayTask).ConfigureAwait(false);
        if (completed == readTask)
        {
            timeoutCancellation.Cancel();
            return await readTask.ConfigureAwait(false);
        }
        cancellationToken.ThrowIfCancellationRequested();
        timeoutCancellation.Cancel();
        try { await readTask.ConfigureAwait(false); }
        catch (OperationCanceledException)
        {
            // The timeout path canceled this pending read; observing cancellation prevents an unobserved task.
        }
        throw new HsmsTimerExpiredException("T8", timeout);
    }

    private static SecsDecodeException Error(SecsValidationCode code, string message, int? offset = null, HsmsHeader? header = null) =>
        new(code, message, offset, header);
}
