using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Dreamine.Secs.Abstractions.Codecs;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;

namespace Dreamine.Secs.Com.Codecs;

/// <summary>\if KO <para>E5-0813 §9 item wire format을 구현하는 순수 codec입니다.</para> \endif \if EN <para>Implements the E5-0813 §9 item wire format without network dependencies.</para> \endif</summary>
public sealed class SecsItemCodec : ISecsItemCodec
{
    private const int MaximumItemLength = 0x00ff_ffff;
    private readonly SecsItemCodecOptions _options;

    /// <summary>\if KO 기본 제한으로 codec을 만듭니다. \endif \if EN Creates a codec with default limits. \endif</summary>
    public SecsItemCodec() : this(new SecsItemCodecOptions()) { }

    /// <summary>\if KO 지정한 방어 한계로 codec을 만듭니다. \endif \if EN Creates a codec with specified defensive limits. \endif</summary>
    /// <param name="options">\if KO 제한 설정입니다. \endif \if EN Limit settings. \endif</param>
    public SecsItemCodec(SecsItemCodecOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
    }

    internal int MaximumMessageLength => _options.MaximumMessageLength;

    /// <inheritdoc />
    public byte[] Encode(SecsItem item)
    {
        ArgumentNullException.ThrowIfNull(item);
        var encodedLength = GetEncodedLength(item);
        return EncodeValidated(item, encodedLength);
    }

    internal int GetEncodedLength(SecsItem item)
    {
        ArgumentNullException.ThrowIfNull(item);
        return GetEncodedLength(item, 0);
    }

    internal byte[] EncodeValidated(SecsItem item, int encodedLength)
    {
        ArgumentNullException.ThrowIfNull(item);
        var writer = new ArrayBufferWriter<byte>(encodedLength);
        EncodeItem(item, writer, 0);
        return writer.WrittenSpan.ToArray();
    }

    /// <inheritdoc />
    public SecsItem Decode(ReadOnlyMemory<byte> data)
    {
        if (data.Length > _options.MaximumMessageLength)
            throw Error(SecsValidationCode.SizeLimitExceeded, $"Input exceeds {_options.MaximumMessageLength} bytes.", 0);
        var offset = 0;
        var item = DecodeItem(data.Span, ref offset, 0);
        if (offset != data.Length)
            throw Error(SecsValidationCode.TrailingData, "Input contains bytes after the decoded item.", offset);
        return item;
    }

    /// <inheritdoc />
    public SecsValidationResult Validate(ReadOnlyMemory<byte> data)
    {
        try
        {
            _ = Decode(data);
            return SecsValidationResult.Success;
        }
        catch (SecsProtocolException exception)
        {
            return SecsValidationResult.Failure(exception.Code, exception.Message, exception.Offset);
        }
    }

    private void EncodeItem(SecsItem item, IBufferWriter<byte> writer, int depth)
    {
        if (depth > _options.MaximumNestingDepth)
            throw Error(SecsValidationCode.DepthLimitExceeded, $"Item nesting exceeds {_options.MaximumNestingDepth}.");

        var length = item is SecsListItem list ? list.Count : item.BodyLength;
        WriteHeader(writer, item.Format, length);

        switch (item)
        {
            case SecsListItem value:
                foreach (var child in value.Items) EncodeItem(child, writer, depth + 1);
                break;
            case SecsBinaryItem value: Write(writer, value.Values.Span); break;
            case SecsBooleanItem value:
                foreach (var element in value.Values.Span) WriteByte(writer, element ? (byte)1 : (byte)0);
                break;
            case SecsAsciiItem value: Write(writer, Encoding.ASCII.GetBytes(value.Value)); break;
            case SecsJis8Item value: Write(writer, value.Values.Span); break;
            case SecsInt8Item value:
                foreach (var element in value.Values.Span) WriteByte(writer, unchecked((byte)element));
                break;
            case SecsUInt8Item value: Write(writer, value.Values.Span); break;
            case SecsInt16Item value: WriteInt16(writer, value.Values.Span); break;
            case SecsUInt16Item value: WriteUInt16(writer, value.Values.Span); break;
            case SecsInt32Item value: WriteInt32(writer, value.Values.Span); break;
            case SecsUInt32Item value: WriteUInt32(writer, value.Values.Span); break;
            case SecsInt64Item value: WriteInt64(writer, value.Values.Span); break;
            case SecsUInt64Item value: WriteUInt64(writer, value.Values.Span); break;
            case SecsFloat32Item value:
                foreach (var element in value.Values.Span) WriteOneInt32(writer, BitConverter.SingleToInt32Bits(element));
                break;
            case SecsFloat64Item value:
                foreach (var element in value.Values.Span) WriteOneInt64(writer, BitConverter.DoubleToInt64Bits(element));
                break;
            default:
                throw Error(SecsValidationCode.UnsupportedFormat, $"Unsupported item type {item.GetType().FullName}.");
        }

        if (writer is ArrayBufferWriter<byte> buffer && buffer.WrittenCount > _options.MaximumMessageLength)
            throw Error(SecsValidationCode.SizeLimitExceeded, $"Encoded item exceeds {_options.MaximumMessageLength} bytes.");
    }

    private SecsItem DecodeItem(ReadOnlySpan<byte> data, ref int offset, int depth)
    {
        if (depth > _options.MaximumNestingDepth)
            throw Error(SecsValidationCode.DepthLimitExceeded, $"Item nesting exceeds {_options.MaximumNestingDepth}.", offset);
        var headerOffset = offset;
        Require(data, offset, 1);
        var formatByte = data[offset++];
        var lengthByteCount = formatByte & 0x03;
        if (lengthByteCount == 0)
            throw Error(SecsValidationCode.InvalidLength, "An item header cannot have zero length bytes.", headerOffset);
        Require(data, offset, lengthByteCount);
        var length = 0;
        for (var index = 0; index < lengthByteCount; index++) length = (length << 8) | data[offset++];
        var formatCode = (byte)(formatByte >> 2);
        if (formatCode == (byte)SecsItemFormat.List)
        {
            if (length > _options.MaximumListItemCount)
                throw Error(SecsValidationCode.SizeLimitExceeded, "List child count exceeds the configured limit.", headerOffset);
            // Every child needs at least a format byte and one length byte. Reject an
            // impossible count before allocating the child-reference array.
            if (length > (data.Length - offset) / 2)
                throw Error(SecsValidationCode.Truncated, "List child count cannot fit in the remaining input.", headerOffset);
            var children = new SecsItem[length];
            for (var index = 0; index < length; index++) children[index] = DecodeItem(data, ref offset, depth + 1);
            return new SecsListItem(children);
        }

        Require(data, offset, length);
        var body = data.Slice(offset, length);
        offset += length;
        return DecodeAtomic(formatCode, body, headerOffset);
    }

    private static SecsItem DecodeAtomic(byte formatCode, ReadOnlySpan<byte> body, int headerOffset)
    {
        return (SecsItemFormat)formatCode switch
        {
            SecsItemFormat.Binary => new SecsBinaryItem(body.ToArray()),
            SecsItemFormat.Boolean => new SecsBooleanItem(body.ToArray().Select(static value => value != 0).ToArray()),
            SecsItemFormat.Ascii => DecodeAscii(body, headerOffset),
            SecsItemFormat.Jis8 => new SecsJis8Item(body.ToArray()),
            SecsItemFormat.Int8 => new SecsInt8Item(body.ToArray().Select(static value => unchecked((sbyte)value)).ToArray()),
            SecsItemFormat.UInt8 => new SecsUInt8Item(body.ToArray()),
            SecsItemFormat.Int16 => new SecsInt16Item(ReadInt16(body, headerOffset)),
            SecsItemFormat.UInt16 => new SecsUInt16Item(ReadUInt16(body, headerOffset)),
            SecsItemFormat.Int32 => new SecsInt32Item(ReadInt32(body, headerOffset)),
            SecsItemFormat.UInt32 => new SecsUInt32Item(ReadUInt32(body, headerOffset)),
            SecsItemFormat.Int64 => new SecsInt64Item(ReadInt64(body, headerOffset)),
            SecsItemFormat.UInt64 => new SecsUInt64Item(ReadUInt64(body, headerOffset)),
            SecsItemFormat.Float32 => new SecsFloat32Item(ReadInt32(body, headerOffset).Select(BitConverter.Int32BitsToSingle).ToArray()),
            SecsItemFormat.Float64 => new SecsFloat64Item(ReadInt64(body, headerOffset).Select(BitConverter.Int64BitsToDouble).ToArray()),
            _ => throw Error(SecsValidationCode.UnsupportedFormat, $"Unsupported format code {formatCode}.", headerOffset)
        };
    }

    private int GetEncodedLength(SecsItem item, int depth)
    {
        if (depth > _options.MaximumNestingDepth)
            throw Error(SecsValidationCode.DepthLimitExceeded, $"Item nesting exceeds {_options.MaximumNestingDepth}.");

        int contentLength;
        int declaredLength;
        if (item is SecsListItem list)
        {
            if (list.Count > _options.MaximumListItemCount)
                throw Error(SecsValidationCode.SizeLimitExceeded, "List child count exceeds the configured limit.");
            declaredLength = list.Count;
            long total = 0;
            foreach (var child in list.Items)
            {
                total += GetEncodedLength(child, depth + 1);
                if (total > _options.MaximumMessageLength)
                    throw Error(SecsValidationCode.SizeLimitExceeded, $"Encoded item exceeds {_options.MaximumMessageLength} bytes.");
            }
            contentLength = (int)total;
        }
        else
        {
            declaredLength = item.BodyLength;
            contentLength = declaredLength;
        }

        if (declaredLength > MaximumItemLength)
            throw Error(SecsValidationCode.InvalidLength, $"Item length {declaredLength} is outside 0..{MaximumItemLength}.");
        var lengthBytes = declaredLength <= byte.MaxValue ? 1 : declaredLength <= ushort.MaxValue ? 2 : 3;
        var encodedLength = (long)1 + lengthBytes + contentLength;
        if (encodedLength > _options.MaximumMessageLength)
            throw Error(SecsValidationCode.SizeLimitExceeded, $"Encoded item exceeds {_options.MaximumMessageLength} bytes.");
        return (int)encodedLength;
    }

    private static SecsAsciiItem DecodeAscii(ReadOnlySpan<byte> body, int offset)
    {
        if (body.IndexOfAnyInRange((byte)0x80, byte.MaxValue) >= 0)
            throw Error(SecsValidationCode.InvalidMessage, "ASCII item contains a byte above 0x7F.", offset);
        return new SecsAsciiItem(Encoding.ASCII.GetString(body));
    }

    private static void WriteHeader(IBufferWriter<byte> writer, SecsItemFormat format, int length)
    {
        if (length < 0 || length > MaximumItemLength)
            throw Error(SecsValidationCode.InvalidLength, $"Item length {length} is outside 0..{MaximumItemLength}.");
        var lengthBytes = length <= byte.MaxValue ? 1 : length <= ushort.MaxValue ? 2 : 3;
        WriteByte(writer, (byte)(((byte)format << 2) | lengthBytes));
        for (var shift = (lengthBytes - 1) * 8; shift >= 0; shift -= 8) WriteByte(writer, (byte)(length >> shift));
    }

    private static void Require(ReadOnlySpan<byte> data, int offset, int count)
    {
        if (count < 0 || offset < 0 || count > data.Length - offset)
            throw Error(SecsValidationCode.Truncated, $"Expected {count} byte(s) at offset {offset}.", offset);
    }

    private static void Write(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        var destination = writer.GetSpan(value.Length);
        value.CopyTo(destination);
        writer.Advance(value.Length);
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        var destination = writer.GetSpan(1); destination[0] = value; writer.Advance(1);
    }

    private static void WriteInt16(IBufferWriter<byte> writer, ReadOnlySpan<short> values) { foreach (var value in values) { var span = writer.GetSpan(2); BinaryPrimitives.WriteInt16BigEndian(span, value); writer.Advance(2); } }
    private static void WriteUInt16(IBufferWriter<byte> writer, ReadOnlySpan<ushort> values) { foreach (var value in values) { var span = writer.GetSpan(2); BinaryPrimitives.WriteUInt16BigEndian(span, value); writer.Advance(2); } }
    private static void WriteInt32(IBufferWriter<byte> writer, ReadOnlySpan<int> values) { foreach (var value in values) WriteOneInt32(writer, value); }
    private static void WriteUInt32(IBufferWriter<byte> writer, ReadOnlySpan<uint> values) { foreach (var value in values) { var span = writer.GetSpan(4); BinaryPrimitives.WriteUInt32BigEndian(span, value); writer.Advance(4); } }
    private static void WriteInt64(IBufferWriter<byte> writer, ReadOnlySpan<long> values) { foreach (var value in values) WriteOneInt64(writer, value); }
    private static void WriteUInt64(IBufferWriter<byte> writer, ReadOnlySpan<ulong> values) { foreach (var value in values) { var span = writer.GetSpan(8); BinaryPrimitives.WriteUInt64BigEndian(span, value); writer.Advance(8); } }
    private static void WriteOneInt32(IBufferWriter<byte> writer, int value) { var span = writer.GetSpan(4); BinaryPrimitives.WriteInt32BigEndian(span, value); writer.Advance(4); }
    private static void WriteOneInt64(IBufferWriter<byte> writer, long value) { var span = writer.GetSpan(8); BinaryPrimitives.WriteInt64BigEndian(span, value); writer.Advance(8); }

    private static short[] ReadInt16(ReadOnlySpan<byte> body, int offset) { EnsureWidth(body, 2, offset); var result = new short[body.Length / 2]; for (var i = 0; i < result.Length; i++) result[i] = BinaryPrimitives.ReadInt16BigEndian(body.Slice(i * 2, 2)); return result; }
    private static ushort[] ReadUInt16(ReadOnlySpan<byte> body, int offset) { EnsureWidth(body, 2, offset); var result = new ushort[body.Length / 2]; for (var i = 0; i < result.Length; i++) result[i] = BinaryPrimitives.ReadUInt16BigEndian(body.Slice(i * 2, 2)); return result; }
    private static int[] ReadInt32(ReadOnlySpan<byte> body, int offset) { EnsureWidth(body, 4, offset); var result = new int[body.Length / 4]; for (var i = 0; i < result.Length; i++) result[i] = BinaryPrimitives.ReadInt32BigEndian(body.Slice(i * 4, 4)); return result; }
    private static uint[] ReadUInt32(ReadOnlySpan<byte> body, int offset) { EnsureWidth(body, 4, offset); var result = new uint[body.Length / 4]; for (var i = 0; i < result.Length; i++) result[i] = BinaryPrimitives.ReadUInt32BigEndian(body.Slice(i * 4, 4)); return result; }
    private static long[] ReadInt64(ReadOnlySpan<byte> body, int offset) { EnsureWidth(body, 8, offset); var result = new long[body.Length / 8]; for (var i = 0; i < result.Length; i++) result[i] = BinaryPrimitives.ReadInt64BigEndian(body.Slice(i * 8, 8)); return result; }
    private static ulong[] ReadUInt64(ReadOnlySpan<byte> body, int offset) { EnsureWidth(body, 8, offset); var result = new ulong[body.Length / 8]; for (var i = 0; i < result.Length; i++) result[i] = BinaryPrimitives.ReadUInt64BigEndian(body.Slice(i * 8, 8)); return result; }
    private static void EnsureWidth(ReadOnlySpan<byte> body, int width, int offset) { if (body.Length % width != 0) throw Error(SecsValidationCode.InvalidLength, $"Body length {body.Length} is not divisible by element width {width}.", offset); }
    private static SecsDecodeException Error(SecsValidationCode code, string message, int? offset = null) => new(code, message, offset);
}
