using System.Buffers.Binary;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Validation;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>임의로 분할되거나 연속된 입력에서 완전한 HSMS frame을 추출합니다.</para> \endif \if EN <para>Extracts complete HSMS frames from arbitrarily split or concatenated input.</para> \endif</summary>
public sealed class HsmsStreamDecoder
{
    private readonly object _gate = new();
    private readonly HsmsFrameCodec _codec;
    private byte[] _buffer = new byte[256];
    private int _count;

    /// <summary>\if KO 지정한 frame codec으로 decoder를 만듭니다. \endif \if EN Creates a decoder with the specified frame codec. \endif</summary>
    /// <param name="codec">\if KO frame codec입니다. \endif \if EN Frame codec. \endif</param>
    public HsmsStreamDecoder(HsmsFrameCodec codec) => _codec = codec ?? throw new ArgumentNullException(nameof(codec));

    /// <summary>\if KO 현재 보류 중인 바이트 수입니다. \endif \if EN Gets the number of currently buffered bytes. \endif</summary>
    public int BufferedByteCount { get { lock (_gate) return _count; } }

    /// <summary>\if KO 입력 조각을 추가하고 완성된 모든 메시지를 반환합니다. \endif \if EN Appends a chunk and returns every completed message. \endif</summary>
    /// <param name="data">\if KO 입력 조각입니다. \endif \if EN Input chunk. \endif</param><returns>\if KO 완성된 메시지입니다. \endif \if EN Completed messages. \endif</returns>
    public IReadOnlyList<HsmsMessage> Append(ReadOnlySpan<byte> data)
    {
        lock (_gate)
        {
            var messages = new List<HsmsMessage>();
            while (!data.IsEmpty)
            {
                if (_count < HsmsFrameCodec.LengthPrefixSize)
                {
                    var prefixBytes = Math.Min(HsmsFrameCodec.LengthPrefixSize - _count, data.Length);
                    data[..prefixBytes].CopyTo(_buffer.AsSpan(_count));
                    _count += prefixBytes;
                    data = data[prefixBytes..];
                    if (_count < HsmsFrameCodec.LengthPrefixSize) break;
                }

                var declared = BinaryPrimitives.ReadUInt32BigEndian(_buffer.AsSpan(0, 4));
                if (declared > int.MaxValue || declared < HsmsFrameCodec.HeaderLength)
                    throw new SecsDecodeException(SecsValidationCode.InvalidLength, $"Invalid HSMS length {declared}.", 0);
                if (declared > _codec.MaximumFrameLength)
                    throw new SecsDecodeException(SecsValidationCode.SizeLimitExceeded, $"HSMS length exceeds {_codec.MaximumFrameLength}.", 0);
                var total = HsmsFrameCodec.LengthPrefixSize + (int)declared;
                EnsureCapacity(total);
                var copied = Math.Min(total - _count, data.Length);
                data[..copied].CopyTo(_buffer.AsSpan(_count));
                _count += copied;
                data = data[copied..];
                if (_count < total) break;
                messages.Add(_codec.Decode(_buffer.AsMemory(0, total)));
                _count = 0;
            }
            return messages;
        }
    }

    /// <summary>\if KO 입력 종료를 표시하고 불완전 frame을 검출합니다. \endif \if EN Marks end of input and detects an incomplete frame. \endif</summary>
    public void Complete()
    {
        lock (_gate)
        {
            if (_count != 0) throw new SecsDecodeException(SecsValidationCode.Truncated, $"Input ended with {_count} buffered HSMS byte(s).", 0);
        }
    }

    /// <summary>\if KO 보류 중인 입력을 폐기합니다. \endif \if EN Discards buffered input. \endif</summary>
    public void Reset() { lock (_gate) _count = 0; }

    private void EnsureCapacity(int required)
    {
        var maximumBuffered = _codec.MaximumFrameLength + HsmsFrameCodec.LengthPrefixSize;
        if (required > maximumBuffered) throw new SecsDecodeException(SecsValidationCode.SizeLimitExceeded, $"Buffered input exceeds {maximumBuffered} bytes.");
        if (required <= _buffer.Length) return;
        var doubled = _buffer.Length > maximumBuffered / 2 ? maximumBuffered : _buffer.Length * 2;
        var size = Math.Min(maximumBuffered, Math.Max(required, doubled));
        Array.Resize(ref _buffer, size);
    }
}
