namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>HSMS frame codec의 크기 제한입니다.</para> \endif \if EN <para>Configures HSMS frame-codec size limits.</para> \endif</summary>
public sealed class HsmsFrameCodecOptions
{
    /// <summary>\if KO 길이 접두사가 나타내는 최대 바이트 수입니다. \endif \if EN Gets the maximum byte count represented by the length prefix. \endif</summary>
    public int MaximumFrameLength { get; init; } = 16 * 1024 * 1024;
    /// <summary>\if KO 설정을 검증합니다. \endif \if EN Validates the setting. \endif</summary>
    public void Validate()
    {
        if (MaximumFrameLength < HsmsFrameCodec.HeaderLength || MaximumFrameLength > int.MaxValue - HsmsFrameCodec.LengthPrefixSize)
            throw new ArgumentOutOfRangeException(nameof(MaximumFrameLength));
    }
}
