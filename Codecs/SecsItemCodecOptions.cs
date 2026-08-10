namespace Dreamine.Secs.Com.Codecs;

/// <summary>\if KO <para>SECS-II item codec의 방어 한계를 설정합니다.</para> \endif \if EN <para>Configures defensive limits for the SECS-II item codec.</para> \endif</summary>
public sealed class SecsItemCodecOptions
{
    /// <summary>\if KO 최대 인코딩/디코딩 바이트 수입니다. \endif \if EN Gets the maximum encoded or decoded byte count. \endif</summary>
    public int MaximumMessageLength { get; init; } = 16 * 1024 * 1024;
    /// <summary>\if KO 허용할 최대 목록 중첩 깊이입니다. \endif \if EN Gets the maximum permitted list nesting depth. \endif</summary>
    public int MaximumNestingDepth { get; init; } = 64;
    /// <summary>\if KO 단일 List에서 허용할 최대 자식 수입니다. \endif \if EN Gets the maximum child count permitted in one List item. \endif</summary>
    public int MaximumListItemCount { get; init; } = 65_535;

    /// <summary>\if KO 설정을 검증합니다. \endif \if EN Validates the settings. \endif</summary>
    public void Validate()
    {
        if (MaximumMessageLength <= 0) throw new ArgumentOutOfRangeException(nameof(MaximumMessageLength));
        if (MaximumNestingDepth < 0) throw new ArgumentOutOfRangeException(nameof(MaximumNestingDepth));
        if (MaximumListItemCount < 0 || MaximumListItemCount > 0x00ff_ffff) throw new ArgumentOutOfRangeException(nameof(MaximumListItemCount));
    }
}
