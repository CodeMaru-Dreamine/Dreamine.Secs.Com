using Dreamine.Secs.Abstractions.Model;

namespace Dreamine.Secs.Com.Transactions;

/// <summary>\if KO <para>세션 범위에서 원자적으로 순환하는 System Bytes를 생성합니다.</para> \endif \if EN <para>Generates atomically wrapping System Bytes within a session.</para> \endif</summary>
public sealed class SecsSystemBytesGenerator
{
    private int _current;

    /// <summary>\if KO 초기값 다음 수부터 생성하도록 generator를 만듭니다. \endif \if EN Creates a generator that returns the value after the seed first. \endif</summary>
    /// <param name="seed">\if KO 초기값입니다. \endif \if EN Initial value. \endif</param>
    public SecsSystemBytesGenerator(uint seed = 0) => _current = unchecked((int)seed);

    /// <summary>\if KO 다음 32비트 값을 원자적으로 가져옵니다. \endif \if EN Atomically gets the next 32-bit value. \endif</summary>
    public SecsSystemBytes Next() => new(unchecked((uint)Interlocked.Increment(ref _current)));
}
