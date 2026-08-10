using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Validation;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>TimeProvider로 HSMS 타이머를 결정론적으로 예약합니다.</para> \endif \if EN <para>Schedules HSMS timers deterministically through TimeProvider.</para> \endif</summary>
public sealed class HsmsTimerScheduler
{
    private readonly HsmsTimerOptions _options;
    private readonly TimeProvider _timeProvider;

    /// <summary>\if KO 설정과 시간 공급자로 scheduler를 만듭니다. \endif \if EN Creates a scheduler with options and a time provider. \endif</summary>
    /// <param name="options">\if KO 타이머 설정입니다. \endif \if EN Timer settings. \endif</param><param name="timeProvider">\if KO 시간 공급자입니다. \endif \if EN Time provider. \endif</param>
    public HsmsTimerScheduler(HsmsTimerOptions options, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options); options.Validate();
        _options = options; _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>\if KO 타이머의 설정 기간을 가져옵니다. \endif \if EN Gets the configured duration of a timer. \endif</summary>
    /// <param name="kind">\if KO 타이머입니다. \endif \if EN Timer. \endif</param><returns>\if KO 기간입니다. \endif \if EN Duration. \endif</returns>
    public TimeSpan GetTimeout(HsmsTimerKind kind) => kind switch
    {
        HsmsTimerKind.T3 => _options.T3, HsmsTimerKind.T5 => _options.T5, HsmsTimerKind.T6 => _options.T6,
        HsmsTimerKind.T7 => _options.T7, HsmsTimerKind.T8 => _options.T8, _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    /// <summary>\if KO 취소되거나 만료될 때까지 기다리고 만료 예외를 발생시킵니다. \endif \if EN Waits until cancellation or expiration and throws on expiration. \endif</summary>
    /// <param name="kind">\if KO 타이머입니다. \endif \if EN Timer. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task WaitForExpirationAsync(HsmsTimerKind kind, CancellationToken cancellationToken = default)
    {
        var timeout = GetTimeout(kind);
        await Task.Delay(timeout, _timeProvider, cancellationToken).ConfigureAwait(false);
        throw new HsmsTimerExpiredException(kind.ToString(), timeout);
    }
}
