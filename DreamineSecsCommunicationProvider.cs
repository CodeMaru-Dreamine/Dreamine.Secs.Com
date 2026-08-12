using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.Secs.Abstractions.Providers;
using Dreamine.Secs.Com.Hsms;

namespace Dreamine.Secs.Com;

/// <summary>\if KO <para>Dreamine 자체 HSMS-SS 연결을 공급자 계약에 연결합니다.</para> \endif \if EN <para>Connects the native Dreamine HSMS-SS session to the provider contract.</para> \endif</summary>
public sealed class DreamineSecsCommunicationProvider : ISecsMessageSessionProvider
{
    private readonly Func<SecsConnectionOptions, HsmsSessionOptions> _optionsFactory;
    private readonly TimeProvider _timeProvider;
    private readonly ISecsDiagnosticSink? _diagnostics;

    /// <summary>\if KO 기본 localhost:5000 HSMS 설정으로 공급자를 만듭니다. \endif \if EN Creates a provider using default localhost:5000 HSMS settings. \endif</summary>
    public DreamineSecsCommunicationProvider() : this(
        static options => new HsmsSessionOptions { Mode = options.Mode, Role = options.Role }) { }

    /// <summary>\if KO 애플리케이션 설정 mapping, 시간 및 진단 경계로 공급자를 만듭니다. \endif \if EN Creates a provider with application option mapping, time, and diagnostics boundaries. \endif</summary>
    /// <param name="optionsFactory">\if KO HSMS 설정 mapping입니다. \endif \if EN HSMS settings mapper. \endif</param><param name="timeProvider">\if KO 시간 공급자입니다. \endif \if EN Time provider. \endif</param><param name="diagnostics">\if KO 진단 sink입니다. \endif \if EN Diagnostic sink. \endif</param>
    public DreamineSecsCommunicationProvider(Func<SecsConnectionOptions, HsmsSessionOptions> optionsFactory, TimeProvider? timeProvider = null, ISecsDiagnosticSink? diagnostics = null)
    {
        _optionsFactory = optionsFactory ?? throw new ArgumentNullException(nameof(optionsFactory));
        _timeProvider = timeProvider ?? TimeProvider.System; _diagnostics = diagnostics;
    }

    /// <inheritdoc />
    public string Key => SecsProviderKeys.Dreamine;

    /// <inheritdoc />
    public ISecsConnection CreateConnection(SecsConnectionOptions options) => CreateSession(options);

    /// <inheritdoc />
    public ISecsMessageSession CreateSession(SecsConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!string.Equals(options.ProviderKey, Key, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException($"Provider key '{options.ProviderKey}' does not match '{Key}'.", nameof(options));
        var hsmsOptions = _optionsFactory(options) ?? throw new InvalidOperationException("The HSMS options factory returned null.");
        hsmsOptions = new HsmsSessionOptions
        {
            Host = hsmsOptions.Host,
            Port = hsmsOptions.Port,
            Mode = options.Mode,
            Role = options.Role,
            SessionId = hsmsOptions.SessionId,
            Timers = hsmsOptions.Timers,
            MaximumFrameLength = hsmsOptions.MaximumFrameLength,
            MaximumMessageLength = hsmsOptions.MaximumMessageLength,
            MaximumNestingDepth = hsmsOptions.MaximumNestingDepth,
            MaximumListItemCount = hsmsOptions.MaximumListItemCount,
            AutoReconnect = hsmsOptions.AutoReconnect,
            WireObservation = hsmsOptions.WireObservation,
            PrimaryDispatcher = hsmsOptions.PrimaryDispatcher
        };
        return new HsmsSession(hsmsOptions, _timeProvider, _diagnostics);
    }
}
