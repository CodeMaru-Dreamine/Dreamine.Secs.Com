using Dreamine.Secs.Abstractions.Diagnostics;

namespace Dreamine.Secs.Com.Diagnostics;

/// <summary>
/// \if KO <para>기존 진단 sink와 provider-neutral typed 진단 이벤트를 하나의 안전한 경계로 결합합니다.</para> \endif
/// \if EN <para>Combines the legacy diagnostic sink and provider-neutral typed diagnostic event behind one safe boundary.</para> \endif
/// </summary>
internal sealed class SecsSessionDiagnosticHub(object owner, ISecsDiagnosticSink? downstream) : ISecsDiagnosticSink
{
    private readonly object _owner = owner ?? throw new ArgumentNullException(nameof(owner));
    private readonly ISecsDiagnosticSink? _downstream = downstream;

    /// <summary>\if KO typed 진단 구독입니다. \endif \if EN Typed diagnostic subscription. \endif</summary>
    public event EventHandler<SecsDiagnosticEvent>? DiagnosticReceived;

    /// <summary>\if KO 각 외부 callback 실패를 격리하여 진단을 전달합니다. \endif \if EN Delivers a diagnostic while isolating every external callback failure. \endif</summary>
    public void Emit(SecsDiagnosticEvent diagnosticEvent)
    {
        ArgumentNullException.ThrowIfNull(diagnosticEvent);
        try { _downstream?.Emit(diagnosticEvent); }
        catch (Exception exception) { SafeTrace.Error("SECS diagnostic sink failed: {0}", exception); }

        var handlers = DiagnosticReceived;
        if (handlers is null) return;
        foreach (EventHandler<SecsDiagnosticEvent> handler in handlers.GetInvocationList())
        {
            try { handler(_owner, diagnosticEvent); }
            catch (Exception exception) { SafeTrace.Error("SECS diagnostic event handler failed: {0}", exception); }
        }
    }
}
