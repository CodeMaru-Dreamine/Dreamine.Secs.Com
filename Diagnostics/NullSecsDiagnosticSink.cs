using Dreamine.Secs.Abstractions.Diagnostics;

namespace Dreamine.Secs.Com.Diagnostics;

/// <summary>\if KO <para>진단을 폐기하는 기본 sink입니다.</para> \endif \if EN <para>Default sink that discards diagnostics.</para> \endif</summary>
public sealed class NullSecsDiagnosticSink : ISecsDiagnosticSink
{
    /// <summary>\if KO 공유 인스턴스입니다. \endif \if EN Gets the shared instance. \endif</summary>
    public static NullSecsDiagnosticSink Instance { get; } = new();
    private NullSecsDiagnosticSink() { }
    /// <inheritdoc />
    public void Emit(SecsDiagnosticEvent diagnosticEvent) => ArgumentNullException.ThrowIfNull(diagnosticEvent);
}
