using System.Diagnostics;
using Dreamine.Secs.Abstractions.Diagnostics;

namespace Dreamine.Secs.Com.Diagnostics;

internal sealed class SafeSecsDiagnosticSink : ISecsDiagnosticSink
{
    private readonly ISecsDiagnosticSink _inner;

    private SafeSecsDiagnosticSink(ISecsDiagnosticSink inner) => _inner = inner;

    public static ISecsDiagnosticSink Wrap(ISecsDiagnosticSink? sink)
    {
        var resolved = sink ?? NullSecsDiagnosticSink.Instance;
        return resolved is SafeSecsDiagnosticSink or NullSecsDiagnosticSink ? resolved : new SafeSecsDiagnosticSink(resolved);
    }

    public void Emit(SecsDiagnosticEvent diagnosticEvent)
    {
        try
        {
            _inner.Emit(diagnosticEvent);
        }
        catch (Exception exception)
        {
            Trace.TraceError("SECS diagnostic sink failed: {0}", exception);
        }
    }
}
