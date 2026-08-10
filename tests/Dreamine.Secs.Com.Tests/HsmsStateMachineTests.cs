using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class HsmsStateMachineTests
{
    [Fact]
    public void TcpConnectSelectDeselectAndDisconnectFollowExplicitStates()
    {
        var machine = new HsmsStateMachine();
        Assert.Equal(HsmsConnectionState.NotConnected, machine.State);
        machine.OnTcpConnected();
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, machine.State);
        _ = machine.CreateSelectRequest(new SecsSystemBytes(1));
        var select = machine.Process(Control(HsmsSType.SelectResponse, 1, (byte)HsmsSelectStatus.Success));
        Assert.True(select.Accepted);
        Assert.Equal(HsmsConnectionState.Selected, machine.State);
        _ = machine.CreateDeselectRequest(new SecsSystemBytes(2));
        machine.Process(Control(HsmsSType.DeselectResponse, 2, (byte)HsmsDeselectStatus.Success));
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, machine.State);
        machine.OnTcpDisconnected();
        Assert.Equal(HsmsConnectionState.NotConnected, machine.State);
    }

    [Fact]
    public void SimultaneousSelectAcceptsPeerRequestAndLaterResponse()
    {
        var left = ConnectedMachine();
        var right = ConnectedMachine();
        var leftRequest = left.CreateSelectRequest(new SecsSystemBytes(1));
        var rightRequest = right.CreateSelectRequest(new SecsSystemBytes(2));
        var leftResult = left.Process(rightRequest);
        var rightResult = right.Process(leftRequest);
        Assert.Equal(HsmsSType.SelectResponse, Assert.IsType<HsmsControlMessage>(leftResult.Response).SType);
        Assert.Equal(HsmsSType.SelectResponse, Assert.IsType<HsmsControlMessage>(rightResult.Response).SType);
        left.Process(rightResult.Response!);
        right.Process(leftResult.Response!);
        Assert.Equal(HsmsConnectionState.Selected, left.State);
        Assert.Equal(HsmsConnectionState.Selected, right.State);
    }

    [Fact]
    public void SelectRequestWhileSelectedReturnsAlreadyActive()
    {
        var machine = SelectedMachine();
        var result = machine.Process(Control(HsmsSType.SelectRequest, 5));
        Assert.Equal((byte)HsmsSelectStatus.AlreadyActive, Assert.IsType<HsmsControlMessage>(result.Response).Header.HeaderByte3);
        Assert.Equal(HsmsConnectionState.Selected, machine.State);
    }

    [Fact]
    public void DataWhileNotSelectedProducesReject()
    {
        var machine = ConnectedMachine();
        var result = machine.Process(Data());
        var reject = Assert.IsType<HsmsControlMessage>(result.Response);
        Assert.False(result.Accepted);
        Assert.Equal(HsmsSType.RejectRequest, reject.SType);
        Assert.Equal((byte)HsmsRejectReason.NotSelected, reject.Header.HeaderByte3);
    }

    [Fact]
    public void DataWhileSelectedIsAccepted()
    {
        Assert.True(SelectedMachine().Process(Data()).Accepted);
    }

    [Fact]
    public void LinktestIsValidInBothConnectedStates()
    {
        foreach (var machine in new[] { ConnectedMachine(), SelectedMachine() })
        {
            _ = machine.CreateLinktestRequest(new SecsSystemBytes(8));
            var result = machine.Process(Control(HsmsSType.LinktestRequest, 8));
            Assert.Equal(HsmsSType.LinktestResponse, Assert.IsType<HsmsControlMessage>(result.Response).SType);
        }
    }

    [Fact]
    public void SeparateRequestsConnectionCloseAndInitiatorLeavesSelected()
    {
        var receiver = SelectedMachine();
        var result = receiver.Process(Control(HsmsSType.SeparateRequest, 4));
        Assert.True(result.CloseConnection);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, receiver.State);

        var sender = SelectedMachine();
        Assert.Equal(HsmsSType.SeparateRequest, sender.CreateSeparateRequest(new SecsSystemBytes(4)).SType);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, sender.State);
    }

    [Fact]
    public void SeparateRequestIsIgnoredWhileNotSelected()
    {
        var machine = ConnectedMachine();
        var result = machine.Process(Control(HsmsSType.SeparateRequest, 5));
        Assert.False(result.Accepted);
        Assert.False(result.CloseConnection);
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, machine.State);
    }

    [Fact]
    public void InvalidInitiatingStatesThrowStructuredException()
    {
        var machine = new HsmsStateMachine();
        Assert.Equal(HsmsConnectionState.NotConnected, Assert.Throws<HsmsStateException>(() => machine.CreateLinktestRequest(new SecsSystemBytes(1))).State);
        machine.OnTcpConnected();
        Assert.Throws<HsmsStateException>(() => machine.CreateDeselectRequest(new SecsSystemBytes(1)));
    }

    [Fact]
    public void DiagnosticsContainStateAndRejectWithoutPayload()
    {
        var sink = new RecordingSink();
        var machine = new HsmsStateMachine(sink);
        machine.OnTcpConnected();
        machine.Process(Data());
        Assert.Contains(sink.Events, item => item.Kind == SecsDiagnosticKind.StateChanged);
        Assert.Contains(sink.Events, item => item.Kind == SecsDiagnosticKind.Reject && item.Message.Length < 256);
    }

    [Fact]
    public void DiagnosticSinkFailureDoesNotChangeProtocolBehavior()
    {
        var machine = new HsmsStateMachine(new ThrowingSink());
        machine.OnTcpConnected();
        Assert.Equal(HsmsConnectionState.ConnectedNotSelected, machine.State);
    }

    private static HsmsStateMachine ConnectedMachine() { var machine = new HsmsStateMachine(); machine.OnTcpConnected(); return machine; }
    private static HsmsStateMachine SelectedMachine() { var machine = ConnectedMachine(); machine.Process(Control(HsmsSType.SelectRequest, 1)); return machine; }
    private static HsmsControlMessage Control(HsmsSType type, uint systemBytes, byte status = 0) => new(HsmsHeader.CreateControl(type, new SecsSystemBytes(systemBytes), headerByte3: status));
    private static HsmsDataMessage Data() => new(new SecsMessage(new SecsSessionId(1), new SecsStream(1), new SecsFunction(1), false, new SecsSystemBytes(3)));

    private sealed class RecordingSink : ISecsDiagnosticSink
    {
        public List<SecsDiagnosticEvent> Events { get; } = new();
        public void Emit(SecsDiagnosticEvent diagnosticEvent) => Events.Add(diagnosticEvent);
    }

    private sealed class ThrowingSink : ISecsDiagnosticSink
    {
        public void Emit(SecsDiagnosticEvent diagnosticEvent) => throw new InvalidOperationException("sink failure");
    }
}
