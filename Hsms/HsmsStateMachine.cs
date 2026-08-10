using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Diagnostics;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>E37-0413 §5와 §7의 단일 세션 상태 전이를 직렬화합니다.</para> \endif \if EN <para>Serializes the single-session state transitions from E37-0413 §§5 and 7.</para> \endif</summary>
public sealed class HsmsStateMachine
{
    private readonly object _gate = new();
    private readonly ISecsDiagnosticSink _diagnostics;
    private HsmsConnectionState _state;

    /// <summary>\if KO 선택적 진단 sink로 상태 머신을 만듭니다. \endif \if EN Creates a state machine with an optional diagnostic sink. \endif</summary>
    /// <param name="diagnostics">\if KO 진단 sink입니다. \endif \if EN Diagnostic sink. \endif</param>
    public HsmsStateMachine(ISecsDiagnosticSink? diagnostics = null) => _diagnostics = SafeSecsDiagnosticSink.Wrap(diagnostics);

    /// <summary>\if KO 현재 명시적 상태입니다. \endif \if EN Gets the current explicit state. \endif</summary>
    public HsmsConnectionState State { get { lock (_gate) return _state; } }

    /// <summary>\if KO TCP 연결 설정을 반영합니다. \endif \if EN Applies TCP establishment. \endif</summary>
    public void OnTcpConnected()
    {
        lock (_gate)
        {
            Ensure(HsmsConnectionState.NotConnected, nameof(OnTcpConnected));
            Transition(HsmsConnectionState.ConnectedNotSelected);
        }
    }

    /// <summary>\if KO TCP 연결 종료를 반영합니다. \endif \if EN Applies TCP termination. \endif</summary>
    public void OnTcpDisconnected()
    {
        lock (_gate) Transition(HsmsConnectionState.NotConnected);
    }

    /// <summary>\if KO 현재 상태에서 Select.req를 만듭니다. \endif \if EN Creates Select.req in the current state. \endif</summary>
    /// <param name="systemBytes">\if KO 시스템 바이트입니다. \endif \if EN System bytes. \endif</param><returns>\if KO 제어 메시지입니다. \endif \if EN Control message. \endif</returns>
    public HsmsControlMessage CreateSelectRequest(SecsSystemBytes systemBytes)
    {
        lock (_gate) { Ensure(HsmsConnectionState.ConnectedNotSelected, nameof(CreateSelectRequest)); return Control(HsmsSType.SelectRequest, systemBytes); }
    }

    /// <summary>\if KO 현재 상태에서 Deselect.req를 만듭니다. \endif \if EN Creates Deselect.req in the current state. \endif</summary>
    /// <param name="systemBytes">\if KO 시스템 바이트입니다. \endif \if EN System bytes. \endif</param><returns>\if KO 제어 메시지입니다. \endif \if EN Control message. \endif</returns>
    public HsmsControlMessage CreateDeselectRequest(SecsSystemBytes systemBytes)
    {
        lock (_gate) { Ensure(HsmsConnectionState.Selected, nameof(CreateDeselectRequest)); return Control(HsmsSType.DeselectRequest, systemBytes); }
    }

    /// <summary>\if KO 연결된 상태에서 Linktest.req를 만듭니다. \endif \if EN Creates Linktest.req while connected. \endif</summary>
    /// <param name="systemBytes">\if KO 시스템 바이트입니다. \endif \if EN System bytes. \endif</param><returns>\if KO 제어 메시지입니다. \endif \if EN Control message. \endif</returns>
    public HsmsControlMessage CreateLinktestRequest(SecsSystemBytes systemBytes)
    {
        lock (_gate) { EnsureConnected(nameof(CreateLinktestRequest)); return Control(HsmsSType.LinktestRequest, systemBytes); }
    }

    /// <summary>\if KO 선택 상태에서 Separate.req를 만들고 미선택 상태로 이동합니다. \endif \if EN Creates Separate.req while selected and moves to not selected. \endif</summary>
    /// <param name="systemBytes">\if KO 시스템 바이트입니다. \endif \if EN System bytes. \endif</param><returns>\if KO 제어 메시지입니다. \endif \if EN Control message. \endif</returns>
    public HsmsControlMessage CreateSeparateRequest(SecsSystemBytes systemBytes)
    {
        lock (_gate)
        {
            Ensure(HsmsConnectionState.Selected, nameof(CreateSeparateRequest));
            Transition(HsmsConnectionState.ConnectedNotSelected);
            return Control(HsmsSType.SeparateRequest, systemBytes);
        }
    }

    /// <summary>\if KO 수신 메시지를 현재 상태에 적용합니다. \endif \if EN Applies an inbound message to the current state. \endif</summary>
    /// <param name="message">\if KO 수신 메시지입니다. \endif \if EN Inbound message. \endif</param><returns>\if KO 처리 결과입니다. \endif \if EN Processing result. \endif</returns>
    public HsmsProcessingResult Process(HsmsMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_gate)
        {
            EnsureConnected(nameof(Process));
            if (message is HsmsDataMessage)
            {
                if (_state == HsmsConnectionState.Selected) return new(true);
                var reject = Reject(message.Header, HsmsRejectReason.NotSelected);
                _diagnostics.Emit(new(SecsDiagnosticKind.Reject, "Data received while not selected.", _state));
                return new(false, reject);
            }
            return ProcessControl((HsmsControlMessage)message);
        }
    }

    private HsmsProcessingResult ProcessControl(HsmsControlMessage message)
    {
        switch (message.SType)
        {
            case HsmsSType.SelectRequest:
                if (_state == HsmsConnectionState.ConnectedNotSelected)
                {
                    Transition(HsmsConnectionState.Selected);
                    return new(false, Control(HsmsSType.SelectResponse, message.Header.SystemBytes, (byte)HsmsSelectStatus.Success));
                }
                return new(false, Control(HsmsSType.SelectResponse, message.Header.SystemBytes, (byte)HsmsSelectStatus.AlreadyActive));
            case HsmsSType.SelectResponse:
                if (message.Header.HeaderByte3 is (byte)HsmsSelectStatus.Success or (byte)HsmsSelectStatus.AlreadyActive)
                {
                    if (_state == HsmsConnectionState.ConnectedNotSelected) Transition(HsmsConnectionState.Selected);
                    return new(true);
                }
                return new(false);
            case HsmsSType.DeselectRequest:
                if (_state == HsmsConnectionState.Selected)
                {
                    Transition(HsmsConnectionState.ConnectedNotSelected);
                    return new(false, Control(HsmsSType.DeselectResponse, message.Header.SystemBytes, (byte)HsmsDeselectStatus.Success));
                }
                return new(false, Control(HsmsSType.DeselectResponse, message.Header.SystemBytes, (byte)HsmsDeselectStatus.NotEstablished));
            case HsmsSType.DeselectResponse:
                if (message.Header.HeaderByte3 == (byte)HsmsDeselectStatus.Success && _state == HsmsConnectionState.Selected)
                    Transition(HsmsConnectionState.ConnectedNotSelected);
                return new(true);
            case HsmsSType.LinktestRequest:
                return new(false, Control(HsmsSType.LinktestResponse, message.Header.SystemBytes));
            case HsmsSType.LinktestResponse:
            case HsmsSType.RejectRequest:
                return new(true);
            case HsmsSType.SeparateRequest:
                if (_state != HsmsConnectionState.Selected) return new(false);
                Transition(HsmsConnectionState.ConnectedNotSelected);
                return new(false, closeConnection: true);
            default:
                return new(false, Reject(message.Header, HsmsRejectReason.UnsupportedSType));
        }
    }

    private static HsmsControlMessage Control(HsmsSType type, SecsSystemBytes systemBytes, byte status = 0) => new(HsmsHeader.CreateControl(type, systemBytes, headerByte3: status));
    private static HsmsControlMessage Reject(HsmsHeader rejected, HsmsRejectReason reason)
    {
        var rejectedType = reason == HsmsRejectReason.UnsupportedPType ? rejected.PType : rejected.SType;
        return new(HsmsHeader.CreateControl(HsmsSType.RejectRequest, rejected.SystemBytes, rejectedType, (byte)reason, rejected.SessionId));
    }

    private void Ensure(HsmsConnectionState expected, string operation) { if (_state != expected) throw new HsmsStateException(_state, operation); }
    private void EnsureConnected(string operation) { if (_state == HsmsConnectionState.NotConnected) throw new HsmsStateException(_state, operation); }
    private void Transition(HsmsConnectionState next)
    {
        if (_state == next) return;
        var previous = _state; _state = next;
        _diagnostics.Emit(new(SecsDiagnosticKind.StateChanged, $"HSMS state changed from {previous} to {next}.", next));
    }
}
