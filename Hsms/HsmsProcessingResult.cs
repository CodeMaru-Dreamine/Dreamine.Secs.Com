using Dreamine.Secs.Abstractions.Hsms;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>수신 HSMS 메시지의 상태 머신 처리 결과입니다.</para> \endif \if EN <para>Represents the state-machine result of an inbound HSMS message.</para> \endif</summary>
public sealed class HsmsProcessingResult
{
    /// <summary>\if KO 처리 결과를 만듭니다. \endif \if EN Creates a processing result. \endif</summary>
    /// <param name="accepted">\if KO 상위 계층 전달 여부입니다. \endif \if EN Whether the message is accepted for upper layers. \endif</param><param name="response">\if KO 선택적 자동 응답입니다. \endif \if EN Optional automatic response. \endif</param><param name="closeConnection">\if KO 연결 종료 여부입니다. \endif \if EN Whether to close the connection. \endif</param>
    public HsmsProcessingResult(bool accepted, HsmsControlMessage? response = null, bool closeConnection = false)
    {
        Accepted = accepted; Response = response; CloseConnection = closeConnection;
    }
    /// <summary>\if KO 상위 계층 전달 여부입니다. \endif \if EN Gets whether the message is accepted. \endif</summary>
    public bool Accepted { get; }
    /// <summary>\if KO 자동 응답입니다. \endif \if EN Gets the automatic response. \endif</summary>
    public HsmsControlMessage? Response { get; }
    /// <summary>\if KO 처리 후 TCP 종료 여부입니다. \endif \if EN Gets whether TCP should be closed after processing. \endif</summary>
    public bool CloseConnection { get; }
}
