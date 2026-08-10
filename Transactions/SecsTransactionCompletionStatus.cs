namespace Dreamine.Secs.Com.Transactions;

/// <summary>\if KO <para>Secondary 상관관계 처리 결과입니다.</para> \endif \if EN <para>Identifies the result of secondary-message correlation.</para> \endif</summary>
public enum SecsTransactionCompletionStatus
{
    /// <summary>\if KO 열린 transaction을 완료했습니다. \endif \if EN An open transaction was completed. \endif</summary>
    Completed,
    /// <summary>\if KO 최근 완료된 응답의 중복입니다. \endif \if EN The response duplicates a recently completed transaction. \endif</summary>
    Duplicate,
    /// <summary>\if KO System Bytes를 알 수 없습니다. \endif \if EN System Bytes are unknown. \endif</summary>
    UnknownSystemBytes,
    /// <summary>\if KO Session/Stream/Function 상관관계가 맞지 않습니다. \endif \if EN Session/Stream/Function correlation does not match. \endif</summary>
    InvalidCorrelation
}
