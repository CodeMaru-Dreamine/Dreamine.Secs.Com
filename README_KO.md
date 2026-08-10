# Dreamine.Secs.Com

Dreamine.Secs.Com은 Dreamine 자체 SECS-II / HSMS 1차 통신 구현입니다.

[➡️ English Version](https://github.com/CodeMaru-Dreamine/Dreamine.Secs.Com/blob/main/README.md)

## 구현 범위

- 정확한 Big Endian 숫자 폭, 중첩 List, 빈 값/다중 값, 완전 소비, 크기/깊이 제한을 지원하는 네트워크 독립 SECS-II item codec
- 4바이트 길이 접두사와 10바이트 Header, 부분/연속 frame 및 T8 stream 읽기를 지원하는 HSMS 전용 codec
- Select, Deselect, Linktest, Reject, Separate, 동시 Select와 잘못된 상태를 처리하는 명시적 `NotConnected` / `ConnectedNotSelected` / `Selected` 상태 머신
- 세션별 thread-safe System Bytes와 T3, 취소, 중복 검출, 연결 정리를 포함한 동시 Primary/Secondary 상관관계
- `TimeProvider`를 주입하는 T5 재연결, T6 Control Transaction, T7 미선택 종료, T8 바이트 간 통신 실패
- 외부 장비가 필요 없는 단일-session Active/Passive TCP runtime, 공급자 합성, 제한된 진단, loopback 테스트
- 방어적 List 수/크기/깊이 제한, Control Header 검증, 직렬 송신, application callback 예외 격리, 개별 transaction 실패 정리

적용 Revision/조항과 보류 범위는 [`docs/SEMI_REQUIREMENTS_TRACE.md`](./docs/SEMI_REQUIREMENTS_TRACE.md)에 기록했습니다.

## Transport 결정

범용 Communication 길이 codec은 접두사 형태는 같지만 HSMS Header 검증, 증분 다중 frame, T8을 제공하지 않습니다. sealed 범용 TCP 서버도 선택된 단일 세션과 응답 대상 지정에 필요한 연결별 원시 Stream을 노출하지 않습니다. 따라서 기존 연결 생명주기는 구현하되 Communication public API는 변경하지 않고 SECS 경계 내부에 작은 HSMS 전용 TCP channel을 두었습니다.

## 의존성 경계

    Dreamine.Secs.Com
        -> Dreamine.Secs.Abstractions
        -> Dreamine.Communication.Abstractions
        -> Dreamine.Communication.Core
        -> Dreamine.Communication.Sockets

로컬 E37.1 규범 문서를 확보하지 못했으므로 단일-session runtime의 E37.1 관련 범위는 provisional입니다. SECS-I, SML, GEM, GEM300, 벤더 SDK 공급자, WPF 모니터, Telemetry, 외부 상호운용, 인증 및 적합성 주장은 포함하지 않습니다.

## 라이선스

MIT.
