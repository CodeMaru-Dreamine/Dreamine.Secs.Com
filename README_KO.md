# Dreamine.Secs.Com

Dreamine.Secs.Com은 Dreamine 자체 SECS-II / HSMS 1차 통신 구현입니다.

[➡️ English Version](https://github.com/CodeMaru-Dreamine/Dreamine.Secs.Com/blob/main/README.md)

## 구현 범위

- 정확한 Big Endian 숫자 폭, 중첩 List, 빈 값/다중 값, 완전 소비, Message/깊이/List 개수 제한을 지원하는 네트워크 독립 SECS-II item codec
- 4바이트 길이 접두사와 10바이트 Header, 부분/연속 frame 및 T8 stream 읽기를 지원하는 HSMS 전용 codec
- Select, Deselect, Linktest, Reject, Separate, 동시 Select와 잘못된 상태를 처리하는 명시적 `NotConnected` / `ConnectedNotSelected` / `Selected` 상태 머신
- 세션별 thread-safe System Bytes와 T3, 취소, 중복 검출, 연결 정리를 포함한 동시 Primary/Secondary 상관관계
- `TimeProvider`를 주입하는 T5 재연결, T6 Control Transaction, T7 미선택 종료, T8 바이트 간 통신 실패
- 외부 장비가 필요 없는 단일-session Active/Passive TCP runtime, 공급자 합성, 제한된 진단, loopback 테스트
- Session까지 전달되는 Frame/Message/깊이/List 제한, Control Header 검증, 직렬 송신, application callback 예외 격리, 개별 transaction 실패 정리
- 실제로 읽거나 기록한 완전한 송수신 wire frame의 opt-in bounded observation, 연결 epoch, drop 계수 및 capture truncation
- 완전한 Header를 확보한 경우 Decode 실패와 진단에 선택적으로 제공되는 typed HSMS Header 문맥
- System Bytes 자동 할당 W0/W1 송신, typed identity/state event 및 정상 dialogue 상관관계를 제공하는 추가 provider-neutral `ISecsMessageSession` / `ISecsMessageSessionProvider` 지원
- exact-before-fallback ownership, 고정 worker, 명시적 drop 계수 및 source/epoch에 결합된 one-shot 응답을 제공하는 bounded 비동기 inbound Primary dispatcher

적용 Revision/조항과 보류 범위는 [`docs/SEMI_REQUIREMENTS_TRACE.md`](https://github.com/CodeMaru-Dreamine/Dreamine.Secs.Com/blob/main/docs/SEMI_REQUIREMENTS_TRACE.md)에 기록했습니다.

## 공개 샘플과 재사용 Runtime

Active Host와 Passive Equipment 샘플은 위 provider-neutral API와 재사용 가능한 `Dreamine.SecsGem.Interop.Runtime` 계층을 사용합니다. 기본값은 loopback port `7000`, 0이 아닌 Session ID `7`, 제한된 연결 2회, 전체 실행 timeout 120초입니다. `--once`를 지정하면 연결을 1회만 실행합니다.

샘플은 `--profile`, `--template`과 `--template-name`, `--scenario`, `--log-directory`도 받습니다. 저장소의 version-1 fixture는 0이 아닌 Session ID, 샘플 전용 W0 Template, 제한 시간 안의 Connect/Select/Linktest 실행, 지속형 Header-Only JSONL 로그를 보여 줍니다. [QUICKSTART_KO.md](QUICKSTART_KO.md)와 [fixture 안내](samples/fixtures/README.md)를 참고하십시오.

Canonical full workspace 안에서는 sample project가 `Dreamine.SecsGem.Interop.Runtime` source를 의도적으로 `ProjectReference`합니다. 게시된 `Dreamine.Secs.Com` package에는 HSMS runtime이 포함되며 해당 Demo/Workbench project는 포함되지 않습니다. 과거 로컬 `1.0.0` 산출물에서 게시 package 세트로 전환할 때는 깨끗한 package cache를 사용하십시오.

## Transport 결정

범용 Communication 길이 codec은 접두사 형태는 같지만 HSMS Header 검증, 증분 다중 frame, T8을 제공하지 않습니다. sealed 범용 TCP 서버도 선택된 단일 세션과 응답 대상 지정에 필요한 연결별 원시 Stream을 노출하지 않습니다. 따라서 기존 연결 생명주기는 구현하되 Communication public API는 변경하지 않고 SECS 경계 내부에 작은 HSMS 전용 TCP channel을 두었습니다.

## Wire 증거 경계

`HsmsSessionOptions.WireObservation`을 설정하지 않으면 wire observation은 비활성입니다. 활성화하면 `HsmsSession`은 `IHsmsWireObservationSource`를 통해 하나의 pull reader를 노출하며 protocol 처리는 reader를 기다리지 않습니다. Outbound observation은 Stream에 기록한 encoded byte를, inbound observation은 decode 전에 읽은 완전한 byte를 사용하므로 canonical 재인코딩을 capture된 wire data로 표시하지 않습니다. `CapturedBytes`는 잘릴 수 있고 sequence 번호의 공백은 queue drop을 뜻하며 부분 Frame은 observation이 아닙니다. Raw Frame에는 민감한 application payload가 포함될 수 있으므로 application data policy에 맞춰 보호·보존·내보내야 합니다.

진단과 로컬 재인코딩 메시지는 문제 분석에 유용하지만 wire observation 또는 외부 counterpart 증거를 대신하지 않습니다.

## Session 및 dispatcher 경계

기존 `ISecsConnection`, `ISecsCommunicationProvider`, `CreateConnection` 계약은 변경하지 않았습니다. Typed message session/provider 계약과 `CreateSession`은 추가 API입니다. exact S/F 등록, fallback, legacy `MessageReceived` event 순서로 claim합니다. exact W-bit 불일치도 claim하지만 응답할 수 없습니다. Bounded FIFO가 가득 찼거나 종료 중이면 이미 claim한 newest Primary를 drop하고 계수하며 다른 경로로 넘기지 않습니다. 첫 응답 시도는 성공, 실패, 취소와 관계없이 ownership을 소비하고, 허용된 응답은 source session, Session ID, Stream, System Bytes, 선언된 정상 Secondary 및 connection epoch에 결합됩니다.

Provider는 session 생성 시 endpoint, mode, Session ID, role, timer, limit, `AutoReconnect`, dispatcher 및 wire observation 설정을 복제하고 검증합니다. 이 설정은 live mutable하지 않으므로 변경 시 새 session을 생성합니다. Profile 계층은 validate → diff → recreate 순서로 변경을 적용합니다. 호출별 cancellation은 현재 작업에만 적용합니다. Role은 상위 application/responder policy이며 HSMS 상태 머신이나 Active/Passive TCP 방향을 바꾸지 않습니다.

Primary에는 일반 재시도 정책이 없습니다. Active 재연결 시도는 T5로 분리하고 Passive 연결이 닫히면 추가 backoff 없이 즉시 listen을 재개합니다.

## 의존성 경계

    Dreamine.Secs.Com
        -> Dreamine.Secs.Abstractions
        -> Dreamine.Communication.Abstractions
        -> Dreamine.Communication.Core
        -> Dreamine.Communication.Sockets

로컬 E37.1 규범 문서를 확보하지 못했으므로 E37.1 적합성 주장은 `BLOCKED_STANDARD`입니다. 외부 Simulator 및 현장 검증은 `NOT_RUN`입니다. Legacy SECS-I, SML, 인증 및 최신 Revision 적합성 주장은 이 package 경계에서 `INTENTIONALLY_EXCLUDED`입니다. GEM, GEM300, WPF Monitoring 및 지속형 Workbench 기능은 별도 package 범위이며 Core의 evidence 상태를 바꾸지 않습니다.

## 라이선스

MIT.
