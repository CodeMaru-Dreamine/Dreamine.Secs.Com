# 빠른 시작

## Package-first Loopback

독립 `Dreamine.Secs.Com` Clone에서 공개 패키지만 참조하는 샘플을 실행합니다.

```powershell
dotnet run --project samples/Dreamine.Secs.Com.PackageQuickStart
```

하나의 제한된 Process에서 Passive Equipment / Active Host Select, S1F1/F2, Linktest, Disconnect를 수행합니다. Full Workspace Demo Project는 필요하지 않습니다.

## Full Workspace 통합 샘플

먼저 Passive 장비를 실행하고 다른 터미널에서 Active 호스트를 실행합니다.

```powershell
dotnet run --project samples/Dreamine.Secs.Equipment.Passive
dotnet run --project samples/Dreamine.Secs.Host.Active
```

TCP/Select, S1F13/S1F14, S1F1/S1F2, 샘플 전용 W0 메시지, Linktest, 정상 Separate 및 1회 재접속을 확인합니다. 기본값은 `127.0.0.1:7000`, Session ID `7`, 제한된 연결 2회, 연결 사이 250 ms 지연, 전체 실행 timeout 120초입니다. 첫 두 인수로 Host와 Port를 바꿀 수 있습니다. 연결을 1회만 실행하려면 `--once`, 전체 옵션은 `--help`를 사용하십시오.

이 두 Project 명령은 Canonical Full Workspace용 Source Build입니다. 형제 Source-only `Dreamine.SecsGem.Interop.Runtime` Workbench Project를 의도적으로 `ProjectReference`합니다. 이 Project는 공개 패키지가 아니며 `Dreamine.Secs.Com` 소비 Application에 필요하지 않습니다. 독립 Clone에서는 위 Package-first Loopback을 사용하십시오.

## Versioned Profile, Template, Scenario 및 지속형 로그

저장소의 공개 fixture는 loopback port `17117`과 0이 아닌 Session ID `37`을 사용합니다. PowerShell 터미널에서 Equipment를 먼저 실행한 다음 다른 터미널에서 Host를 실행합니다. Source control 밖의 서로 다른 product-owned 로그 디렉터리를 사용하십시오.

```powershell
dotnet run --project samples/Dreamine.Secs.Equipment.Passive -- `
  --profile samples/fixtures/equipment-profile.json `
  --template samples/fixtures/message-catalog.json `
  --template-name PublicSampleW0 `
  --log-directory "$env:TEMP\dreamine-secs-equipment" `
  --once

dotnet run --project samples/Dreamine.Secs.Host.Active -- `
  --profile samples/fixtures/host-profile.json `
  --template samples/fixtures/message-catalog.json `
  --template-name PublicSampleW0 `
  --scenario samples/fixtures/host-scenario.json `
  --log-directory "$env:TEMP\dreamine-secs-host" `
  --once
```

`--profile`은 제한된 version-1 연결 Profile을 불러옵니다. `--template`과 `--template-name`은 검증된 Primary Template 하나를 선택합니다. `--scenario`는 공유 bounded Scenario Runner를 실행합니다. `--log-directory`는 Runtime package를 통해 rolling/retention이 적용된 Header-Only JSONL을 활성화합니다. 네트워크 연결 없이 입력 문서만 검증하려면 `--validate-only`를 사용하십시오. Fixture 메시지는 샘플 전용이며 Normative 메시지 정의나 고객 시나리오가 아닙니다.

Endpoint마다 고유 Active/Passive pair를 사용하고 session은 반드시 비동기 해제합니다. Provider-neutral safe API를 우선 사용하십시오. `SendAsync(stream, function, item)`은 W0 System Bytes를 자동 할당하고, `RequestAsync(dialogue, item)`은 System Bytes를 자동 할당한 뒤 선언된 정상 W1 Secondary를 상관시킵니다.

```csharp
var provider = new DreamineSecsCommunicationProvider(options => new HsmsSessionOptions
{
    Host = "127.0.0.1",
    Port = 7000,
    Mode = options.Mode,
    Role = options.Role,
    SessionId = new SecsSessionId(7)
});
await using ISecsMessageSession session = provider.CreateSession(new SecsConnectionOptions
{
    ProviderKey = SecsProviderKeys.Dreamine,
    Mode = SecsConnectionMode.Active,
    Role = SecsRole.Host
});

await session.ConnectAsync(cancellationToken);
await session.SelectAsync(cancellationToken);

await session.SendAsync(new SecsStream(1), new SecsFunction(1), cancellationToken: cancellationToken);

var s1f1 = new SecsDialogueDefinition(
    new SecsStream(1),
    new SecsFunction(1),
    new SecsFunction(2));
var s1f2 = await session.RequestAsync(s1f1, cancellationToken: cancellationToken);
```

비동기 inbound 처리는 bounded dispatcher에 등록합니다. exact S/F, fallback, legacy event 순서로 우선하며 W-bit 불일치도 claim하지만 `CanReply`는 false입니다. 추측 응답을 만들지 마십시오.

```csharp
using var registration = session.PrimaryDispatcher.Register(
    new SecsDialogueDefinition(new SecsStream(1), new SecsFunction(13), new SecsFunction(14)),
    async (context, cancellationToken) =>
    {
        if (context.CanReply)
            await context.ReplyAsync(item: null, cancellationToken: cancellationToken);
    });
```

Bounded FIFO가 가득 찼거나 종료 중이면 이미 claim한 newest Primary를 drop하므로 `DroppedPrimaryCount`를 확인하십시오. 첫 응답 시도는 성공, 실패, 취소와 관계없이 ownership을 소비합니다. `AllocateSystemBytes`, `SendAsync(SecsMessage, ...)`, `SendPrimaryAsync`는 expert API로만 사용하십시오. 저수준 transaction 경로는 Function 0 또는 `Primary Function + 1`을 받고 `SecsDialogueDefinition`은 정상 인접 Secondary dialogue만 선언합니다.

Session 설정은 생성 시 snapshot입니다. Endpoint, mode, Session ID, role, timer, limit, `AutoReconnect`, dispatcher 또는 wire 설정을 바꾸려면 새 profile을 검증하고 새 session을 생성합니다. 호출별 cancellation은 해당 작업에만 적용합니다. 일반 Primary retry는 없고 Active 재연결은 T5로 분리하며 Passive는 연결 종료 후 즉시 listen을 재개합니다.

## 선택적 wire observation

Raw 증거가 필요할 때만 bounded wire observation을 활성화합니다.

```csharp
var options = new HsmsSessionOptions
{
    Host = "127.0.0.1",
    Port = 7000,
    Mode = SecsConnectionMode.Passive,
    Role = SecsRole.Equipment,
    SessionId = new SecsSessionId(7),
    WireObservation = new HsmsWireObservationOptions
    {
        QueueCapacity = 256,
        MaximumCapturedBytes = 64 * 1024
    }
};

await using var session = new HsmsSession(options);
var observationReader = ConsumeObservationsAsync(session, CancellationToken.None);
```

`observationReader`를 보관하고 Protocol 작업과 동시에 정확히 하나의 reader를 실행한 뒤 Session을 Dispose한 후 await합니다.

```csharp
static async Task ConsumeObservationsAsync(
    IHsmsWireObservationSource source,
    CancellationToken cancellationToken)
{
    await foreach (var observation in source.ReadWireObservationsAsync(cancellationToken))
    {
        Console.WriteLine(
            $"#{observation.SequenceNumber} {observation.Direction} " +
            $"epoch={observation.ConnectionEpoch} " +
            $"actual={observation.ActualByteCount} captured={observation.CapturedBytes.Length}");
    }
}
```

Bounded queue가 가득 차도 protocol은 기다리지 않습니다. `DroppedWireObservationCount`와 sequence 번호의 공백을 확인하십시오. `CapturedBytes`는 잘릴 수 있고 민감한 application payload를 포함할 수 있습니다. 증거 자료로 보호하고 부분 Frame, 진단 문자열 또는 canonical 재인코딩을 capture된 wire byte로 취급하지 마십시오. 지속형 파일에는 `--log-directory`가 사용하는 Runtime package의 Header-Only facade를 우선 사용하십시오. 이 facade는 endpoint를 redaction하고 동적 진단 문자열을 저장하지 않으며 drop/failure/flush health를 노출합니다. Session을 먼저 Stop 또는 Dispose한 다음 log recorder를 종료하고, 파일을 evidence로 취급하기 전에 `Health.IsEvidenceEligible`을 확인하십시오.

이 샘플은 로컬 Dreamine 상호 간 경로만 검증합니다. 외부 Simulator 및 현장 검증은 `NOT_RUN`이고 E37.1 적합성 주장은 `BLOCKED_STANDARD`입니다. [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)를 함께 확인하십시오.
