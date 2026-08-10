# 빠른 시작

먼저 Passive 장비를 실행하고 다른 터미널에서 Active 호스트를 실행합니다.

```powershell
dotnet run --project samples/Dreamine.Secs.Equipment.Passive
dotnet run --project samples/Dreamine.Secs.Host.Active
```

TCP/Select, S1F13/S1F14, S1F1/S1F2, Linktest, 정상 Separate 및 1회 재접속을 확인합니다. 기본값은 `127.0.0.1:7000`, Session ID 0이며 첫 두 인수로 Host와 Port를 바꿀 수 있습니다.

W-bit Primary는 `AllocateSystemBytes()`로 System Bytes를 할당하고 `SendPrimaryAsync`로 기다리십시오. Reply Transaction을 열지 않는 메시지만 `SendAsync`로 보내고 세션은 반드시 비동기 해제합니다.

이 샘플은 Dreamine 상호 간 경로만 검증하며 외부 Simulator 또는 표준 적합성을 주장하지 않습니다. [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md)를 함께 확인하십시오.
