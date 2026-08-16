# Quick start

## Package-first loopback

From a standalone `Dreamine.Secs.Com` clone, run the sample that references only the published package:

```powershell
dotnet run --project samples/Dreamine.Secs.Com.PackageQuickStart
```

It performs a bounded in-process Passive Equipment / Active Host Select, S1F1/F2, Linktest, and disconnect scenario. No full-workspace demo project is required.

## Full-workspace integration samples

Run the passive equipment first, then the active host in another terminal:

```powershell
dotnet run --project samples/Dreamine.Secs.Equipment.Passive
dotnet run --project samples/Dreamine.Secs.Host.Active
```

The pair demonstrates TCP/Select, S1F13/S1F14, S1F1/S1F2, a sample-only W0 message, Linktest, normal separation, and one reconnect. Defaults are `127.0.0.1:7000`, Session ID `7`, two bounded connection cycles, a 250 ms delay between cycles, and a 120-second whole-run timeout. Host and port may be passed as the first two arguments. Use `--once` for one cycle and `--help` for every option.

These two project commands are source builds for the canonical full workspace. They deliberately `ProjectReference` the sibling, source-only `Dreamine.SecsGem.Interop.Runtime` workbench project. That project is not a published package and is not required by applications consuming `Dreamine.Secs.Com`. Use the package-first loopback above for a standalone clone.

## Versioned profile, template, scenario, and persistent log

The checked-in public fixtures use loopback port `17117` and non-zero Session ID `37`. Start Equipment first, then Host in another PowerShell terminal. Use separate product-owned log directories that are not source-controlled.

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

`--profile` loads a bounded version-1 connection profile. `--template` and `--template-name` select one validated Primary template. `--scenario` runs the shared bounded scenario runner. `--log-directory` enables rolling, retained Header-Only JSONL through the Runtime package. Use `--validate-only` to validate supplied documents without opening a network connection. The fixture message is sample-only and is not a normative message definition or a customer scenario.

Use a unique Active/Passive pair per endpoint and always dispose the session asynchronously. Prefer the provider-neutral safe APIs: `SendAsync(stream, function, item)` allocates System Bytes for W0, while `RequestAsync(dialogue, item)` allocates System Bytes and correlates the declared normal W1 Secondary.

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

Register asynchronous inbound handling through the bounded dispatcher. Exact S/F wins over fallback, which wins over the legacy event. A W-bit mismatch is still claimed but `CanReply` is false; never construct a guessed reply.

```csharp
using var registration = session.PrimaryDispatcher.Register(
    new SecsDialogueDefinition(new SecsStream(1), new SecsFunction(13), new SecsFunction(14)),
    async (context, cancellationToken) =>
    {
        if (context.CanReply)
            await context.ReplyAsync(item: null, cancellationToken: cancellationToken);
    });
```

The bounded FIFO drops the newest already-claimed Primary when full or stopping; inspect `DroppedPrimaryCount`. The first reply attempt consumes ownership whether it succeeds, fails, or is canceled. Use `AllocateSystemBytes`, `SendAsync(SecsMessage, ...)`, and `SendPrimaryAsync` only as expert APIs. The low-level transaction path accepts Function 0 or `Primary Function + 1`; `SecsDialogueDefinition` intentionally declares only normal adjacent-Secondary dialogue.

Session settings are construction snapshots. To change endpoint, mode, Session ID, role, timers, limits, `AutoReconnect`, dispatcher, or wire settings, validate the new profile and create a new session. Per-call cancellation affects only that operation. There is no generic Primary retry; Active reconnect uses T5 separation, while Passive resumes listening immediately after close.

## Optional wire observation

Enable bounded wire observation only when raw evidence is required:

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

Retain `observationReader`, run exactly one reader concurrently with protocol operations, and await it after the session is disposed:

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

The protocol does not wait when the bounded queue is full; inspect `DroppedWireObservationCount` and sequence gaps. `CapturedBytes` can be truncated and can contain sensitive application payload. Protect it as evidence and do not treat partial frames, diagnostic text, or canonical re-encoding as captured wire bytes. For persistent files, prefer the Runtime package's Header-Only facade shown by `--log-directory`; it redacts the endpoint, withholds dynamic diagnostic text, and exposes drop/failure/flush health. Stop or dispose the session first, then stop the log recorder and require `Health.IsEvidenceEligible` before treating the files as evidence.

These samples verify only the local Dreamine-to-Dreamine path. External simulator and field verification remain `NOT_RUN`; an E37.1 conformance claim remains `BLOCKED_STANDARD`. See [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md).
