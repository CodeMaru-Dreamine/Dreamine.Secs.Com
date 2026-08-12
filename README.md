# Dreamine.Secs.Com

Dreamine.Secs.Com is the native Dreamine implementation of the first SECS-II / HSMS communication layer.

[➡️ 한국어 문서 보기](https://github.com/CodeMaru-Dreamine/Dreamine.Secs.Com/blob/main/README_KO.md)

## Implemented scope

- Network-independent SECS-II item codec with exact big-endian numeric widths, nested lists, empty/multi-value items, full-consumption checks, and configurable message/depth/list-count limits.
- Dedicated HSMS codec for the four-byte length prefix and ten-byte header, plus partial/concatenated-frame decoding and T8-aware stream reads.
- Explicit `NotConnected` / `ConnectedNotSelected` / `Selected` state machine for Select, Deselect, Linktest, Reject, Separate, simultaneous Select, and invalid-state handling.
- Session-scoped, thread-safe System Bytes allocation and concurrent primary/secondary correlation with T3, cancellation, duplicate detection, and connection cleanup.
- T5 reconnect, T6 control transactions, T7 not-selected termination, and T8 inter-character failure through injectable `TimeProvider`.
- Single-session Active and Passive TCP runtime, provider composition, bounded diagnostics, and loopback tests requiring no external equipment.
- Session-propagated frame/message/depth/list limits, validated control headers, serialized writes, isolated application callbacks, and individually cleaned transaction failures.
- Opt-in bounded observation of the complete inbound/outbound wire frames actually read or written, with connection epochs, drop accounting, and capture truncation.
- Optional typed HSMS header context on decode failures and diagnostics when a complete header was available.
- Additive provider-neutral `ISecsMessageSession` / `ISecsMessageSessionProvider` support with safe automatically allocated W0/W1 sends, typed identity/state events, and normal dialogue correlation.
- A bounded asynchronous inbound-Primary dispatcher with exact-before-fallback ownership, fixed workers, explicit drop accounting, and one-shot source/epoch-bound replies.

See [`docs/SEMI_REQUIREMENTS_TRACE.md`](https://github.com/CodeMaru-Dreamine/Dreamine.Secs.Com/blob/main/docs/SEMI_REQUIREMENTS_TRACE.md) for the revision/clause-to-code trace and explicit deferrals.

## Public samples and reusable runtime

The Active Host and Passive Equipment samples use the provider-neutral APIs above and the reusable `Dreamine.SecsGem.Interop.Runtime` layer. Their defaults are loopback port `7000`, non-zero Session ID `7`, two bounded connection cycles, and a 120-second whole-run timeout. `--once` selects one connection cycle.

The samples also accept `--profile`, `--template` with `--template-name`, `--scenario`, and `--log-directory`. The checked-in version-1 fixtures demonstrate a non-zero Session ID, a sample-only W0 template, bounded Connect/Select/Linktest execution, and persistent Header-Only JSONL logging. See [QUICKSTART.md](QUICKSTART.md) and [the fixture guide](samples/fixtures/README.md).

Inside the canonical full workspace, the sample projects deliberately use a source `ProjectReference` to `Dreamine.SecsGem.Interop.Runtime`. The published `Dreamine.Secs.Com` package contains the HSMS runtime, not that demo/workbench project. Use a clean package cache when switching from older local `1.0.0` artifacts to the published package set.

## Transport decision

The generic Communication length-prefix codec shares the prefix shape but does not provide HSMS header validation, incremental multi-frame decoding, or T8 behavior. The sealed generic TCP server also hides the per-connection raw stream needed for a selected single session and reply targeting. This package therefore owns a small HSMS-specific TCP channel inside the SECS boundary while still implementing the existing connection lifecycle and leaving all Communication public APIs unchanged.

## Wire evidence boundary

Wire observation is disabled unless `HsmsSessionOptions.WireObservation` is configured. `HsmsSession` then exposes one pull reader through `IHsmsWireObservationSource`; protocol processing never waits for that reader. Outbound observations use the encoded bytes written to the stream and inbound observations use the complete bytes read before decode, so no canonical re-encoding is presented as captured wire data. `CapturedBytes` can be truncated, sequence gaps indicate queue drops, and partial frames are not observations. Raw frame bytes can contain sensitive application payload and must be protected, retained, and exported according to the application's data policy.

Diagnostics and locally re-encoded messages are useful for troubleshooting but are not substitutes for wire observations or external-counterpart evidence.

## Session and dispatcher boundary

The existing `ISecsConnection`, `ISecsCommunicationProvider`, and `CreateConnection` contract are unchanged. The typed message-session/provider contracts and `CreateSession` are additive. Exact S/F registration claims before fallback, and fallback claims before the legacy `MessageReceived` event. Exact W-bit mismatch remains claimed but cannot reply. A full or stopping bounded FIFO drops and counts the newest already-claimed Primary without falling through. The first reply attempt consumes ownership regardless of success, failure, or cancellation; an allowed reply remains bound to the source session, Session ID, Stream, System Bytes, declared normal Secondary, and connection epoch.

The provider clones and validates endpoint, mode, Session ID, role, timer, limit, `AutoReconnect`, dispatcher, and wire-observation settings at session construction. These settings are not live mutable; create a new session to change them. A profile layer applies changes by validate → diff → recreate. Per-call cancellation affects only the current operation. Role controls upper application/responder policy, not the HSMS state machine or Active/Passive TCP direction.

Primary sends have no generic retry policy. Active reconnect attempts are separated by T5. After a Passive connection closes, the listener resumes immediately without an extra backoff.

## Dependency boundary

    Dreamine.Secs.Com
        -> Dreamine.Secs.Abstractions
        -> Dreamine.Communication.Abstractions
        -> Dreamine.Communication.Core
        -> Dreamine.Communication.Sockets

An E37.1 conformance claim is `BLOCKED_STANDARD` because no local E37.1 normative document was available for this implementation. External simulator and field verification remain `NOT_RUN`. Legacy SECS-I, SML, certification, and current-revision conformance claims are `INTENTIONALLY_EXCLUDED` from this package boundary. GEM, GEM300, WPF monitoring, and persistent workbench facilities belong to their separate packages and do not change the Core evidence status.

## License

MIT.
