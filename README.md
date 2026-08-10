# Dreamine.Secs.Com

Dreamine.Secs.Com is the native Dreamine implementation of the first SECS-II / HSMS communication layer.

[➡️ 한국어 문서 보기](https://github.com/CodeMaru-Dreamine/Dreamine.Secs.Com/blob/main/README_KO.md)

## Implemented scope

- Network-independent SECS-II item codec with exact big-endian numeric widths, nested lists, empty/multi-value items, full-consumption checks, and configurable size/depth limits.
- Dedicated HSMS codec for the four-byte length prefix and ten-byte header, plus partial/concatenated-frame decoding and T8-aware stream reads.
- Explicit `NotConnected` / `ConnectedNotSelected` / `Selected` state machine for Select, Deselect, Linktest, Reject, Separate, simultaneous Select, and invalid-state handling.
- Session-scoped, thread-safe System Bytes allocation and concurrent primary/secondary correlation with T3, cancellation, duplicate detection, and connection cleanup.
- T5 reconnect, T6 control transactions, T7 not-selected termination, and T8 inter-character failure through injectable `TimeProvider`.
- Single-session Active and Passive TCP runtime, provider composition, bounded diagnostics, and loopback tests requiring no external equipment.
- Defensive list-count/size/depth limits, validated control headers, serialized writes, isolated application callbacks, and individually cleaned transaction failures.

See [`docs/SEMI_REQUIREMENTS_TRACE.md`](./docs/SEMI_REQUIREMENTS_TRACE.md) for the revision/clause-to-code trace and explicit deferrals.

## Transport decision

The generic Communication length-prefix codec shares the prefix shape but does not provide HSMS header validation, incremental multi-frame decoding, or T8 behavior. The sealed generic TCP server also hides the per-connection raw stream needed for a selected single session and reply targeting. This package therefore owns a small HSMS-specific TCP channel inside the SECS boundary while still implementing the existing connection lifecycle and leaving all Communication public APIs unchanged.

## Dependency boundary

    Dreamine.Secs.Com
        -> Dreamine.Secs.Abstractions
        -> Dreamine.Communication.Abstractions
        -> Dreamine.Communication.Core
        -> Dreamine.Communication.Sockets

The single-session runtime is provisional with respect to SEMI E37.1 because no local E37.1 normative document was available for this implementation. SECS-I, SML, GEM, GEM300, vendor SDK providers, WPF monitoring, telemetry, external interoperability, certification, and conformance claims are not included.

## License

MIT.
