# Public API review

Review date: 2026-08-12. Baseline package version: 1.0.0 source. See [PUBLIC_API.md](PUBLIC_API.md) for every exported type and member.

## Result

- The dependency graph is acyclic and points from the implementation to `Secs.Abstractions` and Communication packages only.
- `HsmsSession` owns socket/session resources, implements `IAsyncDisposable`, accepts cancellation on lifecycle and send operations, injects `TimeProvider`, serializes writes, and isolates diagnostic/application subscriber exceptions.
- Codecs enforce full consumption, bounded frame/message length, list count and nesting depth, and structured protocol exceptions. Runtime session options are propagated to both inbound and outbound codec paths before message-sized allocations.
- The low-level transaction manager accepts Function 0 or the Primary Function plus one. The high-level `SecsDialogueDefinition` deliberately models only normal W0 or adjacent-Secondary W1 dialogue; Function 0 remains a special low-level transaction termination boundary.
- This hardening pass made additive public API changes: `HsmsSession` now implements `ISecsMessageSession`, retains `IHsmsWireObservationSource`, and exposes provider-neutral safe sends, typed state/identity/diagnostics, and the bounded dispatcher. `DreamineSecsCommunicationProvider` now also implements `ISecsMessageSessionProvider`. Reflection over the Release assembly still records **16 exported types**.
- Existing constructors, `CreateConnection`, `ISecsConnection`, `ISecsCommunicationProvider`, package ID, and target framework were retained. No existing public member was removed or made required.

Wire observation is opt-in and disabled by default. Outbound observations use the encoded bytes written to the stream; inbound observations use the complete bytes read before decode. The bounded queue never waits for a consumer, reports drops, and can retain only a configured prefix of a complete frame. Captured data can contain sensitive application payload. Partial frames are not reported.

The source still produces `Dreamine.Secs.Com.1.0.0`, whose generated dependency is `Dreamine.Secs.Abstractions 1.0.0`. The new runtime references additive types and members absent from an older cached Abstractions 1.0.0 binary, so those artifacts must not be mixed. Before publication, both packages need a matching new version and a consumer smoke test using an isolated local feed and package cache. This review does not change a package version or publish a package.

## Implemented provider-neutral session and dispatcher

- The provider clones and validates construction options. Endpoint, mode, Session ID, role, timers, limits, `AutoReconnect`, dispatcher settings, and wire settings are session-construction snapshots rather than live controls. A later profile application must validate, diff, and recreate when a snapshot changes.
- Per-call cancellation affects only the current operation. Role is upper application/responder policy and is independent from the HSMS state machine and Active/Passive TCP direction.
- Inbound Primary ownership is exact S/F, then fallback, then the legacy `MessageReceived` event. Exact W-bit mismatch remains claimed but cannot reply and never falls through.
- Accepted Primary work enters a bounded FIFO serviced by fixed workers; no per-message `Task.Run` is created. When full or stopping, the newest already-claimed Primary is dropped, counted, and never forwarded to another path.
- The first reply attempt consumes one-shot ownership regardless of success, failure, or cancellation. A permitted reply keeps the source session, Session ID, Stream, System Bytes, declared normal Secondary, and connection epoch.
- `StateChanged` reports every typed TCP/HSMS transition outside internal locks. Handler exceptions are observed and isolated; stop, dispose, disconnect, and reconnect cancel/drain dispatcher work without transferring a claim.
- There is no generic Primary retry. Active reconnect separation follows T5; Passive immediately returns to listening after a connection closes, without adding an extra backoff policy.

## Next-version proposals

| Classification | Proposal | Reason |
|---|---|---|
| Non-breaking candidate | Add opt-in session metrics snapshots beyond existing drop counters | Enables diagnostics without coupling the runtime to a logging/metrics framework. |

The runtime is single-session HSMS-SS. Thread safety applies to lifecycle, writes, allocation, transactions, and the bounded dispatcher; callers must still avoid conflicting semantic commands such as simultaneous manual disconnect and separate. The synchronous legacy `MessageReceived` path is reached only for an unclaimed Primary and can still block receive progress if its subscriber does not return.
