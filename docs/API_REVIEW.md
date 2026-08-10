# Public API review

Review date: 2026-08-10. Baseline: 1.0.0 source. See [PUBLIC_API.md](PUBLIC_API.md) for every exported type and member.

## Result

- The dependency graph is acyclic and points from implementation to `Secs.Abstractions` and Communication packages only.
- `HsmsSession` owns socket/session resources, implements `IAsyncDisposable`, accepts cancellation on lifecycle and send operations, injects `TimeProvider`, serializes writes, and isolates diagnostic/application subscriber exceptions.
- Codecs enforce full consumption, bounded message length/list count/depth, and structured protocol exceptions.
- Transaction correlation now accepts only F0 or the exact paired secondary function. A higher unrelated even function no longer consumes an open transaction.
- No public signature or binary surface changed. All hardening changes are behavioral validation or race fixes.

## Next-version proposals

| Classification | Proposal | Reason |
|---|---|---|
| Source- and binary-breaking | Implement a provider-neutral `IHsmsSession` contract and return it from composition | Consumers currently depend on concrete `HsmsSession` for protocol operations. |
| Source- and binary-breaking | Add async primary-message dispatch with explicit acknowledgement/backpressure | The synchronous event is intentionally isolated but cannot be awaited. |
| Non-breaking candidate | Add opt-in session metrics snapshots | Enables diagnostics without coupling the runtime to a logging/metrics framework. |

The runtime is single-session HSMS-SS. Thread safety applies to lifecycle, writes, allocation, and transactions; callers must still avoid conflicting semantic commands such as simultaneous manual disconnect and separate.
