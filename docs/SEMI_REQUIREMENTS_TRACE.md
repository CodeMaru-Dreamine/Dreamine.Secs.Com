# SEMI requirements trace — SECS-II / HSMS first implementation

This document records the locally held source revisions used for the first implementation. It paraphrases only the behavior required by the code; no SEMI source file or extended source text is included.

| Standard | Document | Local revision / publication | Scope used in this implementation | Latest revision check | Implementation / tests |
|---|---|---|---|---|---|
| SEMI E5 | Equipment Communications Standard 2 Message Content (SECS-II) | E5-0813, available August 2013 (approved June 2013) | §6.4.2 Stream 0–127 and Function 0–255; §6.5 transaction timeout notification; §7.2 primary/secondary and W-bit rules; §8 transaction concepts; §9.2–9.5 item headers, format codes, lengths, byte order, lists, and examples | Required before claiming current-revision conformance. This work claims only behavior traced to E5-0813. | `SecsMessage`, `SecsStream`, `SecsItem` types, `SecsItemCodec`; model/codec/transaction tests |
| SEMI E37 | High-Speed SECS Message Services (HSMS) Generic Services | E37-0413, available April 2013 (approved December 2012) | §5 states; §§7.3–7.10 control/data procedures; §8 4-byte length, 10-byte header, header values and System Bytes; §§9.2–9.4 T3/T5/T6/T7/T8, Separate handling and reply matching; §10.2 ranges/defaults/one-second resolution | Required before claiming current-revision conformance. This work claims only behavior traced to E37-0413. | `HsmsHeader`, codecs, state/session, transaction managers; codec/state/timer/concurrency/loopback tests |
| SEMI E37.1 related-substandard inventory check | Title and revision not asserted without a local source document | No separate E37.1 document is present in `D:\SEMI\SEMI_DOC` | No independent E37.1 clause was used. Single-selected-session behavior in this first implementation is limited to the selected-session procedures explicitly present in E37-0413. | Required before a later conformance or current-revision claim. | Deferred; no class is mapped to an unavailable clause |

## Confirmed implementation decisions

- E5 item format codes and numeric widths are encoded exactly as listed in E5-0813 Table 1. Numeric bodies use big-endian order; floating point uses IEEE 754 bit layouts.
- An item header uses one format byte and one to three length bytes. Zero length-byte count is rejected, and the encoded length is limited to `0xFFFFFF`.
- A list length is its child count. Other item lengths are body byte counts. Empty supported items are valid.
- Defensive implementation limits reject impossible or excessive list counts before allocating child arrays and preflight encoded size before allocating output.
- An HSMS frame length counts the 10-byte header plus optional message text and excludes the four-byte prefix. Control messages contain no text.
- Known control-message reserved fields are validated, and a response must correlate by SType, Session ID, and System Bytes before it can change protocol state.
- HSMS connection state is explicit: `NotConnected`, `ConnectedNotSelected`, or `Selected`. Active/passive describes TCP connection direction and is independent of host/equipment role.
- Default timers follow E37-0413 §10.2: T3 45 s, T5 10 s, T6 5 s, T7 10 s, T8 5 s. Values use the required one-second resolution, T3/T6 start only after request transmission, and tests use `TimeProvider`.

## Audit corrections and public API changes

- `SecsStream` now accepts 0 because E5-0813 §6.4.2 defines the range as 0–127.
- `SecsItemCodecOptions.MaximumListItemCount` makes the defensive allocation boundary explicit and configurable.
- Transaction managers expose `TryAbort` so one failed send cannot terminate unrelated open transactions.
- `SecsDiagnosticKind.ApplicationError` distinguishes upper-layer callback failure from wire/protocol failure; callback exceptions are isolated from the receive loop.
- Maximum frame settings now exclude integer-overflow configurations, and incremental decoding accepts arbitrarily large chunks containing multiple individually bounded frames.
- No Communication project public API was changed.

## Deliberately deferred

- A current SEMI catalog comparison is not available from the local, read-only document set. No claim is made that E5-0813 or E37-0413 is the latest revision.
- T8 is enforced between observable `Stream.ReadAsync` completions. A managed `Stream` cannot expose timing between bytes returned by one operating-system read.
- E5 two-byte-character items and detailed Stream 9 message bodies are outside this first implementation.
- SECS-I, SML, GEM, GEM300, vendor SDK providers, UI, certification, and conformance claims are outside scope.
- The generic `LengthPrefixedMessageFrameCodec` has the same prefix shape but does not expose incremental multi-frame decoding, exact HSMS header validation, or T8 inter-character behavior. The HSMS implementation therefore uses a dedicated codec without modifying Communication projects.
- The existing sealed TCP transports convert frames to generic envelopes and the passive server abstracts away per-connection raw streams. That boundary cannot implement a single selected HSMS session with T8 and reply targeting without structural changes, so the SECS layer uses an internal TCP channel adapter while retaining the existing Communication project references and leaving their public APIs unchanged.
