# Known limitations

- HSMS-SS is single-session. Multi-session/general-session behavior is not implemented.
- SECS-I, SML, two-byte-character items, detailed Stream 9 bodies, and GEM message catalogs are not implemented here.
- Active automatic reconnect exists, but application-level re-establishment after reconnect remains the caller's policy.
- The synchronous `MessageReceived` event is exception-isolated but provides no async backpressure.
- External simulator results remain **Not Run / Waiting for User** unless captured by a separately documented manual run.
- Unit and self-loopback tests are evidence for this implementation, not certification or current-revision conformance.
