# Public runtime sample fixtures

These version-1 fixtures demonstrate the public, provider-neutral sample boundary:

- `host-profile.json` and `equipment-profile.json` configure a non-zero Session ID on loopback port `17117`.
- `message-catalog.json` defines one sample-only Host-to-Equipment `S127F3 W=0` template.
- `host-scenario.json` runs bounded Connect, Select, and Linktest steps through the shared Scenario v1 runner.

Run the Equipment sample first, then the Host sample. Use separate log directories. The resulting JSONL is bounded Header-Only operational evidence, not a standards-conformance certificate and not a customer scenario.

장비 샘플을 먼저 실행한 다음 Host 샘플을 실행하십시오. 로그 디렉터리는 서로 다르게 지정합니다. 생성되는 JSONL은 제한 용량 Header-Only 운영 증거이며, 표준 적합성 인증서나 고객 시나리오가 아닙니다.
