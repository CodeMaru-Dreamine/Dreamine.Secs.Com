# Contributing

Keep public API changes compatible with 1.x. Classify any proposed break as source- and/or binary-breaking in `docs/API_REVIEW.md` before implementation.

Before submitting a change, run Release build/test/pack, both Quick Start builds, deterministic codec fuzz tests, and the self-loopback harness. New race tests should use barriers, completion sources, or injected time instead of timing-only sleeps. Do not add licensed standards, external captures, customer names, secrets, or generated `bin`/`obj` output.

There is no existing per-repository GitHub Actions convention. The projects currently use sibling `ProjectReference` paths, so a standalone workflow must either check out the coordinated repository family or first migrate package composition; do not add a workflow that cannot restore from a clean checkout.
