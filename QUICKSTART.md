# Quick start

Run the passive equipment first, then the active host in another terminal:

```powershell
dotnet run --project samples/Dreamine.Secs.Equipment.Passive
dotnet run --project samples/Dreamine.Secs.Host.Active
```

The pair demonstrates TCP/Select, S1F13/S1F14, S1F1/S1F2, Linktest, normal separation, and one reconnect. Defaults are `127.0.0.1:7000`, Session ID 0. Host and port may be passed as the first two arguments.

Use a unique Active/Passive pair per endpoint. Construct every W-bit primary with `AllocateSystemBytes()` and await `SendPrimaryAsync`; use `SendAsync` only for messages that do not open a reply transaction. Always dispose the session asynchronously.

These samples verify the Dreamine-to-Dreamine path. They do not claim external simulator or standards conformance; see [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md).
