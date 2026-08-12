# Historical performance evidence

Current-run performance status: `NOT_RUN`.

The values below are Historical Evidence from a 2026-08-10 run on Windows 10.0.26200, x64, 16 logical processors (Intel64 family 6 model 154), a Release build, and the .NET 10.0.10 host using SDK 10.0.400-preview. They are retained for comparison only. They are not a current release claim, a service-level guarantee, or fresh evidence for the present source.

| Operation | Iterations | Elapsed | Mean | Allocated/op |
|---|---:|---:|---:|---:|
| Small item encode + decode | 100,000 | 320.147 ms | 3.202 µs | 544 B |
| Depth-12 list encode + decode | 25,000 | 90.411 ms | 3.616 µs | 2,376 B |
| 1 MiB binary item encode + decode | 100 | 147.210 ms | 1.472 ms | 4,194,472 B |
| HSMS frame encode + decode | 100,000 | 180.852 ms | 1.809 µs | 688 B |

The historical 1 MiB round trip materialized encoded and decoded buffers. The temporary probe used `Stopwatch` and `GC.GetAllocatedBytesForCurrentThread`, but the probe and its raw result artifact were not retained as a repeatable benchmark in this package. Consequently these numbers cannot be promoted to `PASS` for the current run.

The same historical run reported 1,000 correlated self-loopback primaries, 100 reconnect cycles, and concurrent primaries in 1.129 s overall with primary P95 of 0.451 ms. That historical result does not replace fresh functional tests and is not external or field evidence.

To establish a new performance `PASS`, retain a repeatable benchmark/load harness and a fresh artifact recording the exact commit, UTC time, Configuration, SDK/runtime, OS, CPU, message sizes, iteration counts, concurrency, warm-up, measurement method, exit code, and results. Until that is done, the current performance status remains `NOT_RUN`.
