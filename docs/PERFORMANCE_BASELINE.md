# Performance baseline

Measured 2026-08-10 on Windows 10.0.26200, x64, 16 logical processors (Intel64 family 6 model 154), Release build, .NET 10.0.10 host using SDK 10.0.400-preview. Values are comparative baselines, not service-level guarantees.

| Operation | Iterations | Elapsed | Mean | Allocated/op |
|---|---:|---:|---:|---:|
| Small item encode + decode | 100,000 | 320.147 ms | 3.202 µs | 544 B |
| Depth-12 list encode + decode | 25,000 | 90.411 ms | 3.616 µs | 2,376 B |
| 1 MiB binary item encode + decode | 100 | 147.210 ms | 1.472 ms | 4,194,472 B |
| HSMS frame encode + decode | 100,000 | 180.852 ms | 1.809 µs | 688 B |

The 1 MiB round trip necessarily materializes encoded and decoded buffers; no optimization was made because the bounded behavior was stable and no release bottleneck was demonstrated. The temporary measurement probe used `Stopwatch` and `GC.GetAllocatedBytesForCurrentThread` and is intentionally not part of the package.

The same run completed 1,000 correlated self-loopback primaries, 100 reconnect cycles, and concurrent primaries in 1.129 s overall; primary P95 was 0.451 ms and the result was Passed.
