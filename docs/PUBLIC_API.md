# Public API Inventory

Assembly: `Dreamine.Secs.Com`

This inventory is generated from the compiled Release assembly. It is an audit artifact, not an additional compatibility promise.

Exported types: **16**

## Types

### `public sealed class Dreamine.Secs.Com.Codecs.SecsItemCodec`

- `Dreamine.Secs.Abstractions.Model.SecsItem Decode(System.ReadOnlyMemory<System.Byte> data)`
- `Dreamine.Secs.Abstractions.Validation.SecsValidationResult Validate(System.ReadOnlyMemory<System.Byte> data)`
- `SecsItemCodec()`
- `SecsItemCodec(Dreamine.Secs.Com.Codecs.SecsItemCodecOptions options)`
- `System.Byte[] Encode(Dreamine.Secs.Abstractions.Model.SecsItem item)`

### `public sealed class Dreamine.Secs.Com.Codecs.SecsItemCodecOptions`

- `SecsItemCodecOptions()`
- `System.Int32 MaximumListItemCount { get; set; }`
- `System.Int32 MaximumMessageLength { get; set; }`
- `System.Int32 MaximumNestingDepth { get; set; }`
- `System.Void Validate()`

### `public sealed class Dreamine.Secs.Com.Diagnostics.NullSecsDiagnosticSink`

- `Dreamine.Secs.Com.Diagnostics.NullSecsDiagnosticSink Instance { get; }`
- `System.Void Emit(Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticEvent diagnosticEvent)`

### `public sealed class Dreamine.Secs.Com.DreamineSecsCommunicationProvider`

- `Dreamine.Secs.Abstractions.Interfaces.ISecsConnection CreateConnection(Dreamine.Secs.Abstractions.Options.SecsConnectionOptions options)`
- `Dreamine.Secs.Abstractions.Interfaces.ISecsMessageSession CreateSession(Dreamine.Secs.Abstractions.Options.SecsConnectionOptions options)`
- `DreamineSecsCommunicationProvider()`
- `DreamineSecsCommunicationProvider(System.Func<Dreamine.Secs.Abstractions.Options.SecsConnectionOptions, Dreamine.Secs.Abstractions.Hsms.HsmsSessionOptions> optionsFactory, System.TimeProvider timeProvider, Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink diagnostics)`
- `System.String Key { get; }`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsFrameCodec`

- `Dreamine.Secs.Abstractions.Hsms.HsmsMessage Decode(System.ReadOnlyMemory<System.Byte> frame)`
- `HsmsFrameCodec()`
- `HsmsFrameCodec(Dreamine.Secs.Com.Hsms.HsmsFrameCodecOptions options, Dreamine.Secs.Com.Codecs.SecsItemCodec itemCodec)`
- `System.Byte[] Encode(Dreamine.Secs.Abstractions.Hsms.HsmsMessage message)`
- `System.Int32 MaximumFrameLength { get; }`
- `System.Threading.Tasks.Task WriteAsync(System.IO.Stream stream, Dreamine.Secs.Abstractions.Hsms.HsmsMessage message, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task<Dreamine.Secs.Abstractions.Hsms.HsmsMessage> ReadAsync(System.IO.Stream stream, System.TimeSpan t8, System.TimeProvider timeProvider, System.Threading.CancellationToken cancellationToken)`
- `const System.Int32 HeaderLength = 10`
- `const System.Int32 LengthPrefixSize = 4`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsFrameCodecOptions`

- `HsmsFrameCodecOptions()`
- `System.Int32 MaximumFrameLength { get; set; }`
- `System.Void Validate()`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsProcessingResult`

- `Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage Response { get; }`
- `HsmsProcessingResult(System.Boolean accepted, Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage response, System.Boolean closeConnection)`
- `System.Boolean Accepted { get; }`
- `System.Boolean CloseConnection { get; }`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsSession`

- `Dreamine.Communication.Abstractions.Enums.ConnectionState State { get; }`
- `Dreamine.Secs.Abstractions.Hsms.HsmsConnectionState HsmsState { get; }`
- `Dreamine.Secs.Abstractions.Interfaces.ISecsPrimaryDispatcher PrimaryDispatcher { get; }`
- `Dreamine.Secs.Abstractions.Model.SecsConnectionIdentity ConnectionIdentity { get; }`
- `Dreamine.Secs.Abstractions.Model.SecsSystemBytes AllocateSystemBytes()`
- `HsmsSession(Dreamine.Secs.Abstractions.Hsms.HsmsSessionOptions options, System.TimeProvider timeProvider, Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink diagnostics)`
- `System.Boolean IsWireObservationEnabled { get; }`
- `System.Collections.Generic.IAsyncEnumerable<Dreamine.Secs.Abstractions.Hsms.HsmsWireObservation> ReadWireObservationsAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Int64 DroppedWireObservationCount { get; }`
- `System.String ProviderKey { get; }`
- `System.Threading.Tasks.Task ConnectAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task DeselectAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task DisconnectAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task LinktestAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task SelectAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task SendAsync(Dreamine.Secs.Abstractions.Model.SecsMessage message, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task SendAsync(Dreamine.Secs.Abstractions.Model.SecsStream stream, Dreamine.Secs.Abstractions.Model.SecsFunction function, Dreamine.Secs.Abstractions.Model.SecsItem item, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task SeparateAsync(System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task<Dreamine.Secs.Abstractions.Model.SecsMessage> RequestAsync(Dreamine.Secs.Abstractions.Model.SecsDialogueDefinition dialogue, Dreamine.Secs.Abstractions.Model.SecsItem item, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.Task<Dreamine.Secs.Abstractions.Model.SecsMessage> SendPrimaryAsync(Dreamine.Secs.Abstractions.Model.SecsMessage message, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.ValueTask DisposeAsync()`
- `event System.EventHandler<Dreamine.Secs.Abstractions.Diagnostics.SecsDiagnosticEvent> DiagnosticReceived`
- `event System.EventHandler<Dreamine.Secs.Abstractions.Hsms.SecsSessionStateChangedEventArgs> StateChanged`
- `event System.EventHandler<Dreamine.Secs.Abstractions.Model.SecsMessage> MessageReceived`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsStateMachine`

- `Dreamine.Secs.Abstractions.Hsms.HsmsConnectionState State { get; }`
- `Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage CreateDeselectRequest(Dreamine.Secs.Abstractions.Model.SecsSystemBytes systemBytes)`
- `Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage CreateLinktestRequest(Dreamine.Secs.Abstractions.Model.SecsSystemBytes systemBytes)`
- `Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage CreateSelectRequest(Dreamine.Secs.Abstractions.Model.SecsSystemBytes systemBytes)`
- `Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage CreateSeparateRequest(Dreamine.Secs.Abstractions.Model.SecsSystemBytes systemBytes)`
- `Dreamine.Secs.Com.Hsms.HsmsProcessingResult Process(Dreamine.Secs.Abstractions.Hsms.HsmsMessage message)`
- `HsmsStateMachine(Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink diagnostics)`
- `System.Void OnTcpConnected()`
- `System.Void OnTcpDisconnected()`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsStreamDecoder`

- `HsmsStreamDecoder(Dreamine.Secs.Com.Hsms.HsmsFrameCodec codec)`
- `System.Collections.Generic.IReadOnlyList<Dreamine.Secs.Abstractions.Hsms.HsmsMessage> Append(System.ReadOnlySpan<System.Byte> data)`
- `System.Int32 BufferedByteCount { get; }`
- `System.Void Complete()`
- `System.Void Reset()`

### `public sealed class Dreamine.Secs.Com.Hsms.HsmsTimerScheduler`

- `HsmsTimerScheduler(Dreamine.Secs.Abstractions.Hsms.HsmsTimerOptions options, System.TimeProvider timeProvider)`
- `System.Threading.Tasks.Task WaitForExpirationAsync(Dreamine.Secs.Abstractions.Hsms.HsmsTimerKind kind, System.Threading.CancellationToken cancellationToken)`
- `System.TimeSpan GetTimeout(Dreamine.Secs.Abstractions.Hsms.HsmsTimerKind kind)`

### `public sealed class Dreamine.Secs.Com.SecsComAssemblyMarker`

- No declared public members.

### `public sealed class Dreamine.Secs.Com.Transactions.HsmsControlTransactionManager`

- `HsmsControlTransactionManager(System.TimeProvider timeProvider, Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink diagnostics)`
- `System.Boolean TryAbort(Dreamine.Secs.Abstractions.Model.SecsSystemBytes systemBytes, System.Exception error)`
- `System.Boolean TryComplete(Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage response)`
- `System.Int32 OutstandingCount { get; }`
- `System.Threading.Tasks.Task<Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage> RegisterAsync(Dreamine.Secs.Abstractions.Hsms.HsmsControlMessage request, System.TimeSpan t6, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.ValueTask DisposeAsync()`
- `System.Void AbortAll(System.Exception error)`
### `public sealed class Dreamine.Secs.Com.Transactions.SecsSystemBytesGenerator`

- `Dreamine.Secs.Abstractions.Model.SecsSystemBytes Next()`
- `SecsSystemBytesGenerator(System.UInt32 seed)`

### `public enum Dreamine.Secs.Com.Transactions.SecsTransactionCompletionStatus`

- `const Dreamine.Secs.Com.Transactions.SecsTransactionCompletionStatus Completed = 0`
- `const Dreamine.Secs.Com.Transactions.SecsTransactionCompletionStatus Duplicate = 1`
- `const Dreamine.Secs.Com.Transactions.SecsTransactionCompletionStatus InvalidCorrelation = 3`
- `const Dreamine.Secs.Com.Transactions.SecsTransactionCompletionStatus UnknownSystemBytes = 2`

### `public sealed class Dreamine.Secs.Com.Transactions.SecsTransactionManager`

- `Dreamine.Secs.Abstractions.Model.SecsSystemBytes AllocateSystemBytes()`
- `Dreamine.Secs.Com.Transactions.SecsTransactionCompletionStatus TryComplete(Dreamine.Secs.Abstractions.Model.SecsMessage secondary)`
- `SecsTransactionManager(System.TimeProvider timeProvider, Dreamine.Secs.Com.Transactions.SecsSystemBytesGenerator generator, Dreamine.Secs.Abstractions.Diagnostics.ISecsDiagnosticSink diagnostics, System.Int32 recentCapacity)`
- `System.Boolean TryAbort(Dreamine.Secs.Abstractions.Model.SecsSystemBytes systemBytes, System.Exception error)`
- `System.Int32 OutstandingCount { get; }`
- `System.Threading.Tasks.Task<Dreamine.Secs.Abstractions.Model.SecsMessage> RegisterPrimaryAsync(Dreamine.Secs.Abstractions.Model.SecsMessage primary, System.TimeSpan t3, System.Threading.CancellationToken cancellationToken)`
- `System.Threading.Tasks.ValueTask DisposeAsync()`
- `System.Void AbortAll(System.Exception error)`
