using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Providers;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Codecs;
using Dreamine.Secs.Com.Diagnostics;
using Dreamine.Secs.Com.Transactions;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>단일 HSMS Active 또는 Passive TCP 세션을 합성으로 구현합니다.</para> \endif \if EN <para>Implements one active or passive HSMS TCP session through composition.</para> \endif</summary>
public sealed class HsmsSession : ISecsConnection
{
    private readonly HsmsSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ISecsDiagnosticSink _diagnostics;
    private readonly HsmsFrameCodec _codec;
    private readonly HsmsStateMachine _stateMachine;
    private readonly HsmsTimerScheduler _timers;
    private readonly SecsSystemBytesGenerator _systemBytes;
    private readonly SecsTransactionManager _transactions;
    private readonly HsmsControlTransactionManager _controlTransactions;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly CancellationTokenSource _disposeCancellation = new();
    private TcpClient? _client;
    private TcpListener? _listener;
    private NetworkStream? _stream;
    private CancellationTokenSource? _connectionCancellation;
    private CancellationTokenSource? _t7Cancellation;
    private Task? _receiveLoop;
    private Task? _reconnectLoop;
    private int _connectionState = (int)ConnectionState.Disconnected;
    private int _disposed;

    /// <summary>\if KO 설정, 시간 및 진단 경계로 세션을 만듭니다. \endif \if EN Creates a session with settings, time, and diagnostic boundaries. \endif</summary>
    /// <param name="options">\if KO 세션 설정입니다. \endif \if EN Session settings. \endif</param><param name="timeProvider">\if KO 시간 공급자입니다. \endif \if EN Time provider. \endif</param><param name="diagnostics">\if KO 진단 sink입니다. \endif \if EN Diagnostic sink. \endif</param>
    public HsmsSession(HsmsSessionOptions options, TimeProvider? timeProvider = null, ISecsDiagnosticSink? diagnostics = null)
    {
        ArgumentNullException.ThrowIfNull(options); options.Validate();
        if (options.Mode == SecsConnectionMode.Unspecified) throw new ArgumentException("HSMS connection mode must be Active or Passive.", nameof(options));
        if (options.Role == SecsRole.Unspecified) throw new ArgumentException("SECS role must be Host or Equipment.", nameof(options));
        _options = options; _timeProvider = timeProvider ?? TimeProvider.System; _diagnostics = SafeSecsDiagnosticSink.Wrap(diagnostics);
        _codec = new HsmsFrameCodec(new HsmsFrameCodecOptions { MaximumFrameLength = options.MaximumFrameLength }, new SecsItemCodec(new SecsItemCodecOptions { MaximumMessageLength = options.MaximumFrameLength }));
        _stateMachine = new HsmsStateMachine(_diagnostics);
        _timers = new HsmsTimerScheduler(options.Timers, _timeProvider);
        _systemBytes = new SecsSystemBytesGenerator();
        _transactions = new SecsTransactionManager(_timeProvider, _systemBytes, _diagnostics);
        _controlTransactions = new HsmsControlTransactionManager(_timeProvider, _diagnostics);
    }

    /// <inheritdoc />
    public string ProviderKey => SecsProviderKeys.Dreamine;

    /// <inheritdoc />
    public ConnectionState State => (ConnectionState)Volatile.Read(ref _connectionState);

    /// <summary>\if KO 현재 HSMS protocol 상태입니다. \endif \if EN Gets the current HSMS protocol state. \endif</summary>
    public HsmsConnectionState HsmsState => _stateMachine.State;

    /// <summary>\if KO 선택된 세션에서 상위 계층으로 전달되는 SECS 메시지 이벤트입니다. \endif \if EN Raised for SECS messages accepted in a selected session. \endif</summary>
    public event EventHandler<SecsMessage>? MessageReceived;

    /// <inheritdoc />
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellation.Token);
        var operationToken = operationCancellation.Token;
        await _lifecycleGate.WaitAsync(operationToken).ConfigureAwait(false);
        try
        {
            if (State is ConnectionState.Connected or ConnectionState.Listening or ConnectionState.Connecting) return;
            SetConnectionState(_options.Mode == SecsConnectionMode.Passive ? ConnectionState.Listening : ConnectionState.Connecting);
            _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionAttempt, $"HSMS {_options.Mode} connection attempt to {_options.Host}:{_options.Port}."));
            try
            {
                var client = _options.Mode == SecsConnectionMode.Active
                    ? await ConnectActiveAsync(operationToken).ConfigureAwait(false)
                    : await AcceptPassiveAsync(operationToken).ConfigureAwait(false);
                client.NoDelay = true;
                _client = client; _stream = client.GetStream();
                _connectionCancellation = CancellationTokenSource.CreateLinkedTokenSource(_disposeCancellation.Token);
                _stateMachine.OnTcpConnected();
                SetConnectionState(ConnectionState.Connected);
                _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionEstablished, "HSMS TCP connection established.", _stateMachine.State));
                StartT7();
                _receiveLoop = ReceiveLoopAsync(_stream, _connectionCancellation.Token);
                BackgroundTaskObserver.Observe(_receiveLoop, "HSMS receive loop");
            }
            catch (Exception exception)
            {
                CleanupSocket();
                SetConnectionState(ConnectionState.Faulted);
                _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, $"HSMS connection attempt failed: {exception.Message}."));
                if (exception is not OperationCanceledException && _options.AutoReconnect && Volatile.Read(ref _disposed) == 0 &&
                    (_reconnectLoop is null || _reconnectLoop.IsCompleted))
                {
                    _reconnectLoop = ReconnectLoopAsync(_disposeCancellation.Token);
                    BackgroundTaskObserver.Observe(_reconnectLoop, "HSMS reconnect loop");
                }
                throw;
            }
        }
        finally { _lifecycleGate.Release(); }
    }

    /// <inheritdoc />
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        Task? receiveLoop;
        try
        {
            if (State == ConnectionState.Disconnected) return;
            SetConnectionState(ConnectionState.Disconnecting);
            CancelConnection();
            receiveLoop = _receiveLoop;
            CleanupSocket();
            _stateMachine.OnTcpDisconnected();
            _transactions.AbortAll(new IOException("HSMS connection closed."));
            _controlTransactions.AbortAll(new IOException("HSMS connection closed."));
            SetConnectionState(ConnectionState.Disconnected);
            _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, "HSMS connection closed normally.", _stateMachine.State));
        }
        finally { _lifecycleGate.Release(); }
        if (receiveLoop is not null)
        {
            await receiveLoop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>\if KO Select transaction을 수행하고 선택 상태가 될 때까지 기다립니다. \endif \if EN Performs a Select transaction and waits until selected. \endif</summary>
    /// <param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task SelectAsync(CancellationToken cancellationToken = default)
    {
        HsmsControlMessage request;
        try { request = _stateMachine.CreateSelectRequest(_transactions.AllocateSystemBytes()); }
        catch (HsmsStateException) when (_stateMachine.State == HsmsConnectionState.Selected)
        {
            return;
        }
        var responseTask = _controlTransactions.RegisterDeferred(request, cancellationToken);
        try
        {
            await SendFrameAsync(request, cancellationToken).ConfigureAwait(false);
            _ = _controlTransactions.StartTimeout(request.Header.SystemBytes, _options.Timers.T6, cancellationToken);
            var response = await responseTask.ConfigureAwait(false);
            if (response.Header.HeaderByte3 != (byte)HsmsSelectStatus.Success && response.Header.HeaderByte3 != (byte)HsmsSelectStatus.AlreadyActive)
                throw new SecsProtocolException(SecsValidationCode.InvalidMessage, $"Select failed with status {response.Header.HeaderByte3}.");
        }
        catch (HsmsTimerExpiredException) { await FailConnectionAsync(new HsmsTimerExpiredException("T6", _options.Timers.T6)).ConfigureAwait(false); throw; }
        catch (Exception exception)
        {
            _controlTransactions.TryAbort(request.Header.SystemBytes, exception);
            ObserveIfFaulted(responseTask);
            throw;
        }
    }

    /// <summary>\if KO Deselect transaction을 수행합니다. \endif \if EN Performs a Deselect transaction. \endif</summary>
    /// <param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task DeselectAsync(CancellationToken cancellationToken = default)
    {
        var request = _stateMachine.CreateDeselectRequest(_transactions.AllocateSystemBytes());
        var responseTask = _controlTransactions.RegisterDeferred(request, cancellationToken);
        try
        {
            await SendFrameAsync(request, cancellationToken).ConfigureAwait(false);
            _ = _controlTransactions.StartTimeout(request.Header.SystemBytes, _options.Timers.T6, cancellationToken);
            var response = await responseTask.ConfigureAwait(false);
            if (response.Header.HeaderByte3 != (byte)HsmsDeselectStatus.Success)
                throw new SecsProtocolException(SecsValidationCode.InvalidMessage, $"Deselect failed with status {response.Header.HeaderByte3}.");
            StartT7();
        }
        catch (HsmsTimerExpiredException) { await FailConnectionAsync(new HsmsTimerExpiredException("T6", _options.Timers.T6)).ConfigureAwait(false); throw; }
        catch (Exception exception)
        {
            _controlTransactions.TryAbort(request.Header.SystemBytes, exception);
            ObserveIfFaulted(responseTask);
            throw;
        }
    }

    /// <summary>\if KO Linktest transaction을 수행합니다. \endif \if EN Performs a Linktest transaction. \endif</summary>
    /// <param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task LinktestAsync(CancellationToken cancellationToken = default)
    {
        var request = _stateMachine.CreateLinktestRequest(_transactions.AllocateSystemBytes());
        var responseTask = _controlTransactions.RegisterDeferred(request, cancellationToken);
        try
        {
            await SendFrameAsync(request, cancellationToken).ConfigureAwait(false);
            _ = _controlTransactions.StartTimeout(request.Header.SystemBytes, _options.Timers.T6, cancellationToken);
            _ = await responseTask.ConfigureAwait(false);
        }
        catch (HsmsTimerExpiredException) { await FailConnectionAsync(new HsmsTimerExpiredException("T6", _options.Timers.T6)).ConfigureAwait(false); throw; }
        catch (Exception exception)
        {
            _controlTransactions.TryAbort(request.Header.SystemBytes, exception);
            ObserveIfFaulted(responseTask);
            throw;
        }
    }

    /// <summary>\if KO Separate.req를 전송하고 TCP 연결을 종료합니다. \endif \if EN Sends Separate.req and closes TCP. \endif</summary>
    /// <param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task SeparateAsync(CancellationToken cancellationToken = default)
    {
        var request = _stateMachine.CreateSeparateRequest(_transactions.AllocateSystemBytes());
        try { await SendFrameAsync(request, cancellationToken).ConfigureAwait(false); }
        catch (Exception exception) { await FailConnectionAsync(exception).ConfigureAwait(false); throw; }
        await DisconnectAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>\if KO 선택된 세션에서 응답을 기다리지 않는 SECS 메시지를 보냅니다. \endif \if EN Sends a SECS message without waiting for a reply in a selected session. \endif</summary>
    /// <param name="message">\if KO 메시지입니다. \endif \if EN Message. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task SendAsync(SecsMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_stateMachine.State != HsmsConnectionState.Selected) throw new HsmsStateException(_stateMachine.State, nameof(SendAsync));
        ValidateSessionId(message);
        if (message.ReplyExpected) throw new ArgumentException("Use SendPrimaryAsync for a W-bit primary.", nameof(message));
        await SendFrameAsync(new HsmsDataMessage(message), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>\if KO W-bit primary를 보내고 T3 내의 secondary를 기다립니다. \endif \if EN Sends a W-bit primary and waits for its secondary within T3. \endif</summary>
    /// <param name="message">\if KO primary입니다. \endif \if EN Primary. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param><returns>\if KO secondary입니다. \endif \if EN Secondary. \endif</returns>
    public async Task<SecsMessage> SendPrimaryAsync(SecsMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_stateMachine.State != HsmsConnectionState.Selected) throw new HsmsStateException(_stateMachine.State, nameof(SendPrimaryAsync));
        ValidateSessionId(message);
        var response = _transactions.RegisterPrimaryDeferred(message, cancellationToken);
        try
        {
            await SendFrameAsync(new HsmsDataMessage(message), cancellationToken).ConfigureAwait(false);
            _ = _transactions.StartTimeout(message.SystemBytes, _options.Timers.T3, cancellationToken);
            _diagnostics.Emit(new(SecsDiagnosticKind.PrimarySent, $"Primary sent with System Bytes 0x{message.SystemBytes.Value:X8}."));
            return await response.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (!response.IsCompleted) _transactions.TryAbort(message.SystemBytes, exception);
            ObserveIfFaulted(response);
            throw;
        }
    }

    /// <summary>\if KO 열린 transaction과 충돌하지 않는 System Bytes를 할당합니다. \endif \if EN Allocates System Bytes not colliding with an open transaction. \endif</summary>
    public SecsSystemBytes AllocateSystemBytes() => _transactions.AllocateSystemBytes();

    /// <summary>\if KO 세션과 모든 비동기 자원을 안전하게 해제합니다. \endif \if EN Safely disposes the session and all asynchronous resources. \endif</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _disposeCancellation.Cancel();
        await DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
        if (_reconnectLoop is not null)
        {
            try { await _reconnectLoop.ConfigureAwait(false); }
            catch (OperationCanceledException) when (_disposeCancellation.IsCancellationRequested)
            {
                // Disposal owns this cancellation and has already closed session resources.
            }
        }
        await _transactions.DisposeAsync().ConfigureAwait(false);
        await _controlTransactions.DisposeAsync().ConfigureAwait(false);
        _disposeCancellation.Dispose(); _lifecycleGate.Dispose(); _sendGate.Dispose();
    }

    private async Task<TcpClient> ConnectActiveAsync(CancellationToken cancellationToken)
    {
        var client = new TcpClient();
        try { await client.ConnectAsync(_options.Host, _options.Port, cancellationToken).ConfigureAwait(false); return client; }
        catch { client.Dispose(); throw; }
    }

    private async Task<TcpClient> AcceptPassiveAsync(CancellationToken cancellationToken)
    {
        var address = ResolveBindAddress(_options.Host);
        _listener = new TcpListener(address, _options.Port); _listener.Start(1);
        try { return await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false); }
        finally { _listener.Stop(); _listener = null; }
    }

    private async Task ReceiveLoopAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await _codec.ReadAsync(stream, _options.Timers.T8, _timeProvider, cancellationToken).ConfigureAwait(false);
                if (message is null) break;
                _diagnostics.Emit(new(SecsDiagnosticKind.FrameReceived, "HSMS frame received.", _stateMachine.State, _codec.Encode(message).Length));
                var before = _stateMachine.State;
                HsmsProcessingResult? result = null;
                if (message is HsmsControlMessage response && IsControlResponse(response.SType))
                {
                    if (!_controlTransactions.TryCompleteAfter(response, () => result = _stateMachine.Process(response)))
                    {
                        BackgroundTaskObserver.Observe(SendRejectBestEffortAsync(response.Header, HsmsRejectReason.TransactionNotOpen), "unmatched control response reject");
                        continue;
                    }
                }
                else
                {
                    result = _stateMachine.Process(message);
                }
                var processing = result ?? throw new InvalidOperationException("HSMS control response processing did not produce a result.");
                if (processing.Response is not null) await SendFrameAsync(processing.Response, cancellationToken).ConfigureAwait(false);
                if (before != _stateMachine.State)
                {
                    if (_stateMachine.State == HsmsConnectionState.Selected) CancelT7();
                    else if (_stateMachine.State == HsmsConnectionState.ConnectedNotSelected) StartT7();
                }
                if (processing.Accepted && message is HsmsDataMessage data) HandleData(data);
                if (processing.CloseConnection) break;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return; }
        catch (Exception exception) { failure = exception; }
        await ConnectionEndedAsync(failure).ConfigureAwait(false);
    }

    private void HandleData(HsmsDataMessage data)
    {
        var message = data.SecsMessage;
        if (message.SessionId != _options.SessionId)
        {
            _diagnostics.Emit(new(SecsDiagnosticKind.ProtocolError, $"Data Session ID {message.SessionId.Value} does not match configured Session ID {_options.SessionId.Value}.", _stateMachine.State));
            return;
        }
        if (message.Function.IsSecondary || message.Function.Value == 0)
        {
            var status = _transactions.TryComplete(message);
            if (status is SecsTransactionCompletionStatus.UnknownSystemBytes or SecsTransactionCompletionStatus.InvalidCorrelation)
                BackgroundTaskObserver.Observe(SendRejectBestEffortAsync(data.Header, HsmsRejectReason.TransactionNotOpen), "unmatched data response reject");
            return;
        }
        var handlers = MessageReceived;
        if (handlers is null) return;
        foreach (EventHandler<SecsMessage> handler in handlers.GetInvocationList())
        {
            try { handler(this, message); }
            catch (Exception exception)
            {
                _diagnostics.Emit(new(SecsDiagnosticKind.ApplicationError, $"SECS message handler failed: {exception.Message}", _stateMachine.State));
            }
        }
    }

    private async Task SendFrameAsync(HsmsMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var stream = _stream ?? throw new IOException("HSMS stream is not connected.");
        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _codec.WriteAsync(stream, message, cancellationToken).ConfigureAwait(false);
            _diagnostics.Emit(new(SecsDiagnosticKind.FrameSent, "HSMS frame sent.", _stateMachine.State, _codec.Encode(message).Length));
        }
        finally { _sendGate.Release(); }
    }

    private async Task SendRejectBestEffortAsync(HsmsHeader rejected, HsmsRejectReason reason)
    {
        try
        {
            var type = reason == HsmsRejectReason.UnsupportedPType ? rejected.PType : rejected.SType;
            var reject = new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.RejectRequest, rejected.SystemBytes, type, (byte)reason, rejected.SessionId));
            await SendFrameAsync(reject, _connectionCancellation?.Token ?? CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or SocketException or OperationCanceledException or ObjectDisposedException)
        {
            _diagnostics.Emit(new(SecsDiagnosticKind.ProtocolError, $"Reject could not be sent: {exception.Message}", _stateMachine.State));
        }
    }

    private void StartT7()
    {
        CancelT7();
        var connectionToken = _connectionCancellation?.Token ?? CancellationToken.None;
        _t7Cancellation = CancellationTokenSource.CreateLinkedTokenSource(connectionToken);
        BackgroundTaskObserver.Observe(MonitorT7Async(_t7Cancellation.Token), "T7 monitor");
    }

    private async Task MonitorT7Async(CancellationToken cancellationToken)
    {
        try { await _timers.WaitForExpirationAsync(HsmsTimerKind.T7, cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return; }
        catch (HsmsTimerExpiredException exception) { await FailConnectionAsync(exception).ConfigureAwait(false); }
    }

    private void CancelT7()
    {
        var cancellation = Interlocked.Exchange(ref _t7Cancellation, null);
        if (cancellation is null) return;
        cancellation.Cancel(); cancellation.Dispose();
    }

    private async Task ConnectionEndedAsync(Exception? failure)
    {
        await _lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (State is ConnectionState.Disconnected or ConnectionState.Disconnecting) return;
            CancelConnection(); CleanupSocket(); _stateMachine.OnTcpDisconnected();
            var error = failure ?? new IOException("Remote HSMS endpoint closed the connection.");
            _transactions.AbortAll(error); _controlTransactions.AbortAll(error);
            SetConnectionState(failure is null ? ConnectionState.Disconnected : ConnectionState.Faulted);
            _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, failure?.Message ?? "Remote HSMS endpoint closed the connection.", _stateMachine.State));
            if (_options.AutoReconnect && Volatile.Read(ref _disposed) == 0 && (_reconnectLoop is null || _reconnectLoop.IsCompleted))
            {
                _reconnectLoop = ReconnectLoopAsync(_disposeCancellation.Token);
                BackgroundTaskObserver.Observe(_reconnectLoop, "HSMS reconnect loop");
            }
        }
        finally { _lifecycleGate.Release(); }
    }

    private async Task FailConnectionAsync(Exception failure)
    {
        _diagnostics.Emit(new(SecsDiagnosticKind.Timeout, failure.Message, _stateMachine.State));
        await ConnectionEndedAsync(failure).ConfigureAwait(false);
    }

    private async Task ReconnectLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try { await _timers.WaitForExpirationAsync(HsmsTimerKind.T5, cancellationToken).ConfigureAwait(false); }
            catch (HsmsTimerExpiredException)
            {
                // T5 expiration is the signal that permits the next connection attempt.
            }
            try { await ConnectAsync(cancellationToken).ConfigureAwait(false); return; }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return; }
            catch (SocketException exception) { _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, $"Reconnect failed: {exception.Message}.")); }
            catch (IOException exception) { _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, $"Reconnect failed: {exception.Message}.")); }
        }
    }

    private void CancelConnection()
    {
        CancelT7();
        var cancellation = Interlocked.Exchange(ref _connectionCancellation, null);
        if (cancellation is null) return;
        cancellation.Cancel(); cancellation.Dispose();
    }

    private void CleanupSocket()
    {
        _listener?.Stop(); _listener = null;
        _stream?.Dispose(); _stream = null;
        _client?.Dispose(); _client = null;
        _receiveLoop = null;
    }

    private static IPAddress ResolveBindAddress(string host)
    {
        if (host is "0.0.0.0" or "*") return IPAddress.Any;
        if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase)) return IPAddress.Loopback;
        return IPAddress.TryParse(host, out var address) ? address : throw new ArgumentException($"Passive HSMS host '{host}' must be a bindable IP address.", nameof(host));
    }

    private void SetConnectionState(ConnectionState state) => Interlocked.Exchange(ref _connectionState, (int)state);
    private static bool IsControlResponse(HsmsSType type) => type is HsmsSType.SelectResponse or HsmsSType.DeselectResponse or HsmsSType.LinktestResponse;
    private static void ObserveIfFaulted(Task task) { if (task.IsFaulted) _ = task.Exception; }
    private void ValidateSessionId(SecsMessage message)
    {
        if (message.SessionId != _options.SessionId)
            throw new ArgumentException($"Message Session ID {message.SessionId.Value} does not match configured Session ID {_options.SessionId.Value}.", nameof(message));
    }
    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
