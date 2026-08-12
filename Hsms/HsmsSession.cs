using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
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
using Dreamine.Secs.Com.Dispatching;
using Dreamine.Secs.Com.Transactions;

namespace Dreamine.Secs.Com.Hsms;

/// <summary>\if KO <para>단일 HSMS Active 또는 Passive TCP 세션을 합성으로 구현합니다.</para> \endif \if EN <para>Implements one active or passive HSMS TCP session through composition.</para> \endif</summary>
public sealed class HsmsSession : ISecsMessageSession
{
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(2);
    private static readonly AsyncLocal<HsmsSession?> CurrentDisconnectOwner = new();
    private static readonly AsyncLocal<HsmsSession?> CurrentReconnectOwner = new();
    private static readonly AsyncLocal<RunContext?> CurrentConnectRun = new();
    private static readonly AsyncLocal<RunContext?> CurrentReceiveRun = new();
    private static readonly AsyncLocal<RunContext?> CurrentT7Run = new();
    [ThreadStatic]
    private static HsmsSession? CurrentMessageCallbackOwner;
    private readonly HsmsSessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly SecsSessionDiagnosticHub _diagnosticHub;
    private readonly ISecsDiagnosticSink _diagnostics;
    private readonly SecsPrimaryDispatcher _primaryDispatcher;
    private readonly HsmsFrameCodec _codec;
    private readonly HsmsStateMachine _stateMachine;
    private readonly HsmsTimerScheduler _timers;
    private readonly SecsSystemBytesGenerator _systemBytes;
    private readonly SecsTransactionManager _transactions;
    private readonly HsmsControlTransactionManager _controlTransactions;
    private readonly object _runGate = new();
    private readonly object _disposeGate = new();
    private readonly object _workerGate = new();
    private readonly object _stateEventGate = new();
    private readonly HashSet<Task> _workers = new();
    private readonly List<Exception> _shutdownWorkerFailures = new();
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly CancellationTokenSource _disposeCancellation = new();
    private readonly ConcurrentQueue<SecsSessionStateChangedEventArgs> _stateChanges = new();
    private readonly Guid _sessionInstanceId = Guid.NewGuid();
    private readonly Channel<HsmsWireObservation>? _wireObservationChannel;
    private readonly HsmsWireObservationOptions? _wireObservationOptions;
    private TcpClient? _client;
    private TcpListener? _listener;
    private NetworkStream? _stream;
    private CancellationTokenSource? _connectionCancellation;
    private RunContext? _currentRun;
    private RunContext? _connectingRun;
    private RunContext? _connectedRun;
    private Task? _disposeTask;
    private int _connectionState = (int)ConnectionState.Disconnected;
    private int _disposed;
    private int _wireObservationReaderActive;
    private int _lifetimeCleanupStarted;
    private bool _stateEventDraining;
    private long _wireObservationSequence;
    private long _wireObservationDrops;
    private long _connectionEpoch;
    private long _selectedTransitionGeneration;

    /// <summary>\if KO 설정, 시간 및 진단 경계로 세션을 만듭니다. \endif \if EN Creates a session with settings, time, and diagnostic boundaries. \endif</summary>
    /// <param name="options">\if KO 세션 설정입니다. \endif \if EN Session settings. \endif</param><param name="timeProvider">\if KO 시간 공급자입니다. \endif \if EN Time provider. \endif</param><param name="diagnostics">\if KO 진단 sink입니다. \endif \if EN Diagnostic sink. \endif</param>
    public HsmsSession(HsmsSessionOptions options, TimeProvider? timeProvider = null, ISecsDiagnosticSink? diagnostics = null)
    {
        ArgumentNullException.ThrowIfNull(options); options.Validate();
        if (options.Mode == SecsConnectionMode.Unspecified) throw new ArgumentException("HSMS connection mode must be Active or Passive.", nameof(options));
        if (options.Role == SecsRole.Unspecified) throw new ArgumentException("SECS role must be Host or Equipment.", nameof(options));
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _diagnosticHub = new SecsSessionDiagnosticHub(this, diagnostics);
        _diagnostics = SafeSecsDiagnosticSink.Wrap(_diagnosticHub);
        var maximumMessageLength = Math.Max(
            1,
            options.MaximumMessageLength ?? options.MaximumFrameLength - HsmsFrameCodec.HeaderLength);
        _codec = new HsmsFrameCodec(
            new HsmsFrameCodecOptions { MaximumFrameLength = options.MaximumFrameLength },
            new SecsItemCodec(new SecsItemCodecOptions
            {
                MaximumMessageLength = maximumMessageLength,
                MaximumNestingDepth = options.MaximumNestingDepth,
                MaximumListItemCount = options.MaximumListItemCount
            }));
        _stateMachine = new HsmsStateMachine(_diagnostics);
        _timers = new HsmsTimerScheduler(options.Timers, _timeProvider);
        _systemBytes = new SecsSystemBytesGenerator();
        _transactions = new SecsTransactionManager(_timeProvider, _systemBytes, _diagnostics);
        _controlTransactions = new HsmsControlTransactionManager(_timeProvider, _diagnostics);
        _primaryDispatcher = new SecsPrimaryDispatcher(options.PrimaryDispatcher, _diagnostics);
        _wireObservationOptions = options.WireObservation is { } wireObservation
            ? new HsmsWireObservationOptions
            {
                QueueCapacity = wireObservation.QueueCapacity,
                MaximumCapturedBytes = wireObservation.MaximumCapturedBytes,
                DefaultCaptureMode = wireObservation.DefaultCaptureMode,
                CaptureRules = wireObservation.CaptureRules.ToArray()
            }
            : null;
        if (_wireObservationOptions is not null)
        {
            _wireObservationChannel = Channel.CreateBounded<HsmsWireObservation>(new BoundedChannelOptions(_wireObservationOptions.QueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
        }
    }

    /// <inheritdoc />
    public string ProviderKey => SecsProviderKeys.Dreamine;

    /// <inheritdoc />
    public ConnectionState State => (ConnectionState)Volatile.Read(ref _connectionState);

    /// <summary>\if KO 현재 HSMS protocol 상태입니다. \endif \if EN Gets the current HSMS protocol state. \endif</summary>
    public HsmsConnectionState HsmsState => _stateMachine.State;

    /// <inheritdoc />
    public SecsConnectionIdentity ConnectionIdentity => CreateConnectionIdentity(Volatile.Read(ref _connectionEpoch));

    /// <inheritdoc />
    public ISecsPrimaryDispatcher PrimaryDispatcher => _primaryDispatcher;

    /// <inheritdoc />
    public bool IsWireObservationEnabled => _wireObservationChannel is not null;

    /// <inheritdoc />
    public long DroppedWireObservationCount => Volatile.Read(ref _wireObservationDrops);

    /// <inheritdoc />
    public async IAsyncEnumerable<HsmsWireObservation> ReadWireObservationsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = _wireObservationChannel;
        if (channel is null) yield break;
        if (Interlocked.CompareExchange(ref _wireObservationReaderActive, 1, 0) != 0)
            throw new InvalidOperationException("Only one active HSMS wire-observation reader is supported.");
        try
        {
            await foreach (var observation in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return observation;
        }
        finally
        {
            Volatile.Write(ref _wireObservationReaderActive, 0);
        }
    }

    /// <summary>\if KO 선택된 세션에서 상위 계층으로 전달되는 SECS 메시지 이벤트입니다. \endif \if EN Raised for SECS messages accepted in a selected session. \endif</summary>
    public event EventHandler<SecsMessage>? MessageReceived;

    /// <inheritdoc />
    public event EventHandler<SecsDiagnosticEvent>? DiagnosticReceived
    {
        add => _diagnosticHub.DiagnosticReceived += value;
        remove => _diagnosticHub.DiagnosticReceived -= value;
    }

    /// <inheritdoc />
    public event EventHandler<SecsSessionStateChangedEventArgs>? StateChanged;

    /// <inheritdoc />
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        var (run, operation) = AcquireRunOperation();
        var previousConnectRun = CurrentConnectRun.Value;
        CurrentConnectRun.Value = run;
        using (operation)
        {
            try
            {
                var reconnect = GetReconnectTask(run);
                if (reconnect is { IsCompleted: false })
                {
                    PublishPendingPassiveListen(run);
                    EmitStateTransitions();
                    await reconnect.WaitAsync(cancellationToken).ConfigureAwait(false);
                    EnsureConnectedRun(run);
                    return;
                }
                await ConnectCoreAsync(run, cancellationToken, isReconnect: false).ConfigureAwait(false);
            }
            finally { CurrentConnectRun.Value = previousConnectRun; }
        }
    }

    private async Task ConnectCoreAsync(RunContext run, CancellationToken cancellationToken, bool isReconnect)
    {
        var deferredDiagnostics = new List<SecsDiagnosticEvent>();
        TaskCompletionSource? ownedAttempt = null;
        TaskCompletionSource? receiveStart = null;
        var receiveCanStart = false;
        TcpClient? connectingClient = null;
        ThrowIfDisposed();
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, run.Cancellation.Token, _disposeCancellation.Token);
        var operationToken = operationCancellation.Token;
        await _lifecycleGate.WaitAsync(operationToken).ConfigureAwait(false);
        var lifecycleHeld = true;
        try
        {
            operationToken.ThrowIfCancellationRequested();
            if (!IsCurrentRun(run)) throw new OperationCanceledException(run.Cancellation.Token);
            if (!isReconnect && GetReconnectTask(run) is { IsCompleted: false } pendingReconnect)
            {
                PublishPendingPassiveListen(run);
                _lifecycleGate.Release();
                lifecycleHeld = false;
                EmitStateTransitions();
                await pendingReconnect.WaitAsync(operationToken).ConfigureAwait(false);
                EnsureConnectedRun(run);
                return;
            }
            if (State == ConnectionState.Connected && ReferenceEquals(_connectedRun, run)) return;
            if (State == ConnectionState.Connected)
            {
                DetachedConnectionResources resources;
                SecsDiagnosticEvent? previousStateDiagnostic;
                lock (_runGate)
                {
                    resources = DetachConnectionResourcesLocked(_connectedRun, clearAll: true);
                    var error = new IOException("HSMS connection was superseded by a newer connect run.");
                    _transactions.AbortAll(error);
                    _controlTransactions.AbortAll(error);
                    SetConnectionState(ConnectionState.Disconnected);
                    previousStateDiagnostic = ApplyTcpDisconnectedLocked();
                }
                DisposeDetachedConnectionResources(resources);
                Add(deferredDiagnostics, previousStateDiagnostic);
                deferredDiagnostics.Add(new(SecsDiagnosticKind.ConnectionClosed, "HSMS previous run was closed before reconnecting.", _stateMachine.State));
            }
            if (State is ConnectionState.Listening or ConnectionState.Connecting)
            {
                var transitionOwner = GetConnectingRun();
                if (ReferenceEquals(transitionOwner, run))
                {
                    var pendingAttempt = run.ConnectAttempt?.Task ??
                        throw new InvalidOperationException("HSMS connecting state has no owned connection attempt.");
                    _lifecycleGate.Release();
                    lifecycleHeld = false;
                    await pendingAttempt.WaitAsync(operationToken).ConfigureAwait(false);
                    EnsureConnectedRun(run);
                    return;
                }
                DetachedConnectionResources resources;
                lock (_runGate)
                {
                    resources = DetachConnectionResourcesLocked(transitionOwner, clearAll: true);
                    SetConnectionState(ConnectionState.Disconnected);
                }
                DisposeDetachedConnectionResources(resources);
            }
            ownedAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!TryOwnConnectAttempt(run, ownedAttempt, isReconnect))
                throw new OperationCanceledException("The HSMS connection run was stopped before its attempt started.", run.Cancellation.Token);
            deferredDiagnostics.Add(new(SecsDiagnosticKind.ConnectionAttempt, $"HSMS {_options.Mode} connection attempt to {_options.Host}:{_options.Port}."));
            _lifecycleGate.Release();
            lifecycleHeld = false;
            Emit(deferredDiagnostics);
            operationToken.ThrowIfCancellationRequested();
            if (!IsCurrentRun(run)) throw new OperationCanceledException(operationToken);
            await _lifecycleGate.WaitAsync(operationToken).ConfigureAwait(false);
            lifecycleHeld = true;
            operationToken.ThrowIfCancellationRequested();
            if (!IsCurrentRun(run)) throw new OperationCanceledException(operationToken);
            connectingClient = _options.Mode == SecsConnectionMode.Active
                ? await ConnectActiveAsync(operationToken).ConfigureAwait(false)
                : await AcceptPassiveAsync(operationToken).ConfigureAwait(false);
            connectingClient.NoDelay = true;
            var connectedStream = connectingClient.GetStream();
            if (!TryPublishConnectedClientCore(run, connectingClient, connectedStream, operationToken, out var stateDiagnostic))
                throw new OperationCanceledException(operationToken);
            connectingClient = null;
            Add(deferredDiagnostics, stateDiagnostic);
            deferredDiagnostics.Add(new(SecsDiagnosticKind.ConnectionEstablished, "HSMS TCP connection established.", _stateMachine.State));
            var connectionEpoch = run.ConnectionEpoch;
            receiveStart = new(TaskCreationOptions.RunContinuationsAsynchronously);
            run.ReceiveLoop = ReceiveLoopAsync(
                run,
                connectionEpoch,
                connectedStream,
                receiveStart.Task,
                _connectionCancellation!.Token);
            TrackWorker(run.ReceiveLoop, "HSMS receive loop");
            if (!TryCompleteConnectAttempt(run, ownedAttempt))
                throw new OperationCanceledException("The HSMS connection run was stopped before connection completed.", run.Cancellation.Token);
            receiveCanStart = true;
        }
        catch (Exception exception)
        {
            connectingClient?.Dispose();
            if (ownedAttempt is not null)
            {
                if (!lifecycleHeld)
                {
                    await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                    lifecycleHeld = true;
                }
                if (TryClearOwnedConnectAttempt(run, ownedAttempt))
                {
                    DetachedConnectionResources resources = default;
                    SecsDiagnosticEvent? stateDiagnostic = null;
                    var ownedResources = false;
                    var stopped = false;
                    lock (_runGate)
                    {
                        ownedResources = ReferenceEquals(_connectingRun, run) || ReferenceEquals(_connectedRun, run);
                        if (ownedResources)
                        {
                            var wasPublished = ReferenceEquals(_connectedRun, run);
                            resources = DetachConnectionResourcesLocked(run);
                            stopped = run.Cancellation.IsCancellationRequested || Volatile.Read(ref _disposed) != 0;
                            if (wasPublished)
                            {
                                var error = new IOException("HSMS connection attempt ended before publication completed.", exception);
                                _transactions.AbortAll(error);
                                _controlTransactions.AbortAll(error);
                                stateDiagnostic = ApplyTcpDisconnectedLocked();
                            }
                            SetConnectionState(stopped ? ConnectionState.Disconnected : ConnectionState.Faulted);
                        }
                    }
                    DisposeDetachedConnectionResources(resources);
                    if (ownedResources)
                    {
                        Add(deferredDiagnostics, stateDiagnostic);
                        if (!isReconnect)
                            deferredDiagnostics.Add(new(SecsDiagnosticKind.ConnectionClosed, stopped
                                ? "HSMS connection attempt stopped."
                                : $"HSMS connection attempt failed: {exception.Message}."));
                        if (!stopped && !isReconnect && _options.Mode == SecsConnectionMode.Active && _options.AutoReconnect &&
                            exception is not OperationCanceledException && !HasDifferentCurrentRun(run))
                            EnsureReconnect(run);
                    }
                    ownedAttempt.TrySetException(exception);
                    ObserveIfFaulted(ownedAttempt.Task);
                }
            }
            throw;
        }
        finally
        {
            if (lifecycleHeld) _lifecycleGate.Release();
            try { Emit(deferredDiagnostics); }
            finally
            {
                if (receiveStart is not null)
                {
                    if (receiveCanStart) receiveStart.TrySetResult();
                    else receiveStart.TrySetCanceled();
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        var run = StopCurrentRun();
        var cleanup = DisconnectCoreAsync(run, CancellationToken.None, disposing: false);
        TrackWorker(cleanup, "HSMS explicit disconnect cleanup");
        await cleanup.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task DisconnectCoreAsync(
        RunContext? run,
        CancellationToken cancellationToken,
        bool disposing,
        List<SecsDiagnosticEvent>? deferredUntilDisposeCompletion = null)
    {
        var deferredDiagnostics = new List<SecsDiagnosticEvent>();
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        Task? receiveLoop;
        DetachedConnectionResources resources = default;
        try
        {
            lock (_runGate)
            {
                var targetRun = run ?? (disposing ? _connectedRun ?? _connectingRun : null);
                receiveLoop = targetRun?.ReceiveLoop;
                var hasNewerRun = _currentRun is not null && !ReferenceEquals(_currentRun, targetRun);
                var ownsConnection = targetRun is not null &&
                    (ReferenceEquals(_connectingRun, targetRun) || ReferenceEquals(_connectedRun, targetRun));
                if (ownsConnection || (!hasNewerRun && (State != ConnectionState.Disconnected || disposing)))
                {
                    if (State != ConnectionState.Disconnected) SetConnectionState(ConnectionState.Disconnecting);
                    resources = DetachConnectionResourcesLocked(targetRun, clearAll: !ownsConnection);
                    Add(deferredDiagnostics, ApplyTcpDisconnectedLocked());
                    var error = new IOException("HSMS connection closed.");
                    _transactions.AbortAll(error);
                    _controlTransactions.AbortAll(error);
                    SetConnectionState(ConnectionState.Disconnected);
                    deferredDiagnostics.Add(new(SecsDiagnosticKind.ConnectionClosed, "HSMS connection closed normally.", _stateMachine.State));
                }
            }
            DisposeDetachedConnectionResources(resources);
        }
        finally { _lifecycleGate.Release(); }
        if (deferredUntilDisposeCompletion is not null)
        {
            deferredUntilDisposeCompletion.AddRange(deferredDiagnostics);
            deferredDiagnostics.Clear();
        }
        else
        {
            var previousDisconnectOwner = CurrentDisconnectOwner.Value;
            CurrentDisconnectOwner.Value = this;
            try { Emit(deferredDiagnostics); }
            finally { CurrentDisconnectOwner.Value = previousDisconnectOwner; }
        }

        if (run is not null && IsInSynchronousProtocolCallback() &&
            (ReferenceEquals(CurrentConnectRun.Value, run) ||
             ReferenceEquals(CurrentReceiveRun.Value, run) ||
             ReferenceEquals(CurrentReconnectOwner.Value, this)))
        {
            run.RequestDisposal();
            return;
        }
        try
        {
            if (receiveLoop is not null)
            {
                await receiveLoop.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            var reconnect = run?.ReconnectTask;
            if (reconnect is not null)
            {
                try { await reconnect.WaitAsync(cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) when (run!.Cancellation.IsCancellationRequested)
                {
                    // An explicit stop owns this cancellation.
                }
            }
        }
        finally
        {
            if (run is not null)
            {
                await run.WaitForOperationsAsync(cancellationToken).ConfigureAwait(false);
                run.Dispose();
            }
        }
    }

    /// <summary>\if KO Select transaction을 수행하고 선택 상태가 될 때까지 기다립니다. \endif \if EN Performs a Select transaction and waits until selected. \endif</summary>
    /// <param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task SelectAsync(CancellationToken cancellationToken = default)
    {
        ConnectionSnapshot connection;
        HsmsControlMessage request;
        Task<HsmsControlMessage> responseTask;
        lock (_runGate)
        {
            connection = GetConnectedRunSnapshotLocked();
            try { request = _stateMachine.CreateSelectRequest(_transactions.AllocateSystemBytes()); }
            catch (HsmsStateException) when (_stateMachine.State == HsmsConnectionState.Selected)
            {
                return;
            }
            responseTask = _controlTransactions.RegisterDeferred(request, cancellationToken);
        }
        try
        {
            await SendFrameAsync(request, connection, cancellationToken, () =>
            {
                _ = _controlTransactions.StartTimeout(request.Header.SystemBytes, _options.Timers.T6, cancellationToken);
            }).ConfigureAwait(false);
            var response = await responseTask.ConfigureAwait(false);
            if (response.Header.HeaderByte3 != (byte)HsmsSelectStatus.Success && response.Header.HeaderByte3 != (byte)HsmsSelectStatus.AlreadyActive)
                throw new SecsProtocolException(SecsValidationCode.InvalidMessage, $"Select failed with status {response.Header.HeaderByte3}.");
        }
        catch (HsmsTimerExpiredException) { await FailConnectionAsync(connection.Run, connection.Epoch, new HsmsTimerExpiredException("T6", _options.Timers.T6)).ConfigureAwait(false); throw; }
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
        ConnectionSnapshot connection;
        HsmsControlMessage request;
        Task<HsmsControlMessage> responseTask;
        lock (_runGate)
        {
            connection = GetConnectedRunSnapshotLocked();
            request = _stateMachine.CreateDeselectRequest(_transactions.AllocateSystemBytes());
            responseTask = _controlTransactions.RegisterDeferred(request, cancellationToken);
        }
        try
        {
            await SendFrameAsync(request, connection, cancellationToken, () =>
            {
                _ = _controlTransactions.StartTimeout(request.Header.SystemBytes, _options.Timers.T6, cancellationToken);
            }).ConfigureAwait(false);
            var response = await responseTask.ConfigureAwait(false);
            if (response.Header.HeaderByte3 != (byte)HsmsDeselectStatus.Success)
                throw new SecsProtocolException(SecsValidationCode.InvalidMessage, $"Deselect failed with status {response.Header.HeaderByte3}.");
        }
        catch (HsmsTimerExpiredException) { await FailConnectionAsync(connection.Run, connection.Epoch, new HsmsTimerExpiredException("T6", _options.Timers.T6)).ConfigureAwait(false); throw; }
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
        ConnectionSnapshot connection;
        HsmsControlMessage request;
        Task<HsmsControlMessage> responseTask;
        lock (_runGate)
        {
            connection = GetConnectedRunSnapshotLocked();
            request = _stateMachine.CreateLinktestRequest(_transactions.AllocateSystemBytes());
            responseTask = _controlTransactions.RegisterDeferred(request, cancellationToken);
        }
        try
        {
            await SendFrameAsync(request, connection, cancellationToken, () =>
            {
                _ = _controlTransactions.StartTimeout(request.Header.SystemBytes, _options.Timers.T6, cancellationToken);
            }).ConfigureAwait(false);
            _ = await responseTask.ConfigureAwait(false);
        }
        catch (HsmsTimerExpiredException) { await FailConnectionAsync(connection.Run, connection.Epoch, new HsmsTimerExpiredException("T6", _options.Timers.T6)).ConfigureAwait(false); throw; }
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
        var systemBytes = _transactions.AllocateSystemBytes();
        SelectedConnectionSnapshot selection;
        lock (_runGate)
        {
            selection = GetSelectedConnectionSnapshotLocked(nameof(SeparateAsync));
            selection.Connection.Run.IntentionalStopRequested = true;
        }

        Exception? sendFailure = null;
        byte[]? sentFrame = null;
        SecsDiagnosticEvent? stateDiagnostic = null;
        var sendGateHeld = false;
        try
        {
            await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            sendGateHeld = true;
            HsmsControlMessage request;
            lock (_runGate)
            {
                ValidateSelectedConnectionLocked(selection);
                var before = _stateMachine.State;
                request = _stateMachine.CreateSeparateRequestDeferred(systemBytes, out stateDiagnostic);
                RecordSelectedTransitionLocked(before, _stateMachine.State);
            }
            var frame = _codec.Encode(request);
            await HsmsFrameCodec.WriteEncodedAsync(selection.Connection.Stream, frame, cancellationToken).ConfigureAwait(false);
            ObserveWireFrame(frame, HsmsWireDirection.Outbound, selection.Connection.Epoch);
            sentFrame = frame;
        }
        catch (Exception exception) { sendFailure = exception; }
        finally
        {
            if (sendGateHeld) _sendGate.Release();
        }
        if (stateDiagnostic is not null) _diagnostics.Emit(stateDiagnostic);
        EmitStateTransitions();
        if (sentFrame is not null)
            _diagnostics.Emit(new(SecsDiagnosticKind.FrameSent, "HSMS frame sent.", _stateMachine.State, sentFrame.Length));

        try { await StopIntentionalRunAsync(selection.Connection.Run).ConfigureAwait(false); }
        catch (Exception cleanupFailure) when (sendFailure is not null)
        {
            _diagnostics.Emit(new(
                SecsDiagnosticKind.ApplicationError,
                $"HSMS cleanup after a failed Separate request also failed: {cleanupFailure.Message}",
                _stateMachine.State));
        }
        if (sendFailure is not null) ExceptionDispatchInfo.Capture(sendFailure).Throw();
    }

    /// <summary>\if KO 선택된 세션에서 응답을 기다리지 않는 SECS 메시지를 보냅니다. \endif \if EN Sends a SECS message without waiting for a reply in a selected session. \endif</summary>
    /// <param name="message">\if KO 메시지입니다. \endif \if EN Message. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param>
    public async Task SendAsync(SecsMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ValidateSessionId(message);
        if (message.ReplyExpected) throw new ArgumentException("Use SendPrimaryAsync for a W-bit primary.", nameof(message));
        SelectedConnectionSnapshot selection;
        lock (_runGate) selection = GetSelectedConnectionSnapshotLocked(nameof(SendAsync));
        await SendSelectedFrameAsync(new HsmsDataMessage(message), selection, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task SendAsync(
        SecsStream stream,
        SecsFunction function,
        SecsItem? item = null,
        CancellationToken cancellationToken = default)
    {
        _ = new SecsDialogueDefinition(stream, function);
        var message = new SecsMessage(
            _options.SessionId,
            stream,
            function,
            replyExpected: false,
            AllocateSystemBytes(),
            item);
        return SendAsync(message, cancellationToken);
    }

    /// <summary>\if KO W-bit primary를 보내고 T3 내의 secondary를 기다립니다. \endif \if EN Sends a W-bit primary and waits for its secondary within T3. \endif</summary>
    /// <param name="message">\if KO primary입니다. \endif \if EN Primary. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param><returns>\if KO secondary입니다. \endif \if EN Secondary. \endif</returns>
    public async Task<SecsMessage> SendPrimaryAsync(SecsMessage message, CancellationToken cancellationToken = default)
        => await SendPrimaryCoreAsync(message, expectedSecondary: null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public Task<SecsMessage> RequestAsync(
        SecsDialogueDefinition dialogue,
        SecsItem? item = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dialogue);
        if (dialogue.SecondaryFunction is not { } expectedSecondary)
            throw new ArgumentException("A safe request requires a W1 dialogue with its adjacent normal secondary.", nameof(dialogue));
        var message = new SecsMessage(
            _options.SessionId,
            dialogue.Stream,
            dialogue.PrimaryFunction,
            replyExpected: true,
            AllocateSystemBytes(),
            item);
        return SendPrimaryCoreAsync(message, expectedSecondary, cancellationToken);
    }

    private async Task<SecsMessage> SendPrimaryCoreAsync(
        SecsMessage message,
        SecsFunction? expectedSecondary,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ValidateSessionId(message);
        SelectedConnectionSnapshot selection;
        lock (_runGate) selection = GetSelectedConnectionSnapshotLocked(nameof(SendPrimaryAsync));
        Task<SecsMessage>? response = null;
        try
        {
            await SendSelectedFrameAsync(
                new HsmsDataMessage(message),
                selection,
                cancellationToken,
                () => response = expectedSecondary is { } expected
                    ? _transactions.RegisterPrimaryDeferred(message, expected, cancellationToken)
                    : _transactions.RegisterPrimaryDeferred(message, cancellationToken),
                () => _ = _transactions.StartTimeout(message.SystemBytes, _options.Timers.T3, cancellationToken))
                .ConfigureAwait(false);
            _diagnostics.Emit(new(SecsDiagnosticKind.PrimarySent, $"Primary sent with System Bytes 0x{message.SystemBytes.Value:X8}."));
            return await response!.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (response is { IsCompleted: false }) _transactions.TryAbort(message.SystemBytes, exception);
            if (response is not null) ObserveIfFaulted(response);
            throw;
        }
    }

    /// <summary>\if KO 열린 transaction과 충돌하지 않는 System Bytes를 할당합니다. \endif \if EN Allocates System Bytes not colliding with an open transaction. \endif</summary>
    public SecsSystemBytes AllocateSystemBytes() => _transactions.AllocateSystemBytes();

    /// <summary>\if KO 세션과 모든 비동기 자원을 안전하게 해제합니다. \endif \if EN Safely disposes the session and all asynchronous resources. \endif</summary>
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource? owner = null;
        Task disposeTask;
        lock (_disposeGate)
        {
            if (_disposeTask is null)
            {
                owner = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = owner.Task;
            }
            disposeTask = _disposeTask;
        }
        if (owner is not null) _ = CompleteDisposeAsync(owner);
        return new ValueTask(disposeTask);
    }

    private async Task CompleteDisposeAsync(TaskCompletionSource completion)
    {
        var deferredDiagnostics = new List<SecsDiagnosticEvent>();
        try
        {
            await DisposeCoreAsync(deferredDiagnostics).ConfigureAwait(false);
            completion.TrySetResult();
        }
        catch (Exception exception) { completion.TrySetException(exception); }
        var previousDisconnectOwner = CurrentDisconnectOwner.Value;
        CurrentDisconnectOwner.Value = this;
        try { Emit(deferredDiagnostics); }
        finally { CurrentDisconnectOwner.Value = previousDisconnectOwner; }
    }

    private async Task DisposeCoreAsync(List<SecsDiagnosticEvent> deferredDiagnostics)
    {
        var errors = new List<Exception>();
        Interlocked.Exchange(ref _disposed, 1);
        var run = StopCurrentRun();
        var isSynchronousProtocolCallback = IsInSynchronousProtocolCallback();
        var reentrantWorker = isSynchronousProtocolCallback && ReferenceEquals(CurrentReceiveRun.Value, run)
            ? run?.ReceiveLoop
            : isSynchronousProtocolCallback && ReferenceEquals(CurrentReconnectOwner.Value, this) ? run?.ReconnectTask : null;
        var reentrantProtocolCallback = reentrantWorker is not null ||
            (isSynchronousProtocolCallback && ReferenceEquals(CurrentT7Run.Value, run)) ||
            (isSynchronousProtocolCallback && ReferenceEquals(CurrentDisconnectOwner.Value, this));
        _disposeCancellation.Cancel();
        using var shutdown = new CancellationTokenSource(ShutdownTimeout);
        var dispatcherShutdown = _primaryDispatcher.StopAsync(shutdown.Token);
        var disconnect = DisconnectCoreAsync(
            run,
            CancellationToken.None,
            disposing: true,
            deferredUntilDisposeCompletion: deferredDiagnostics);
        try { await dispatcherShutdown.ConfigureAwait(false); }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            errors.Add(new TimeoutException($"SECS Primary dispatcher shutdown did not complete within {ShutdownTimeout}."));
        }
        catch (Exception exception) { errors.Add(exception); }
        var workersDrained = false;
        try { await disconnect.WaitAsync(shutdown.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            errors.Add(new TimeoutException($"HSMS shutdown did not complete within {ShutdownTimeout}."));
        }
        catch (Exception exception) { errors.Add(exception); }

        if (!shutdown.IsCancellationRequested)
        {
            if (reentrantProtocolCallback)
            {
                // A synchronous protocol callback can be executing on an owned worker. Complete the public
                // dispose request now and let cleanup drain that worker after the callback returns.
            }
            else
            {
                try
                {
                    await DrainWorkersAsync().WaitAsync(shutdown.Token).ConfigureAwait(false);
                    workersDrained = true;
                }
                catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
                {
                    errors.Add(new TimeoutException($"HSMS worker drain did not complete within {ShutdownTimeout}."));
                }
                catch (Exception exception)
                {
                    // DrainWorkers reports recorded failures only after its owned-worker set reached zero.
                    workersDrained = true;
                    AddFlattened(errors, exception);
                }
            }
        }
        else if (!errors.Any(exception => exception is TimeoutException))
        {
            errors.Add(new TimeoutException($"HSMS worker drain did not complete within {ShutdownTimeout}."));
        }

        if (!workersDrained)
        {
            var deferredCleanup = DisposeAfterWorkersAsync(disconnect);
            _ = BackgroundTaskObserver.Observe(deferredCleanup, "deferred HSMS worker and lifetime cleanup");
        }
        else
        {
            var transactionDisposal = _transactions.DisposeAsync().AsTask();
            var controlTransactionDisposal = _controlTransactions.DisposeAsync().AsTask();
            var managerShutdown = Task.WhenAll(transactionDisposal, controlTransactionDisposal);
            _ = BackgroundTaskObserver.Observe(managerShutdown, "HSMS transaction-manager shutdown");
            var managersDrained = false;
            try
            {
                await managerShutdown.WaitAsync(shutdown.Token).ConfigureAwait(false);
                managersDrained = true;
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
            {
                if (managerShutdown.IsCompleted)
                {
                    managersDrained = true;
                    CaptureManagerFailures(errors, transactionDisposal, controlTransactionDisposal);
                }
                else
                {
                    errors.Add(new TimeoutException($"HSMS transaction-manager shutdown did not complete within {ShutdownTimeout}."));
                }
            }
            catch (Exception exception)
            {
                managersDrained = true;
                if (!CaptureManagerFailures(errors, transactionDisposal, controlTransactionDisposal))
                    errors.Add(exception);
            }

            if (managersDrained)
            {
                try { FinalizeLifetimeCleanup(); }
                catch (Exception exception) { errors.Add(exception); }
            }
            else
            {
                var deferredCleanup = DisposeLifetimeAfterManagersAsync(managerShutdown);
                _ = BackgroundTaskObserver.Observe(deferredCleanup, "deferred HSMS lifetime cleanup");
            }
        }

        if (errors.Count > 0) throw new AggregateException("One or more HSMS shutdown operations failed.", errors);
    }

    private async Task DisposeAfterWorkersAsync(Task disconnect)
    {
        try { await disconnect.ConfigureAwait(false); }
        catch { _ = disconnect.Exception; }
        try { await DrainWorkersAsync().ConfigureAwait(false); }
        catch { }

        var transactionDisposal = _transactions.DisposeAsync().AsTask();
        var controlTransactionDisposal = _controlTransactions.DisposeAsync().AsTask();
        try { await Task.WhenAll(transactionDisposal, controlTransactionDisposal).ConfigureAwait(false); }
        catch
        {
            _ = transactionDisposal.Exception;
            _ = controlTransactionDisposal.Exception;
        }
        FinalizeLifetimeCleanup();
    }

    private async Task DisposeLifetimeAfterManagersAsync(Task managerShutdown)
    {
        try { await managerShutdown.ConfigureAwait(false); }
        catch { _ = managerShutdown.Exception; }
        FinalizeLifetimeCleanup();
    }

    private void FinalizeLifetimeCleanup()
    {
        if (Interlocked.Exchange(ref _lifetimeCleanupStarted, 1) != 0) return;
        _wireObservationChannel?.Writer.TryComplete();
        _disposeCancellation.Dispose();
    }

    private static void AddFlattened(List<Exception> errors, Exception exception)
    {
        if (exception is AggregateException aggregate)
            errors.AddRange(aggregate.Flatten().InnerExceptions);
        else
            errors.Add(exception);
    }

    private static bool CaptureManagerFailures(List<Exception> errors, params Task[] disposals)
    {
        var captured = false;
        foreach (var disposal in disposals)
        {
            if (disposal.Exception is not { } failure) continue;
            errors.AddRange(failure.Flatten().InnerExceptions);
            captured = true;
        }
        return captured;
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
        var listener = new TcpListener(address, _options.Port);
        _listener = listener;
        listener.Start(1);
        try { return await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false); }
        finally
        {
            listener.Stop();
            if (ReferenceEquals(_listener, listener)) _listener = null;
        }
    }

    private async Task ReceiveLoopAsync(
        RunContext run,
        long connectionEpoch,
        NetworkStream stream,
        Task startSignal,
        CancellationToken cancellationToken)
    {
        using var operation = run.AcquireOperation();
        try { await startSignal.WaitAsync(cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || startSignal.IsCanceled) { return; }
        var previousReceiveRun = CurrentReceiveRun.Value;
        CurrentReceiveRun.Value = run;
        try
        {
            Exception? failure = null;
            var reconnectAfterClose = true;
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var frame = await _codec.ReadRawFrameAsync(stream, _options.Timers.T8, _timeProvider, cancellationToken).ConfigureAwait(false);
                    if (frame is null) break;
                    ObserveWireFrame(frame, HsmsWireDirection.Inbound, connectionEpoch);
                    _diagnostics.Emit(new(SecsDiagnosticKind.FrameReceived, "HSMS frame received.", _stateMachine.State, frame.Length));
                    var message = _codec.Decode(frame);
                    var inboundDiagnostics = new List<SecsDiagnosticEvent>();
                    HsmsProcessingResult? processing = null;
                    SecsMessage? applicationMessage = null;
                    SelectedConnectionSnapshot? applicationSelection = null;
                    SecsConnectionIdentity? applicationIdentity = null;
                    var isCurrent = false;
                    var unmatchedControlResponse = false;
                    var unmatchedDataResponse = false;
                    var controlSerializationHeld = false;
                    if (message is HsmsControlMessage)
                    {
                        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                        controlSerializationHeld = true;
                    }
                    try
                    {
                        lock (_runGate)
                        {
                            if (IsCurrentConnectionLocked(run, connectionEpoch, stream))
                            {
                                isCurrent = true;
                                var before = _stateMachine.State;
                                SecsDiagnosticEvent? stateDiagnostic = null;
                                HsmsProcessingResult? result = null;
                                if (message is HsmsControlMessage response && IsControlResponse(response.SType))
                                {
                                    if (!_controlTransactions.TryCompleteAfter(response, () =>
                                        result = _stateMachine.ProcessDeferred(response, out stateDiagnostic)))
                                        unmatchedControlResponse = true;
                                }
                                else
                                {
                                    result = _stateMachine.ProcessDeferred(message, out stateDiagnostic);
                                }
                                Add(inboundDiagnostics, stateDiagnostic);
                                if (!unmatchedControlResponse)
                                {
                                    processing = result ?? throw new InvalidOperationException("HSMS control response processing did not produce a result.");
                                    var after = _stateMachine.State;
                                    RecordSelectedTransitionLocked(before, after);
                                    if (before != after)
                                    {
                                        if (after == HsmsConnectionState.Selected) CancelT7(run);
                                        else if (after == HsmsConnectionState.ConnectedNotSelected) StartT7(run);
                                    }
                                    if (processing.Accepted && message is HsmsDataMessage data)
                                    {
                                        var secsMessage = data.SecsMessage;
                                        if (secsMessage.SessionId != _options.SessionId)
                                        {
                                            inboundDiagnostics.Add(new(
                                                SecsDiagnosticKind.ProtocolError,
                                                $"Data Session ID {secsMessage.SessionId.Value} does not match configured Session ID {_options.SessionId.Value}.",
                                                _stateMachine.State));
                                        }
                                        else if (secsMessage.Function.IsSecondary || secsMessage.Function.Value == 0)
                                        {
                                            var status = _transactions.TryCompleteDeferred(secsMessage, out var transactionDiagnostic);
                                            Add(inboundDiagnostics, transactionDiagnostic);
                                            unmatchedDataResponse = status is SecsTransactionCompletionStatus.UnknownSystemBytes or SecsTransactionCompletionStatus.InvalidCorrelation;
                                        }
                                        else
                                        {
                                            applicationMessage = secsMessage;
                                            applicationSelection = new(
                                                new ConnectionSnapshot(run, connectionEpoch, stream),
                                                _selectedTransitionGeneration);
                                            applicationIdentity = CreateConnectionIdentity(connectionEpoch);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    finally
                    {
                        if (controlSerializationHeld) _sendGate.Release();
                    }
                    Emit(inboundDiagnostics);
                    if (!isCurrent) break;
                    if (unmatchedControlResponse)
                    {
                        TrackWorker(
                            SendRejectBestEffortAsync(message.Header, HsmsRejectReason.TransactionNotOpen, run, connectionEpoch, stream, cancellationToken),
                            "unmatched control response reject");
                        continue;
                    }
                    if (processing!.Response is not null &&
                        !await TrySendFrameAsync(processing.Response, run, connectionEpoch, stream, cancellationToken).ConfigureAwait(false)) break;
                    if (unmatchedDataResponse)
                        TrackWorker(
                            SendRejectBestEffortAsync(message.Header, HsmsRejectReason.TransactionNotOpen, run, connectionEpoch, stream, cancellationToken),
                            "unmatched data response reject");
                    if (applicationMessage is not null && applicationSelection is { } selected && applicationIdentity is not null)
                    {
                        var claim = _primaryDispatcher.TryDispatch(
                            applicationMessage,
                            applicationIdentity,
                            cancellationToken,
                            (dialogue, primary, item, token) => ReplyToPrimaryAsync(selected, dialogue, primary, item, token));
                        if (claim == PrimaryDispatchClaim.Unclaimed) DispatchMessage(applicationMessage);
                    }
                    if (processing.CloseConnection)
                    {
                        reconnectAfterClose = false;
                        break;
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return; }
            catch (Exception exception) { failure = exception; }
            await ConnectionEndedAsync(run, connectionEpoch, failure, allowReconnect: reconnectAfterClose).ConfigureAwait(false);
        }
        finally { CurrentReceiveRun.Value = previousReceiveRun; }
    }

    private void DispatchMessage(SecsMessage message)
    {
        var handlers = MessageReceived;
        if (handlers is null) return;
        foreach (EventHandler<SecsMessage> handler in handlers.GetInvocationList())
        {
            var previousCallbackOwner = CurrentMessageCallbackOwner;
            CurrentMessageCallbackOwner = this;
            try { handler(this, message); }
            catch (Exception exception)
            {
                _diagnostics.Emit(new(SecsDiagnosticKind.ApplicationError, $"SECS message handler failed: {exception.Message}", _stateMachine.State));
            }
            finally { CurrentMessageCallbackOwner = previousCallbackOwner; }
        }
    }

    private async ValueTask ReplyToPrimaryAsync(
        SelectedConnectionSnapshot selection,
        SecsDialogueDefinition dialogue,
        SecsMessage primary,
        SecsItem? item,
        CancellationToken cancellationToken)
    {
        if (!primary.ReplyExpected || dialogue.SecondaryFunction is not { } secondaryFunction)
            throw new InvalidOperationException("The inbound Primary does not own a normal Secondary reply.");
        var response = new SecsMessage(
            primary.SessionId,
            primary.Stream,
            secondaryFunction,
            replyExpected: false,
            primary.SystemBytes,
            item);
        await SendSelectedFrameAsync(new HsmsDataMessage(response), selection, cancellationToken).ConfigureAwait(false);
    }

    private async Task SendFrameAsync(
        HsmsMessage message,
        ConnectionSnapshot connection,
        CancellationToken cancellationToken,
        Action? onTransmitted = null)
    {
        ThrowIfDisposed();
        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        byte[]? sentFrame = null;
        try
        {
            if (!IsCurrentConnection(connection.Run, connection.Epoch, connection.Stream))
                throw new IOException("HSMS connection changed before frame transmission.");
            var frame = _codec.Encode(message);
            await HsmsFrameCodec.WriteEncodedAsync(connection.Stream, frame, cancellationToken).ConfigureAwait(false);
            onTransmitted?.Invoke();
            ObserveWireFrame(frame, HsmsWireDirection.Outbound, connection.Epoch);
            sentFrame = frame;
        }
        finally { _sendGate.Release(); }
        if (sentFrame is not null)
            _diagnostics.Emit(new(SecsDiagnosticKind.FrameSent, "HSMS frame sent.", _stateMachine.State, sentFrame.Length));
    }

    private async Task SendSelectedFrameAsync(
        HsmsDataMessage message,
        SelectedConnectionSnapshot selection,
        CancellationToken cancellationToken,
        Action? onValidated = null,
        Action? onTransmitted = null)
    {
        ThrowIfDisposed();
        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        byte[]? sentFrame = null;
        try
        {
            lock (_runGate)
            {
                ValidateSelectedConnectionLocked(selection);
                onValidated?.Invoke();
            }
            var frame = _codec.Encode(message);
            await HsmsFrameCodec.WriteEncodedAsync(selection.Connection.Stream, frame, cancellationToken).ConfigureAwait(false);
            onTransmitted?.Invoke();
            ObserveWireFrame(frame, HsmsWireDirection.Outbound, selection.Connection.Epoch);
            sentFrame = frame;
        }
        finally { _sendGate.Release(); }
        if (sentFrame is not null)
            _diagnostics.Emit(new(SecsDiagnosticKind.FrameSent, "HSMS frame sent.", _stateMachine.State, sentFrame.Length));
    }

    private async Task<bool> TrySendFrameAsync(
        HsmsMessage message,
        RunContext run,
        long connectionEpoch,
        NetworkStream stream,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        byte[]? sentFrame = null;
        try
        {
            if (!IsCurrentConnection(run, connectionEpoch, stream)) return false;
            var frame = _codec.Encode(message);
            await HsmsFrameCodec.WriteEncodedAsync(stream, frame, cancellationToken).ConfigureAwait(false);
            ObserveWireFrame(frame, HsmsWireDirection.Outbound, connectionEpoch);
            sentFrame = frame;
        }
        finally { _sendGate.Release(); }
        if (sentFrame is not null)
            _diagnostics.Emit(new(SecsDiagnosticKind.FrameSent, "HSMS frame sent.", _stateMachine.State, sentFrame.Length));
        return true;
    }

    private async Task SendRejectBestEffortAsync(
        HsmsHeader rejected,
        HsmsRejectReason reason,
        RunContext run,
        long connectionEpoch,
        NetworkStream stream,
        CancellationToken cancellationToken)
    {
        try
        {
            var type = reason == HsmsRejectReason.UnsupportedPType ? rejected.PType : rejected.SType;
            var reject = new HsmsControlMessage(HsmsHeader.CreateControl(HsmsSType.RejectRequest, rejected.SystemBytes, type, (byte)reason, rejected.SessionId));
            await TrySendFrameAsync(reject, run, connectionEpoch, stream, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or SocketException or OperationCanceledException or ObjectDisposedException)
        {
            _diagnostics.Emit(new(SecsDiagnosticKind.ProtocolError, $"Reject could not be sent: {exception.Message}", _stateMachine.State));
        }
    }

    private void StartT7(RunContext run)
    {
        var connectionToken = _connectionCancellation?.Token ?? CancellationToken.None;
        var lease = run.StartT7(connectionToken);
        TrackWorker(MonitorT7Async(run, lease), "T7 monitor");
    }

    private async Task MonitorT7Async(RunContext run, T7Lease lease)
    {
        var previousT7Run = CurrentT7Run.Value;
        CurrentT7Run.Value = run;
        try
        {
            try { await _timers.WaitForExpirationAsync(HsmsTimerKind.T7, lease.CancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) when (lease.CancellationToken.IsCancellationRequested) { return; }
            catch (HsmsTimerExpiredException exception)
            {
                await FailConnectionAsync(run, lease.ConnectionEpoch, exception, lease.Generation).ConfigureAwait(false);
            }
            finally { run.CompleteT7(lease.Generation); }
        }
        finally { CurrentT7Run.Value = previousT7Run; }
    }

    private static void CancelT7(RunContext run) => run.CancelT7();

    private async Task<bool> ConnectionEndedAsync(
        RunContext run,
        long expectedEpoch,
        Exception? failure,
        long? expectedT7Generation = null,
        SecsDiagnosticEvent? leadingDiagnostic = null,
        bool allowReconnect = true)
    {
        var deferredDiagnostics = new List<SecsDiagnosticEvent>();
        var ended = false;
        var reconnect = false;
        DetachedConnectionResources resources = default;
        await _lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            lock (_runGate)
            {
                if (!ReferenceEquals(_connectedRun, run) || run.ConnectionEpoch != expectedEpoch) return false;
                if (expectedT7Generation is not null &&
                    (_stateMachine.State != HsmsConnectionState.ConnectedNotSelected ||
                     !run.IsCurrentT7(expectedEpoch, expectedT7Generation.Value))) return false;
                if (State is ConnectionState.Disconnected or ConnectionState.Disconnecting) return false;
                resources = DetachConnectionResourcesLocked(run);
                var error = failure ?? new IOException("Remote HSMS endpoint closed the connection.");
                _transactions.AbortAll(error);
                _controlTransactions.AbortAll(error);
                if (leadingDiagnostic is not null) deferredDiagnostics.Add(leadingDiagnostic);
                Add(deferredDiagnostics, ApplyTcpDisconnectedLocked());
                SetConnectionState(failure is null ? ConnectionState.Disconnected : ConnectionState.Faulted);
                deferredDiagnostics.Add(new(
                    SecsDiagnosticKind.ConnectionClosed,
                    failure?.Message ?? "Remote HSMS endpoint closed the connection.",
                    _stateMachine.State,
                    null,
                    (failure as SecsDecodeException)?.HsmsHeader));
                reconnect = allowReconnect && !run.IntentionalStopRequested;
                ended = true;
            }
            DisposeDetachedConnectionResources(resources);
        }
        finally { _lifecycleGate.Release(); }
        if (reconnect && _options.Mode == SecsConnectionMode.Active) EnsureReconnect(run);
        Emit(deferredDiagnostics);
        if (reconnect && _options.Mode == SecsConnectionMode.Passive) EnsureReconnect(run);
        return ended;
    }

    private Task<bool> FailConnectionAsync(RunContext run, long expectedEpoch, Exception failure, long? expectedT7Generation = null) =>
        ConnectionEndedAsync(
            run,
            expectedEpoch,
            failure,
            expectedT7Generation,
            new(SecsDiagnosticKind.Timeout, failure.Message, _stateMachine.State));

    private async Task ReconnectLoopAsync(RunContext run, Task startSignal)
    {
        using var operation = run.AcquireOperation();
        try { await startSignal.WaitAsync(run.Cancellation.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) when (run.Cancellation.IsCancellationRequested || startSignal.IsCanceled) { return; }
        var previousReconnectOwner = CurrentReconnectOwner.Value;
        CurrentReconnectOwner.Value = this;
        try
        {
            var cancellationToken = run.Cancellation.Token;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_options.Mode == SecsConnectionMode.Active)
                {
                    try { await _timers.WaitForExpirationAsync(HsmsTimerKind.T5, cancellationToken).ConfigureAwait(false); }
                    catch (HsmsTimerExpiredException)
                    {
                        // T5 expiration is the signal that permits the next active connection attempt.
                    }
                }
                try { await ConnectCoreAsync(run, cancellationToken, isReconnect: true).ConfigureAwait(false); return; }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) != 0 || cancellationToken.IsCancellationRequested) { return; }
                catch (SocketException exception)
                {
                    _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, $"Reconnect failed: {exception.Message}."));
                    if (_options.Mode == SecsConnectionMode.Passive) return;
                }
                catch (IOException exception)
                {
                    _diagnostics.Emit(new(SecsDiagnosticKind.ConnectionClosed, $"Reconnect failed: {exception.Message}."));
                    if (_options.Mode == SecsConnectionMode.Passive) return;
                }
            }
        }
        finally { CurrentReconnectOwner.Value = previousReconnectOwner; }
    }

    private DetachedConnectionResources DetachConnectionResourcesLocked(RunContext? owner, bool clearAll = false)
    {
        var ownsResources = clearAll || owner is not null &&
            (ReferenceEquals(_connectingRun, owner) || ReferenceEquals(_connectedRun, owner));
        if (!ownsResources) return default;
        _connectedRun?.CancelT7();
        var resources = new DetachedConnectionResources(
            true,
            _connectionCancellation,
            _listener,
            _stream,
            _client);
        _connectionCancellation = null;
        _listener = null;
        _stream = null;
        _client = null;
        if (clearAll || ReferenceEquals(_connectingRun, owner)) _connectingRun = null;
        if (clearAll || ReferenceEquals(_connectedRun, owner)) _connectedRun = null;
        return resources;
    }

    private static void DisposeDetachedConnectionResources(DetachedConnectionResources resources)
    {
        if (!resources.Detached) return;
        if (resources.Cancellation is not null)
        {
            resources.Cancellation.Cancel();
            resources.Cancellation.Dispose();
        }
        resources.Listener?.Stop();
        resources.Stream?.Dispose();
        resources.Client?.Dispose();
    }

    private (RunContext Run, IDisposable Operation) AcquireRunOperation()
    {
        ThrowIfDisposed();
        lock (_runGate)
        {
            ThrowIfDisposed();
            if (_currentRun is null || _currentRun.Cancellation.IsCancellationRequested)
                _currentRun = new RunContext(_disposeCancellation.Token);
            return (_currentRun, _currentRun.AcquireOperation());
        }
    }

    private bool TryPublishConnectedClient(RunContext run, TcpClient client, NetworkStream stream, CancellationToken operationToken)
    {
        var published = TryPublishConnectedClientCore(run, client, stream, operationToken, out var stateDiagnostic);
        if (!published) client.Dispose();
        if (stateDiagnostic is not null) _diagnostics.Emit(stateDiagnostic);
        EmitStateTransitions();
        return published;
    }

    private bool TryPublishConnectedClientCore(
        RunContext run,
        TcpClient client,
        NetworkStream stream,
        CancellationToken operationToken,
        out SecsDiagnosticEvent? stateDiagnostic)
    {
        lock (_runGate)
        {
            stateDiagnostic = null;
            if (operationToken.IsCancellationRequested || !ReferenceEquals(_currentRun, run) ||
                !ReferenceEquals(_connectingRun, run) || run.Cancellation.IsCancellationRequested)
            {
                return false;
            }
            _client = client;
            _stream = stream;
            _connectionCancellation = CancellationTokenSource.CreateLinkedTokenSource(run.Cancellation.Token, _disposeCancellation.Token);
            _connectingRun = null;
            _connectedRun = run;
            run.PassiveListenRequested = false;
            run.ConnectionEpoch = Interlocked.Increment(ref _connectionEpoch);
            stateDiagnostic = ApplyTcpConnectedLocked();
            SetConnectionState(ConnectionState.Connected);
            StartT7(run);
            return true;
        }
    }

    private bool TryOwnConnectAttempt(RunContext run, TaskCompletionSource attempt, bool isReconnect)
    {
        lock (_runGate)
        {
            if (!ReferenceEquals(_currentRun, run) || run.Cancellation.IsCancellationRequested) return false;
            run.ConnectAttempt = attempt;
            _connectingRun = run;
            if (!isReconnect || _options.Mode == SecsConnectionMode.Active || run.PassiveListenRequested)
                SetConnectionState(_options.Mode == SecsConnectionMode.Passive ? ConnectionState.Listening : ConnectionState.Connecting);
            return true;
        }
    }

    private void PublishPendingPassiveListen(RunContext run)
    {
        if (_options.Mode != SecsConnectionMode.Passive) return;
        lock (_runGate)
        {
            if (!ReferenceEquals(_currentRun, run) || run.Cancellation.IsCancellationRequested) return;
            run.PassiveListenRequested = true;
            if (ReferenceEquals(_connectingRun, run) && run.ConnectAttempt is not null)
                SetConnectionState(ConnectionState.Listening);
        }
    }

    private bool TryClearOwnedConnectAttempt(RunContext run, TaskCompletionSource attempt)
    {
        lock (_runGate)
        {
            if (!ReferenceEquals(run.ConnectAttempt, attempt)) return false;
            run.ConnectAttempt = null;
            return true;
        }
    }

    private bool TryCompleteConnectAttempt(RunContext run, TaskCompletionSource attempt)
    {
        lock (_runGate)
        {
            if (!ReferenceEquals(_currentRun, run) || !ReferenceEquals(_connectedRun, run) ||
                run.Cancellation.IsCancellationRequested || !ReferenceEquals(run.ConnectAttempt, attempt)) return false;
            run.ConnectAttempt = null;
            return attempt.TrySetResult();
        }
    }

    private RunContext? GetConnectingRun()
    {
        lock (_runGate) return _connectingRun;
    }

    private void ObserveWireFrame(byte[] frame, HsmsWireDirection direction, long connectionEpoch)
    {
        var channel = _wireObservationChannel;
        var options = _wireObservationOptions;
        if (channel is null || options is null) return;

        var header = TryReadWireHeader(frame);
        var captureMode = options.DefaultCaptureMode;
        var captureLimit = options.MaximumCapturedBytes;
        if (header is { IsData: true })
        {
            var rule = options.CaptureRules.FirstOrDefault(candidate =>
                    candidate.Stream == header.Value.Stream && candidate.Function == header.Value.Function &&
                    candidate.Direction == direction)
                ?? options.CaptureRules.FirstOrDefault(candidate =>
                    candidate.Stream == header.Value.Stream && candidate.Function == header.Value.Function &&
                    candidate.Direction is null);
            if (rule is not null)
            {
                captureMode = rule.Mode;
                if (captureMode == HsmsWireCaptureMode.FullFrame) captureLimit = rule.MaximumCapturedBytes;
            }
        }
        var capturedLength = captureMode switch
        {
            HsmsWireCaptureMode.Excluded => 0,
            HsmsWireCaptureMode.HeaderOnly => Math.Min(frame.Length, Math.Min(options.MaximumCapturedBytes, 14)),
            HsmsWireCaptureMode.FullFrame => Math.Min(frame.Length, captureLimit),
            _ => throw new InvalidOperationException($"Unsupported wire-capture mode {captureMode}.")
        };
        var sequence = Interlocked.Increment(ref _wireObservationSequence);
        var declaredLength = BinaryPrimitives.ReadInt32BigEndian(frame);
        var observation = new HsmsWireObservation(
            sequence,
            connectionEpoch,
            _timeProvider.GetUtcNow().ToUniversalTime(),
            direction,
            frame.Length,
            declaredLength,
            frame.AsMemory(0, capturedLength),
            header);
        if (!channel.Writer.TryWrite(observation)) Interlocked.Increment(ref _wireObservationDrops);
    }

    private static HsmsHeader? TryReadWireHeader(ReadOnlySpan<byte> frame)
    {
        if (frame.Length < 14) return null;
        return new HsmsHeader(
            BinaryPrimitives.ReadUInt16BigEndian(frame[4..6]),
            frame[6],
            frame[7],
            frame[8],
            frame[9],
            new SecsSystemBytes(BinaryPrimitives.ReadUInt32BigEndian(frame[10..14])));
    }

    private Task? GetReconnectTask(RunContext run)
    {
        lock (_runGate) return ReferenceEquals(_currentRun, run) ? run.ReconnectTask : null;
    }

    private async Task StopIntentionalRunAsync(RunContext expectedRun)
    {
        var run = StopCurrentRun(expectedRun);
        if (run is null) return;
        var cleanup = DisconnectCoreAsync(run, CancellationToken.None, disposing: false);
        TrackWorker(cleanup, "HSMS intentional Separate cleanup");
        await cleanup.ConfigureAwait(false);
    }

    private RunContext? StopCurrentRun() => StopCurrentRun(expectedRun: null);

    private RunContext? StopCurrentRun(RunContext? expectedRun)
    {
        RunContext? run;
        lock (_runGate)
        {
            run = _currentRun;
            if (expectedRun is not null && !ReferenceEquals(run, expectedRun)) return null;
            _currentRun = null;
        }
        run?.Cancel();
        return run;
    }

    private bool IsCurrentRun(RunContext run)
    {
        lock (_runGate) return ReferenceEquals(_currentRun, run) && !run.Cancellation.IsCancellationRequested;
    }

    private bool IsCurrentConnection(RunContext run, long connectionEpoch, NetworkStream stream)
    {
        lock (_runGate) return IsCurrentConnectionLocked(run, connectionEpoch, stream);
    }

    private bool IsCurrentConnectionLocked(RunContext run, long connectionEpoch, NetworkStream stream) =>
        ReferenceEquals(_currentRun, run) && ReferenceEquals(_connectedRun, run) &&
        ReferenceEquals(_stream, stream) && run.ConnectionEpoch == connectionEpoch &&
        !run.Cancellation.IsCancellationRequested && State == ConnectionState.Connected;

    private void EnsureConnectedRun(RunContext run)
    {
        lock (_runGate)
        {
            if (ReferenceEquals(_currentRun, run) && ReferenceEquals(_connectedRun, run) &&
                !run.Cancellation.IsCancellationRequested && State == ConnectionState.Connected) return;
        }
        if (run.Cancellation.IsCancellationRequested)
            throw new OperationCanceledException("The HSMS connection run was stopped before connection completed.", run.Cancellation.Token);
        throw new IOException("The HSMS connection attempt completed without publishing a connected session.");
    }

    private bool HasDifferentCurrentRun(RunContext? run)
    {
        lock (_runGate) return _currentRun is not null && !ReferenceEquals(_currentRun, run);
    }

    private void EnsureReconnect(RunContext run)
    {
        Task? reconnect = null;
        TaskCompletionSource? reconnectStart = null;
        lock (_runGate)
        {
            var reconnectEnabled = _options.Mode == SecsConnectionMode.Passive || _options.AutoReconnect;
            if (!reconnectEnabled || Volatile.Read(ref _disposed) != 0 ||
                !ReferenceEquals(_currentRun, run) || run.Cancellation.IsCancellationRequested ||
                run.ConnectAttempt is not null || run.ReconnectTask is { IsCompleted: false }) return;
            reconnectStart = new(TaskCreationOptions.RunContinuationsAsynchronously);
            reconnect = ReconnectLoopAsync(run, reconnectStart.Task);
            run.ReconnectTask = reconnect;
        }
        TrackWorker(reconnect, "HSMS reconnect loop");
        reconnectStart.TrySetResult();
    }

    private void TrackWorker(Task task, string operation)
    {
        ArgumentNullException.ThrowIfNull(task);
        lock (_workerGate) _workers.Add(task);
        _ = BackgroundTaskObserver.Observe(task, operation);
        _ = task.ContinueWith(
            static (completed, state) => ((HsmsSession)state!).RemoveWorker(completed),
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void RemoveWorker(Task task)
    {
        lock (_workerGate)
        {
            _workers.Remove(task);
            if (Volatile.Read(ref _disposed) != 0 && task.Exception is { } failure)
                _shutdownWorkerFailures.AddRange(failure.Flatten().InnerExceptions);
        }
    }

    private async Task DrainWorkersAsync(Task? excludedWorker = null)
    {
        while (true)
        {
            Task[] workers;
            lock (_workerGate)
            {
                workers = _workers.Where(worker => !ReferenceEquals(worker, excludedWorker)).ToArray();
                if (workers.Length == 0)
                {
                    if (_shutdownWorkerFailures.Count == 0) return;
                    var failures = _shutdownWorkerFailures.ToArray();
                    _shutdownWorkerFailures.Clear();
                    throw new AggregateException("One or more owned HSMS workers failed during shutdown.", failures);
                }
            }
            try { await Task.WhenAll(workers).ConfigureAwait(false); }
            catch
            {
                foreach (var worker in workers)
                    if (worker.IsFaulted) _ = worker.Exception;
            }
        }
    }

    private static IPAddress ResolveBindAddress(string host)
    {
        if (host is "0.0.0.0" or "*") return IPAddress.Any;
        if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase)) return IPAddress.Loopback;
        return IPAddress.TryParse(host, out var address) ? address : throw new ArgumentException($"Passive HSMS host '{host}' must be a bindable IP address.", nameof(host));
    }

    private void SetConnectionState(ConnectionState state)
    {
        var previous = (ConnectionState)Interlocked.Exchange(ref _connectionState, (int)state);
        if (previous == state) return;
        var hsms = _stateMachine.State;
        lock (_stateEventGate)
            _stateChanges.Enqueue(new(
                previous,
                state,
                hsms,
                hsms,
                CreateConnectionIdentity(Volatile.Read(ref _connectionEpoch))));
    }
    private ConnectionSnapshot GetConnectedRunSnapshot()
    {
        lock (_runGate) return GetConnectedRunSnapshotLocked();
    }

    private ConnectionSnapshot GetConnectedRunSnapshotLocked()
    {
        var run = _connectedRun;
        var stream = _stream;
        if (run is null || stream is null || !ReferenceEquals(_currentRun, run) ||
            run.Cancellation.IsCancellationRequested || State != ConnectionState.Connected)
            throw new IOException("HSMS stream is not connected.");
        return new(run, run.ConnectionEpoch, stream);
    }
    private SelectedConnectionSnapshot GetSelectedConnectionSnapshotLocked(string operation)
    {
        var connection = GetConnectedRunSnapshotLocked();
        var state = _stateMachine.State;
        if (state != HsmsConnectionState.Selected) throw new HsmsStateException(state, operation);
        return new(connection, _selectedTransitionGeneration);
    }
    private void ValidateSelectedConnectionLocked(SelectedConnectionSnapshot selection)
    {
        var connection = selection.Connection;
        if (!IsCurrentConnectionLocked(connection.Run, connection.Epoch, connection.Stream))
            throw new IOException("HSMS connection changed before data-frame transmission.");
        var state = _stateMachine.State;
        if (state != HsmsConnectionState.Selected)
            throw new HsmsStateException(state, "data-frame transmission");
        if (_selectedTransitionGeneration != selection.Generation)
            throw new IOException("HSMS selected transition changed before data-frame transmission.");
    }
    private static void Add(List<SecsDiagnosticEvent> diagnostics, SecsDiagnosticEvent? diagnostic)
    {
        if (diagnostic is not null) diagnostics.Add(diagnostic);
    }
    private SecsDiagnosticEvent? ApplyTcpConnectedLocked()
    {
        var before = _stateMachine.State;
        var diagnostic = _stateMachine.OnTcpConnectedDeferred();
        RecordSelectedTransitionLocked(before, _stateMachine.State);
        return diagnostic;
    }
    private SecsDiagnosticEvent? ApplyTcpDisconnectedLocked()
    {
        var before = _stateMachine.State;
        var diagnostic = _stateMachine.OnTcpDisconnectedDeferred();
        RecordSelectedTransitionLocked(before, _stateMachine.State);
        return diagnostic;
    }
    private void RecordSelectedTransitionLocked(HsmsConnectionState before, HsmsConnectionState after)
    {
        if (before != after)
        {
            var connection = State;
            lock (_stateEventGate)
                _stateChanges.Enqueue(new(
                    connection,
                    connection,
                    before,
                    after,
                    CreateConnectionIdentity(Volatile.Read(ref _connectionEpoch))));
        }
        if ((before == HsmsConnectionState.Selected) != (after == HsmsConnectionState.Selected))
            _selectedTransitionGeneration++;
    }
    private void Emit(List<SecsDiagnosticEvent> diagnostics)
    {
        foreach (var diagnostic in diagnostics) _diagnostics.Emit(diagnostic);
        diagnostics.Clear();
        EmitStateTransitions();
    }
    private void EmitStateTransitions()
    {
        lock (_stateEventGate)
        {
            if (_stateEventDraining) return;
            _stateEventDraining = true;
        }
        while (true)
        {
            SecsSessionStateChangedEventArgs transition;
            lock (_stateEventGate)
            {
                if (!_stateChanges.TryDequeue(out transition!))
                {
                    _stateEventDraining = false;
                    return;
                }
            }
            var handlers = StateChanged;
            if (handlers is null) continue;
            foreach (EventHandler<SecsSessionStateChangedEventArgs> handler in handlers.GetInvocationList())
            {
                var previousCallbackOwner = CurrentMessageCallbackOwner;
                CurrentMessageCallbackOwner = this;
                try { handler(this, transition); }
                catch (Exception exception) { SafeTrace.Error("SECS state-change event handler failed: {0}", exception); }
                finally { CurrentMessageCallbackOwner = previousCallbackOwner; }
            }
        }
    }
    private static bool IsControlResponse(HsmsSType type) => type is HsmsSType.SelectResponse or HsmsSType.DeselectResponse or HsmsSType.LinktestResponse;
    private static void ObserveIfFaulted(Task task) { if (task.IsFaulted) _ = task.Exception; }
    private bool IsInSynchronousProtocolCallback() =>
        ReferenceEquals(CurrentMessageCallbackOwner, this) || SafeSecsDiagnosticSink.IsInvoking(_diagnostics);
    private SecsConnectionIdentity CreateConnectionIdentity(long connectionEpoch) => new(
        ProviderKey,
        _sessionInstanceId,
        connectionEpoch,
        _options.SessionId,
        _options.Role,
        _options.Mode);
    private void ValidateSessionId(SecsMessage message)
    {
        if (message.SessionId != _options.SessionId)
            throw new ArgumentException($"Message Session ID {message.SessionId.Value} does not match configured Session ID {_options.SessionId.Value}.", nameof(message));
    }
    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    private sealed class RunContext : IDisposable
    {
        private readonly object _operationGate = new();
        private readonly object _t7Gate = new();
        private TaskCompletionSource? _operationsDrained;
        private CancellationTokenSource? _t7Cancellation;
        private int _operationCount;
        private bool _disposeRequested;
        private int _disposed;
        private long _t7Generation;

        public RunContext(CancellationToken disposeToken) =>
            Cancellation = CancellationTokenSource.CreateLinkedTokenSource(disposeToken);

        public CancellationTokenSource Cancellation { get; }
        public TaskCompletionSource? ConnectAttempt { get; set; }
        public Task? ReceiveLoop { get; set; }
        public Task? ReconnectTask { get; set; }
        public bool PassiveListenRequested { get; set; }
        public bool IntentionalStopRequested { get; set; }
        public long ConnectionEpoch { get; set; }

        public IDisposable AcquireOperation()
        {
            lock (_operationGate)
            {
                ObjectDisposedException.ThrowIf(_disposed != 0 || _disposeRequested, this);
                if (_operationCount == 0)
                    _operationsDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _operationCount++;
                return new OperationLease(this);
            }
        }

        public void Cancel()
        {
            lock (_operationGate)
            {
                if (_disposed == 0) Cancellation.Cancel();
            }
        }

        public Task WaitForOperationsAsync(CancellationToken cancellationToken)
        {
            lock (_operationGate)
            {
                if (_operationCount == 0) return Task.CompletedTask;
                return _operationsDrained!.Task.WaitAsync(cancellationToken);
            }
        }

        private void ReleaseOperation()
        {
            var dispose = false;
            lock (_operationGate)
            {
                if (--_operationCount == 0)
                {
                    _operationsDrained!.TrySetResult();
                    dispose = _disposeRequested;
                }
            }
            if (dispose) DisposeCore();
        }

        public void RequestDisposal()
        {
            var dispose = false;
            lock (_operationGate)
            {
                _disposeRequested = true;
                dispose = _operationCount == 0;
            }
            if (dispose) DisposeCore();
        }

        public T7Lease StartT7(CancellationToken connectionToken)
        {
            CancellationTokenSource cancellation;
            CancellationTokenSource? previous;
            long generation;
            lock (_t7Gate)
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                cancellation = CancellationTokenSource.CreateLinkedTokenSource(Cancellation.Token, connectionToken);
                generation = ++_t7Generation;
                previous = Interlocked.Exchange(ref _t7Cancellation, cancellation);
            }
            CancelAndDispose(previous);
            return new(ConnectionEpoch, generation, cancellation.Token);
        }

        public void CancelT7()
        {
            CancellationTokenSource? cancellation;
            lock (_t7Gate)
            {
                _t7Generation++;
                cancellation = Interlocked.Exchange(ref _t7Cancellation, null);
            }
            CancelAndDispose(cancellation);
        }

        public bool IsCurrentT7(long expectedEpoch, long expectedGeneration)
        {
            lock (_t7Gate)
                return ConnectionEpoch == expectedEpoch && _t7Generation == expectedGeneration && _t7Cancellation is not null;
        }

        public void CompleteT7(long generation)
        {
            CancellationTokenSource? cancellation = null;
            lock (_t7Gate)
            {
                if (_t7Generation == generation)
                    cancellation = Interlocked.Exchange(ref _t7Cancellation, null);
            }
            cancellation?.Dispose();
        }

        public void Dispose()
        {
            lock (_operationGate)
            {
                if (_operationCount != 0) throw new InvalidOperationException("A run cannot be disposed while connect operations are active.");
            }
            DisposeCore();
        }

        private void DisposeCore()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            CancelT7();
            Cancellation.Dispose();
        }

        private static void CancelAndDispose(CancellationTokenSource? cancellation)
        {
            if (cancellation is null) return;
            try { cancellation.Cancel(); }
            finally { cancellation.Dispose(); }
        }

        private sealed class OperationLease(RunContext owner) : IDisposable
        {
            private RunContext? _owner = owner;
            public void Dispose() => Interlocked.Exchange(ref _owner, null)?.ReleaseOperation();
        }
    }

    private readonly record struct ConnectionSnapshot(RunContext Run, long Epoch, NetworkStream Stream);
    private readonly record struct SelectedConnectionSnapshot(ConnectionSnapshot Connection, long Generation);
    private readonly record struct DetachedConnectionResources(
        bool Detached,
        CancellationTokenSource? Cancellation,
        TcpListener? Listener,
        NetworkStream? Stream,
        TcpClient? Client);
    private readonly record struct T7Lease(long ConnectionEpoch, long Generation, CancellationToken CancellationToken);
}
