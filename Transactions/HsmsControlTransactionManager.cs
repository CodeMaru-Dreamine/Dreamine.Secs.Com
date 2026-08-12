using System.Collections.Concurrent;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Diagnostics;

namespace Dreamine.Secs.Com.Transactions;

/// <summary>\if KO <para>Select/Deselect/Linktest 제어 transaction과 T6를 관리합니다.</para> \endif \if EN <para>Manages Select, Deselect, and Linktest control transactions with T6.</para> \endif</summary>
public sealed class HsmsControlTransactionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<uint, PendingControl> _pending = new();
    private readonly object _monitorGate = new();
    private readonly object _disposeGate = new();
    private readonly HashSet<ProtocolDrainRegistration> _protocolDrains = new();
    private readonly TimeProvider _timeProvider;
    private readonly ISecsDiagnosticSink _diagnostics;
    private readonly CancellationTokenSource _lifetime = new();
    private Task? _disposeTask;
    private int _disposed;

    /// <summary>\if KO 시간 공급자와 진단 sink로 manager를 만듭니다. \endif \if EN Creates a manager with a time provider and diagnostic sink. \endif</summary>
    /// <param name="timeProvider">\if KO 시간 공급자입니다. \endif \if EN Time provider. \endif</param><param name="diagnostics">\if KO 진단 sink입니다. \endif \if EN Diagnostic sink. \endif</param>
    public HsmsControlTransactionManager(TimeProvider? timeProvider = null, ISecsDiagnosticSink? diagnostics = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System; _diagnostics = SafeSecsDiagnosticSink.Wrap(diagnostics);
    }

    /// <summary>\if KO 열린 제어 transaction 수입니다. \endif \if EN Gets the open control-transaction count. \endif</summary>
    public int OutstandingCount => _pending.Count;

    /// <summary>\if KO 응답이 필요한 제어 요청을 등록합니다. \endif \if EN Registers a control request requiring a response. \endif</summary>
    /// <param name="request">\if KO 요청입니다. \endif \if EN Request. \endif</param><param name="t6">\if KO T6입니다. \endif \if EN T6. \endif</param><param name="cancellationToken">\if KO 취소 토큰입니다. \endif \if EN Cancellation token. \endif</param><returns>\if KO 상관된 응답입니다. \endif \if EN Correlated response. \endif</returns>
    public Task<HsmsControlMessage> RegisterAsync(HsmsControlMessage request, TimeSpan t6, CancellationToken cancellationToken = default)
    {
        if (t6 <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(t6));
        lock (_monitorGate)
        {
            var completion = RegisterDeferred(request, cancellationToken);
            _ = StartTimeout(request.Header.SystemBytes, t6, cancellationToken);
            return completion;
        }
    }

    internal Task<HsmsControlMessage> RegisterDeferred(HsmsControlMessage request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var expected = ExpectedResponse(request.SType);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_monitorGate)
        {
            ThrowIfDisposed();
            var pending = new PendingControl(expected, request.Header.SystemBytes, request.Header.SessionId);
            if (!_pending.TryAdd(request.Header.SystemBytes.Value, pending))
            {
                pending.CancelFromOwner();
                throw new InvalidOperationException("Control System Bytes is already open.");
            }
            return pending.Completion.Task;
        }
    }

    internal bool StartTimeout(SecsSystemBytes systemBytes, TimeSpan t6, CancellationToken cancellationToken)
    {
        if (t6 <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(t6));
        lock (_monitorGate)
        {
            ThrowIfDisposed();
            if (!_pending.TryGetValue(systemBytes.Value, out var pending)) return false;
            var drain = new ProtocolDrainRegistration(this);
            _protocolDrains.Add(drain);
            var monitor = pending.TryStartMonitor(token => MonitorAsync(pending, drain, t6, cancellationToken, token, _lifetime.Token));
            if (monitor is null)
            {
                drain.Complete();
                return false;
            }
            BackgroundTaskObserver.Observe(monitor, "T6 control transaction monitor and external diagnostic delivery");
            return true;
        }
    }

    /// <summary>\if KO 수신 제어 응답으로 transaction 완료를 시도합니다. \endif \if EN Attempts to complete a transaction with an inbound control response. \endif</summary>
    /// <param name="response">\if KO 응답입니다. \endif \if EN Response. \endif</param><returns>\if KO 완료 여부입니다. \endif \if EN Whether completion succeeded. \endif</returns>
    public bool TryComplete(HsmsControlMessage response)
    {
        ArgumentNullException.ThrowIfNull(response); ThrowIfDisposed();
        if (!_pending.TryGetValue(response.Header.SystemBytes.Value, out var pending) || response.SType != pending.ExpectedResponse || response.Header.SessionId != pending.SessionId) return false;
        if (!_pending.TryRemove(response.Header.SystemBytes.Value, out pending)) return false;
        pending.CancelFromOwner(); pending.Completion.TrySetResult(response); return true;
    }

    internal bool TryCompleteAfter(HsmsControlMessage response, Action applyProtocolState)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(applyProtocolState);
        ThrowIfDisposed();
        if (!_pending.TryGetValue(response.Header.SystemBytes.Value, out var pending) || response.SType != pending.ExpectedResponse || response.Header.SessionId != pending.SessionId) return false;
        if (!_pending.TryRemove(response.Header.SystemBytes.Value, out pending)) return false;
        pending.CancelFromOwner();
        try
        {
            applyProtocolState();
            pending.Completion.TrySetResult(response);
        }
        catch (Exception exception)
        {
            pending.Completion.TrySetException(exception);
            throw;
        }
        return true;
    }

    /// <summary>\if KO 연결 오류로 모든 제어 대기를 종료합니다. \endif \if EN Terminates every control waiter with a connection error. \endif</summary>
    /// <param name="error">\if KO 오류입니다. \endif \if EN Error. \endif</param>
    public void AbortAll(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        foreach (var pair in _pending.ToArray()) if (_pending.TryRemove(pair.Key, out var pending)) { pending.CancelFromOwner(); pending.Completion.TrySetException(error); }
    }

    /// <summary>\if KO 특정 제어 transaction만 오류로 종료합니다. \endif \if EN Terminates only the specified control transaction with an error. \endif</summary>
    /// <param name="systemBytes">\if KO 종료할 System Bytes입니다. \endif \if EN System Bytes to terminate. \endif</param><param name="error">\if KO 전달할 오류입니다. \endif \if EN Error to deliver. \endif</param><returns>\if KO transaction을 제거했는지 여부입니다. \endif \if EN Whether a transaction was removed. \endif</returns>
    public bool TryAbort(SecsSystemBytes systemBytes, Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (!_pending.TryRemove(systemBytes.Value, out var pending)) return false;
        pending.CancelFromOwner();
        pending.Completion.TrySetException(error);
        return true;
    }

    /// <summary>\if KO manager를 안전하게 해제합니다. \endif \if EN Safely disposes the manager. \endif</summary>
    public ValueTask DisposeAsync()
    {
        lock (_disposeGate)
        {
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        lock (_monitorGate) Interlocked.Exchange(ref _disposed, 1);
        try
        {
            _lifetime.Cancel();
            AbortAll(new ObjectDisposedException(nameof(HsmsControlTransactionManager)));
            Task[] drains;
            lock (_monitorGate) drains = _protocolDrains.Select(item => item.Completion).ToArray();
            await Task.WhenAll(drains).ConfigureAwait(false);
        }
        finally
        {
            lock (_monitorGate) _protocolDrains.Clear();
            _lifetime.Dispose();
        }
    }

    private async Task MonitorAsync(
        PendingControl pending,
        ProtocolDrainRegistration drain,
        TimeSpan t6,
        CancellationToken callerCancellation,
        CancellationToken pendingCancellation,
        CancellationToken managerCancellation)
    {
        var emitTimeoutDiagnostic = false;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(pendingCancellation, callerCancellation, managerCancellation);
        try
        {
            await Task.Delay(t6, _timeProvider, linked.Token).ConfigureAwait(false);
            if (_pending.TryRemove(pending.SystemBytes.Value, out _))
            {
                var error = new HsmsTimerExpiredException("T6", t6); pending.Completion.TrySetException(error);
                emitTimeoutDiagnostic = true;
            }
        }
        catch (OperationCanceledException) when (callerCancellation.IsCancellationRequested)
        {
            if (_pending.TryRemove(pending.SystemBytes.Value, out _))
            {
                pending.Completion.TrySetCanceled(callerCancellation);
            }
        }
        catch (OperationCanceledException) when (managerCancellation.IsCancellationRequested || pendingCancellation.IsCancellationRequested) { }
        finally
        {
            pending.DisposeAfterMonitor();
            drain.Complete();
        }
        if (emitTimeoutDiagnostic)
            _diagnostics.Emit(new(SecsDiagnosticKind.Timeout, $"T6 expired for 0x{pending.SystemBytes.Value:X8}."));
    }

    private void CompleteProtocolDrain(ProtocolDrainRegistration drain)
    {
        lock (_monitorGate) _protocolDrains.Remove(drain);
        drain.Signal();
    }

    private static HsmsSType ExpectedResponse(HsmsSType request) => request switch
    {
        HsmsSType.SelectRequest => HsmsSType.SelectResponse,
        HsmsSType.DeselectRequest => HsmsSType.DeselectResponse,
        HsmsSType.LinktestRequest => HsmsSType.LinktestResponse,
        _ => throw new ArgumentException($"SType {request} does not open a control transaction.", nameof(request))
    };
    private void ThrowIfDisposed() { ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this); }

    private sealed class PendingControl(HsmsSType expectedResponse, SecsSystemBytes systemBytes, ushort sessionId)
    {
        private readonly object _gate = new();
        private readonly CancellationTokenSource _lifetime = new();
        private bool _removed;
        private bool _monitorStarted;
        private bool _lifetimeDisposed;

        public HsmsSType ExpectedResponse { get; } = expectedResponse;
        public SecsSystemBytes SystemBytes { get; } = systemBytes;
        public ushort SessionId { get; } = sessionId;
        public TaskCompletionSource<HsmsControlMessage> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task? TryStartMonitor(Func<CancellationToken, Task> monitorFactory)
        {
            lock (_gate)
            {
                if (_removed) return null;
                if (_monitorStarted) throw new InvalidOperationException("The control timeout was already started.");
                _monitorStarted = true;
                return monitorFactory(_lifetime.Token);
            }
        }

        public void CancelFromOwner()
        {
            lock (_gate)
            {
                if (_removed) return;
                _removed = true;
                if (_lifetimeDisposed) return;
                _lifetime.Cancel();
                if (_monitorStarted) return;
                _lifetime.Dispose();
                _lifetimeDisposed = true;
            }
        }

        public void DisposeAfterMonitor()
        {
            lock (_gate)
            {
                _removed = true;
                if (_lifetimeDisposed) return;
                _lifetime.Dispose();
                _lifetimeDisposed = true;
            }
        }

    }

    private sealed class ProtocolDrainRegistration(HsmsControlTransactionManager owner)
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _completed;

        public Task Completion => _completion.Task;

        public void Complete()
        {
            if (Interlocked.Exchange(ref _completed, 1) == 0) owner.CompleteProtocolDrain(this);
        }

        public void Signal() => _completion.TrySetResult();
    }
}
