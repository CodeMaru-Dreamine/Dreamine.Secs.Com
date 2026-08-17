using System.Collections.Concurrent;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Diagnostics;

namespace Dreamine.Secs.Com.Transactions;

/// <summary>\if KO <para>세션별 SECS transaction과 T3 수명을 thread-safe하게 관리합니다.</para> \endif \if EN <para>Manages session-scoped SECS transactions and T3 lifetimes safely across threads.</para> \endif</summary>
public sealed class SecsTransactionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<uint, PendingTransaction> _pending = new();
    private readonly object _monitorGate = new();
    private readonly object _disposeGate = new();
    private readonly HashSet<ProtocolDrainRegistration> _protocolDrains = new();
    private readonly object _recentGate = new();
    private readonly HashSet<uint> _recent = new();
    private readonly Queue<uint> _recentOrder = new();
    private readonly SecsSystemBytesGenerator _generator;
    private readonly TimeProvider _timeProvider;
    private readonly ISecsDiagnosticSink _diagnostics;
    private readonly int _recentCapacity;
    private readonly CancellationTokenSource _lifetime = new();
    private Task? _disposeTask;
    private int _disposed;

    /// <summary>\if KO manager를 만듭니다. \endif \if EN Creates a manager. \endif</summary>
    /// <param name="timeProvider">\if KO 결정론적 시간 공급자입니다. \endif \if EN Deterministic time provider. \endif</param><param name="generator">\if KO System Bytes generator입니다. \endif \if EN System Bytes generator. \endif</param><param name="diagnostics">\if KO 진단 sink입니다. \endif \if EN Diagnostic sink. \endif</param><param name="recentCapacity">\if KO 중복 검출 보관 수입니다. \endif \if EN Recent-completion capacity. \endif</param>
    public SecsTransactionManager(TimeProvider? timeProvider = null, SecsSystemBytesGenerator? generator = null, ISecsDiagnosticSink? diagnostics = null, int recentCapacity = 1024)
    {
        if (recentCapacity < 0) throw new ArgumentOutOfRangeException(nameof(recentCapacity));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _generator = generator ?? new SecsSystemBytesGenerator();
        _diagnostics = SafeSecsDiagnosticSink.Wrap(diagnostics);
        _recentCapacity = recentCapacity;
    }

    /// <summary>\if KO 현재 열린 transaction 수입니다. \endif \if EN Gets the current open-transaction count. \endif</summary>
    public int OutstandingCount => _pending.Count;

    /// <summary>\if KO 열린/최근 완료 값과 충돌하지 않는 다음 System Bytes를 할당합니다. \endif \if EN Allocates the next System Bytes not colliding with open or recent transactions. \endif</summary>
    public SecsSystemBytes AllocateSystemBytes()
    {
        ThrowIfDisposed();
        while (true)
        {
            var candidate = _generator.Next();
            if (_pending.ContainsKey(candidate.Value)) continue;
            lock (_recentGate) { if (_recent.Contains(candidate.Value)) continue; }
            return candidate;
        }
    }

    /// <summary>\if KO W-bit primary를 등록하고 Secondary 또는 T3/취소를 기다립니다. \endif \if EN Registers a W-bit primary and waits for its secondary, T3, or cancellation. \endif</summary>
    /// <param name="primary">\if KO primary 메시지입니다. \endif \if EN Primary message. \endif</param><param name="t3">\if KO T3 기간입니다. \endif \if EN T3 duration. \endif</param><param name="cancellationToken">\if KO 호출자 취소입니다. \endif \if EN Caller cancellation. \endif</param><returns>\if KO 상관된 secondary입니다. \endif \if EN Correlated secondary. \endif</returns>
    public Task<SecsMessage> RegisterPrimaryAsync(SecsMessage primary, TimeSpan t3, CancellationToken cancellationToken = default)
    {
        if (t3 <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(t3));
        lock (_monitorGate)
        {
            var completion = RegisterPrimaryDeferred(primary, cancellationToken);
            _ = StartTimeout(primary.SystemBytes, t3, cancellationToken);
            return completion;
        }
    }

    internal Task<SecsMessage> RegisterPrimaryDeferred(SecsMessage primary, CancellationToken cancellationToken)
        => RegisterPrimaryDeferred(primary, expectedSecondary: null, cancellationToken);

    /// <summary>
    /// \if KO <para>선언된 인접 정상 Secondary만 허용하는 provider-neutral W1 transaction을 등록합니다.</para> \endif
    /// \if EN <para>Registers a provider-neutral W1 transaction that accepts only its declared adjacent normal secondary.</para> \endif
    /// </summary>
    internal Task<SecsMessage> RegisterPrimaryDeferred(
        SecsMessage primary,
        SecsFunction expectedSecondary,
        CancellationToken cancellationToken)
        => RegisterPrimaryDeferred(primary, (SecsFunction?)expectedSecondary, cancellationToken);

    private Task<SecsMessage> RegisterPrimaryDeferred(
        SecsMessage primary,
        SecsFunction? expectedSecondary,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(primary);
        if (!primary.Function.IsPrimary || !primary.ReplyExpected) throw new ArgumentException("A transaction requires a primary message with W-bit set.", nameof(primary));
        if (expectedSecondary is { } expected &&
            (primary.Function.Value == byte.MaxValue ||
             !expected.IsSecondary ||
             expected.Value != primary.Function.Value + 1))
            throw new ArgumentException("The expected normal secondary must be the adjacent even function immediately following the primary.", nameof(expectedSecondary));
        cancellationToken.ThrowIfCancellationRequested();
        lock (_monitorGate)
        {
            ThrowIfDisposed();
            var pending = new PendingTransaction(primary, expectedSecondary);
            if (!_pending.TryAdd(primary.SystemBytes.Value, pending))
            {
                pending.CancelFromOwner();
                throw new InvalidOperationException($"System Bytes 0x{primary.SystemBytes.Value:X8} is already open.");
            }
            return pending.Completion.Task;
        }
    }

    internal bool StartTimeout(SecsSystemBytes systemBytes, TimeSpan t3, CancellationToken cancellationToken)
    {
        if (t3 <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(t3));
        lock (_monitorGate)
        {
            ThrowIfDisposed();
            if (!_pending.TryGetValue(systemBytes.Value, out var pending)) return false;
            var drain = new ProtocolDrainRegistration(this);
            _protocolDrains.Add(drain);
            if (!pending.TryStartMonitor(
                    token => MonitorAsync(pending, drain, t3, cancellationToken, token, _lifetime.Token),
                    out var monitor))
            {
                drain.Complete();
                return false;
            }
            BackgroundTaskObserver.Observe(monitor!, "T3 transaction monitor and external diagnostic delivery");
            return true;
        }
    }

    /// <summary>\if KO 수신 Secondary로 열린 transaction을 완료합니다. \endif \if EN Completes an open transaction with an inbound secondary. \endif</summary>
    /// <param name="secondary">\if KO secondary 메시지입니다. \endif \if EN Secondary message. \endif</param><returns>\if KO 상관관계 결과입니다. \endif \if EN Correlation result. \endif</returns>
    public SecsTransactionCompletionStatus TryComplete(SecsMessage secondary)
    {
        var status = TryCompleteDeferred(secondary, out var diagnostic);
        if (diagnostic is not null) _diagnostics.Emit(diagnostic);
        return status;
    }

    internal SecsTransactionCompletionStatus TryCompleteDeferred(SecsMessage secondary, out SecsDiagnosticEvent? diagnostic)
    {
        ArgumentNullException.ThrowIfNull(secondary); ThrowIfDisposed();
        diagnostic = null;
        if (!_pending.TryGetValue(secondary.SystemBytes.Value, out var pending))
        {
            lock (_recentGate) return _recent.Contains(secondary.SystemBytes.Value) ? SecsTransactionCompletionStatus.Duplicate : SecsTransactionCompletionStatus.UnknownSystemBytes;
        }
        var primary = pending.Primary;
        var validReplyFunction = secondary.Function.Value == 0 ||
            (pending.ExpectedSecondary is { } expected
                ? secondary.Function == expected
                : secondary.Function.IsSecondary && secondary.Function.Value == primary.Function.Value + 1);
        if (secondary.ReplyExpected || !validReplyFunction ||
            secondary.SessionId != primary.SessionId || secondary.Stream != primary.Stream)
            return SecsTransactionCompletionStatus.InvalidCorrelation;
        if (!_pending.TryRemove(secondary.SystemBytes.Value, out pending))
            return IsRecent(secondary.SystemBytes.Value) ? SecsTransactionCompletionStatus.Duplicate : SecsTransactionCompletionStatus.UnknownSystemBytes;
        pending.CancelFromOwner();
        AddRecent(secondary.SystemBytes.Value);
        pending.Completion.TrySetResult(secondary);
        diagnostic = new(SecsDiagnosticKind.SecondaryReceived, $"Secondary completed transaction 0x{secondary.SystemBytes.Value:X8}.");
        return SecsTransactionCompletionStatus.Completed;
    }

    /// <summary>\if KO 연결 종료 오류로 모든 대기를 정리합니다. \endif \if EN Completes all waiters with a connection-termination error. \endif</summary>
    /// <param name="error">\if KO 전달할 오류입니다. \endif \if EN Error to deliver. \endif</param>
    public void AbortAll(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        foreach (var pair in _pending.ToArray())
        {
            if (!_pending.TryRemove(pair.Key, out var pending)) continue;
            pending.CancelFromOwner();
            pending.Completion.TrySetException(error);
        }
    }

    /// <summary>\if KO 특정 transaction만 오류로 종료합니다. \endif \if EN Terminates only the specified transaction with an error. \endif</summary>
    /// <param name="systemBytes">\if KO 종료할 System Bytes입니다. \endif \if EN System Bytes to terminate. \endif</param><param name="error">\if KO 전달할 오류입니다. \endif \if EN Error to deliver. \endif</param><returns>\if KO transaction을 제거했는지 여부입니다. \endif \if EN Whether a transaction was removed. \endif</returns>
    public bool TryAbort(SecsSystemBytes systemBytes, Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (!_pending.TryRemove(systemBytes.Value, out var pending)) return false;
        pending.CancelFromOwner();
        pending.Completion.TrySetException(error);
        return true;
    }

    /// <summary>\if KO 모든 대기를 취소하고 manager를 해제합니다. \endif \if EN Cancels all waiters and disposes the manager. \endif</summary>
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
            AbortAll(new ObjectDisposedException(nameof(SecsTransactionManager)));
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
        PendingTransaction pending,
        ProtocolDrainRegistration drain,
        TimeSpan t3,
        CancellationToken callerCancellation,
        CancellationToken pendingCancellation,
        CancellationToken managerCancellation)
    {
        var emitTimeoutDiagnostic = false;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(pendingCancellation, callerCancellation, managerCancellation);
        try
        {
            await Task.Delay(t3, _timeProvider, linked.Token).ConfigureAwait(false);
            if (_pending.TryRemove(pending.Primary.SystemBytes.Value, out _))
            {
                AddRecent(pending.Primary.SystemBytes.Value);
                var error = new SecsTransactionTimeoutException(pending.Primary.SystemBytes, t3);
                pending.Completion.TrySetException(error);
                emitTimeoutDiagnostic = true;
            }
        }
        catch (OperationCanceledException) when (callerCancellation.IsCancellationRequested)
        {
            if (_pending.TryRemove(pending.Primary.SystemBytes.Value, out _))
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
            _diagnostics.Emit(new(SecsDiagnosticKind.Timeout, $"T3 expired for 0x{pending.Primary.SystemBytes.Value:X8}."));
    }

    private void CompleteProtocolDrain(ProtocolDrainRegistration drain)
    {
        lock (_monitorGate) _protocolDrains.Remove(drain);
        drain.Signal();
    }

    private void AddRecent(uint value)
    {
        if (_recentCapacity == 0) return;
        lock (_recentGate)
        {
            if (!_recent.Add(value)) return;
            _recentOrder.Enqueue(value);
            while (_recentOrder.Count > _recentCapacity) _recent.Remove(_recentOrder.Dequeue());
        }
    }
    private bool IsRecent(uint value) { lock (_recentGate) return _recent.Contains(value); }
    private void ThrowIfDisposed() { ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this); }

    private sealed class PendingTransaction(SecsMessage primary, SecsFunction? expectedSecondary)
    {
        private readonly object _gate = new();
        private readonly CancellationTokenSource _lifetime = new();
        private bool _removed;
        private bool _monitorStarted;
        private bool _lifetimeDisposed;

        public SecsMessage Primary { get; } = primary;
        public SecsFunction? ExpectedSecondary { get; } = expectedSecondary;
        public TaskCompletionSource<SecsMessage> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool TryStartMonitor(Func<CancellationToken, Task> monitorFactory, out Task? monitor)
        {
            lock (_gate)
            {
                if (_removed)
                {
                    monitor = null;
                    return false;
                }
                if (_monitorStarted) throw new InvalidOperationException("The transaction timeout was already started.");
                _monitorStarted = true;
                monitor = monitorFactory(_lifetime.Token);
                return true;
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

    private sealed class ProtocolDrainRegistration(SecsTransactionManager owner)
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
