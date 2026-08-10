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
    private readonly object _recentGate = new();
    private readonly HashSet<uint> _recent = new();
    private readonly Queue<uint> _recentOrder = new();
    private readonly SecsSystemBytesGenerator _generator;
    private readonly TimeProvider _timeProvider;
    private readonly ISecsDiagnosticSink _diagnostics;
    private readonly int _recentCapacity;
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
        var completion = RegisterPrimaryDeferred(primary, cancellationToken);
        _ = StartTimeout(primary.SystemBytes, t3, cancellationToken);
        return completion;
    }

    internal Task<SecsMessage> RegisterPrimaryDeferred(SecsMessage primary, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(primary); ThrowIfDisposed();
        if (!primary.Function.IsPrimary || !primary.ReplyExpected) throw new ArgumentException("A transaction requires a primary message with W-bit set.", nameof(primary));
        cancellationToken.ThrowIfCancellationRequested();
        var pending = new PendingTransaction(primary);
        if (!_pending.TryAdd(primary.SystemBytes.Value, pending)) throw new InvalidOperationException($"System Bytes 0x{primary.SystemBytes.Value:X8} is already open.");
        return pending.Completion.Task;
    }

    internal bool StartTimeout(SecsSystemBytes systemBytes, TimeSpan t3, CancellationToken cancellationToken)
    {
        if (t3 <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(t3));
        if (!_pending.TryGetValue(systemBytes.Value, out var pending)) return false;
        if (Interlocked.Exchange(ref pending.MonitorStarted, 1) != 0) throw new InvalidOperationException("The transaction timeout was already started.");
        BackgroundTaskObserver.Observe(MonitorAsync(pending, t3, cancellationToken), "T3 transaction monitor");
        return true;
    }

    /// <summary>\if KO 수신 Secondary로 열린 transaction을 완료합니다. \endif \if EN Completes an open transaction with an inbound secondary. \endif</summary>
    /// <param name="secondary">\if KO secondary 메시지입니다. \endif \if EN Secondary message. \endif</param><returns>\if KO 상관관계 결과입니다. \endif \if EN Correlation result. \endif</returns>
    public SecsTransactionCompletionStatus TryComplete(SecsMessage secondary)
    {
        ArgumentNullException.ThrowIfNull(secondary); ThrowIfDisposed();
        if (!_pending.TryGetValue(secondary.SystemBytes.Value, out var pending))
        {
            lock (_recentGate) return _recent.Contains(secondary.SystemBytes.Value) ? SecsTransactionCompletionStatus.Duplicate : SecsTransactionCompletionStatus.UnknownSystemBytes;
        }
        var primary = pending.Primary;
        var validReplyFunction = secondary.Function.Value == 0 ||
            (secondary.Function.IsSecondary && secondary.Function.Value == primary.Function.Value + 1);
        if (secondary.ReplyExpected || !validReplyFunction ||
            secondary.SessionId != primary.SessionId || secondary.Stream != primary.Stream)
            return SecsTransactionCompletionStatus.InvalidCorrelation;
        if (!_pending.TryRemove(secondary.SystemBytes.Value, out pending))
            return IsRecent(secondary.SystemBytes.Value) ? SecsTransactionCompletionStatus.Duplicate : SecsTransactionCompletionStatus.UnknownSystemBytes;
        pending.Lifetime.Cancel();
        AddRecent(secondary.SystemBytes.Value);
        pending.Completion.TrySetResult(secondary);
        pending.Lifetime.Dispose();
        _diagnostics.Emit(new(SecsDiagnosticKind.SecondaryReceived, $"Secondary completed transaction 0x{secondary.SystemBytes.Value:X8}."));
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
            pending.Lifetime.Cancel();
            pending.Completion.TrySetException(error);
            pending.Lifetime.Dispose();
        }
    }

    /// <summary>\if KO 특정 transaction만 오류로 종료합니다. \endif \if EN Terminates only the specified transaction with an error. \endif</summary>
    /// <param name="systemBytes">\if KO 종료할 System Bytes입니다. \endif \if EN System Bytes to terminate. \endif</param><param name="error">\if KO 전달할 오류입니다. \endif \if EN Error to deliver. \endif</param><returns>\if KO transaction을 제거했는지 여부입니다. \endif \if EN Whether a transaction was removed. \endif</returns>
    public bool TryAbort(SecsSystemBytes systemBytes, Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (!_pending.TryRemove(systemBytes.Value, out var pending)) return false;
        pending.Lifetime.Cancel();
        pending.Completion.TrySetException(error);
        pending.Lifetime.Dispose();
        return true;
    }

    /// <summary>\if KO 모든 대기를 취소하고 manager를 해제합니다. \endif \if EN Cancels all waiters and disposes the manager. \endif</summary>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0) AbortAll(new ObjectDisposedException(nameof(SecsTransactionManager)));
        return ValueTask.CompletedTask;
    }

    private async Task MonitorAsync(PendingTransaction pending, TimeSpan t3, CancellationToken callerCancellation)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(pending.Lifetime.Token, callerCancellation);
        try
        {
            await Task.Delay(t3, _timeProvider, linked.Token).ConfigureAwait(false);
            if (_pending.TryRemove(pending.Primary.SystemBytes.Value, out _))
            {
                AddRecent(pending.Primary.SystemBytes.Value);
                var error = new SecsTransactionTimeoutException(pending.Primary.SystemBytes, t3);
                pending.Completion.TrySetException(error);
                _diagnostics.Emit(new(SecsDiagnosticKind.Timeout, $"T3 expired for 0x{pending.Primary.SystemBytes.Value:X8}."));
                pending.Lifetime.Dispose();
            }
        }
        catch (OperationCanceledException) when (callerCancellation.IsCancellationRequested)
        {
            if (_pending.TryRemove(pending.Primary.SystemBytes.Value, out _))
            {
                pending.Completion.TrySetCanceled(callerCancellation);
                pending.Lifetime.Dispose();
            }
        }
        catch (OperationCanceledException) when (pending.Lifetime.IsCancellationRequested) { }
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

    private sealed class PendingTransaction(SecsMessage primary)
    {
        public SecsMessage Primary { get; } = primary;
        public TaskCompletionSource<SecsMessage> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationTokenSource Lifetime { get; } = new();
        public int MonitorStarted;
    }
}
