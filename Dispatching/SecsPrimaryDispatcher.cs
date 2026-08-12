using System.Threading.Channels;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.Secs.Com.Diagnostics;

namespace Dreamine.Secs.Com.Dispatching;

/// <summary>
/// \if KO <para>고정 worker와 bounded queue로 inbound Primary를 exact/fallback handler에 전달합니다.</para> \endif
/// \if EN <para>Routes inbound primaries to exact/fallback handlers using fixed workers and a bounded queue.</para> \endif
/// </summary>
internal sealed class SecsPrimaryDispatcher : ISecsPrimaryDispatcher
{
    private static readonly AsyncLocal<SecsPrimaryDispatcher?> CurrentWorker = new();
    private readonly object _registrationGate = new();
    private readonly Dictionary<DialogueKey, Registration> _registrations = new();
    private readonly Channel<DispatchWorkItem> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ISecsDiagnosticSink _diagnostics;
    private Registration? _fallback;
    private long _droppedCount;
    private int _stopping;
    private int _lifetimeDisposed;

    /// <summary>\if KO dispatcher 설정 및 진단 경계로 instance를 만듭니다. \endif \if EN Creates an instance with dispatcher settings and a diagnostic boundary. \endif</summary>
    public SecsPrimaryDispatcher(SecsPrimaryDispatcherOptions options, ISecsDiagnosticSink diagnostics)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(diagnostics);
        options.Validate();
        _diagnostics = diagnostics;
        _queue = Channel.CreateBounded<DispatchWorkItem>(new BoundedChannelOptions(options.QueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = options.MaximumConcurrency == 1,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        _workers = new Task[options.MaximumConcurrency];
        for (var index = 0; index < _workers.Length; index++) _workers[index] = WorkerAsync();
    }

    /// <summary>\if KO queue 포화 또는 정지 중 claim된 뒤 버려진 Primary 수입니다. \endif \if EN Gets the number of claimed primaries dropped because the queue was full or stopping. \endif</summary>
    public long DroppedCount => Interlocked.Read(ref _droppedCount);

    /// <inheritdoc />
    public long DroppedPrimaryCount => DroppedCount;

    /// <inheritdoc />
    public IDisposable Register(
        SecsDialogueDefinition dialogue,
        Func<ISecsPrimaryContext, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(dialogue);
        ArgumentNullException.ThrowIfNull(handler);
        var registration = new Registration(this, dialogue, handler, isFallback: false);
        lock (_registrationGate)
        {
            ThrowIfStopping();
            if (!_registrations.TryAdd(new(dialogue.Stream.Value, dialogue.PrimaryFunction.Value), registration))
            {
                registration.DisposeDetached();
                throw new InvalidOperationException($"A Primary handler is already registered for S{dialogue.Stream.Value}F{dialogue.PrimaryFunction.Value}.");
            }
        }
        return registration;
    }

    /// <inheritdoc />
    public IDisposable RegisterFallback(Func<ISecsPrimaryContext, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var registration = new Registration(this, dialogue: null, handler, isFallback: true);
        lock (_registrationGate)
        {
            ThrowIfStopping();
            if (_fallback is not null)
            {
                registration.DisposeDetached();
                throw new InvalidOperationException("A fallback Primary handler is already registered.");
            }
            _fallback = registration;
        }
        return registration;
    }

    /// <summary>
    /// \if KO <para>exact 우선, fallback 차선으로 Primary를 claim합니다. 포화 시에도 claim은 유지됩니다.</para> \endif
    /// \if EN <para>Claims a primary using exact-first, fallback-second routing. A full queue still retains the claim.</para> \endif
    /// </summary>
    internal PrimaryDispatchClaim TryDispatch(
        SecsMessage primary,
        SecsConnectionIdentity connectionIdentity,
        CancellationToken connectionCancellation,
        Func<SecsDialogueDefinition, SecsMessage, SecsItem?, CancellationToken, ValueTask> reply)
    {
        ArgumentNullException.ThrowIfNull(primary);
        ArgumentNullException.ThrowIfNull(connectionIdentity);
        ArgumentNullException.ThrowIfNull(reply);

        if (Volatile.Read(ref _stopping) != 0)
        {
            RecordDrop(primary);
            return PrimaryDispatchClaim.ClaimedDropped;
        }

        Registration? registration;
        var stoppingWithoutRegistration = false;
        lock (_registrationGate)
        {
            _registrations.TryGetValue(new(primary.Stream.Value, primary.Function.Value), out registration);
            registration ??= _fallback;
            if (registration is null)
            {
                stoppingWithoutRegistration = Volatile.Read(ref _stopping) != 0;
                if (!stoppingWithoutRegistration) return PrimaryDispatchClaim.Unclaimed;
            }
            else if (!registration.TryAcquire()) return PrimaryDispatchClaim.ClaimedDropped;
        }

        if (stoppingWithoutRegistration)
        {
            RecordDrop(primary);
            return PrimaryDispatchClaim.ClaimedDropped;
        }
        var claimedRegistration = registration ?? throw new InvalidOperationException("A claimed Primary has no dispatcher registration.");

        var context = new PrimaryContext(
            connectionIdentity,
            primary,
            claimedRegistration.IsFallback ? null : claimedRegistration.Dialogue,
            reply);
        if (!claimedRegistration.IsFallback && claimedRegistration.Dialogue is { } dialogue &&
            dialogue.ReplyExpected != primary.ReplyExpected)
        {
            _diagnostics.Emit(new(
                SecsDiagnosticKind.ApplicationError,
                $"SECS Primary S{primary.Stream.Value}F{primary.Function.Value} was claimed with W-bit {primary.ReplyExpected}, which does not match its registered dialogue."));
        }
        var work = new DispatchWorkItem(claimedRegistration, context, connectionCancellation);
        if (Volatile.Read(ref _stopping) == 0 && _queue.Writer.TryWrite(work))
            return PrimaryDispatchClaim.Claimed;

        claimedRegistration.Release();
        RecordDrop(primary);
        return PrimaryDispatchClaim.ClaimedDropped;
    }

    /// <summary>\if KO future claim을 중지하고 worker drain을 기다립니다. worker 내부 재진입은 자신을 기다리지 않습니다. \endif \if EN Stops future claims and waits for worker drain; worker reentrancy never waits on itself. \endif</summary>
    internal async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            Registration[] registrations;
            lock (_registrationGate)
            {
                registrations = _registrations.Values
                    .Append(_fallback)
                    .Where(static item => item is not null)
                    .Cast<Registration>()
                    .Distinct()
                    .ToArray();
                _registrations.Clear();
                _fallback = null;
            }
            foreach (var registration in registrations) registration.DisposeDetached();
            _lifetime.Cancel();
            _queue.Writer.TryComplete();
        }

        var drain = Task.WhenAll(_workers);
        if (ReferenceEquals(CurrentWorker.Value, this))
        {
            _ = BackgroundTaskObserver.Observe(drain, "reentrant Primary dispatcher shutdown");
            _ = DisposeLifetimeAfterDrainAsync(drain);
            return;
        }
        try
        {
            await drain.WaitAsync(cancellationToken).ConfigureAwait(false);
            DisposeLifetime();
        }
        catch
        {
            _ = BackgroundTaskObserver.Observe(drain, "deferred Primary dispatcher drain");
            _ = DisposeLifetimeAfterDrainAsync(drain);
            throw;
        }
    }

    private async Task WorkerAsync()
    {
        try
        {
            await foreach (var work in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                        _lifetime.Token,
                        work.ConnectionCancellation,
                        work.Registration.CancellationToken);
                    if (linked.IsCancellationRequested) continue;
                    var previous = CurrentWorker.Value;
                    CurrentWorker.Value = this;
                    try { await work.Registration.Handler(work.Context, linked.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
                    catch (Exception exception)
                    {
                        _diagnostics.Emit(new(
                            SecsDiagnosticKind.ApplicationError,
                            $"SECS Primary handler failed: {exception.Message}"));
                    }
                    finally { CurrentWorker.Value = previous; }
                }
                finally { work.Registration.Release(); }
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception exception)
        {
            _diagnostics.Emit(new(SecsDiagnosticKind.ApplicationError, $"SECS Primary dispatcher worker failed: {exception.Message}"));
        }
    }

    private async Task DisposeLifetimeAfterDrainAsync(Task drain)
    {
        try { await drain.ConfigureAwait(false); }
        catch { _ = drain.Exception; }
        finally { DisposeLifetime(); }
    }

    private void DisposeLifetime()
    {
        if (Interlocked.Exchange(ref _lifetimeDisposed, 1) == 0) _lifetime.Dispose();
    }

    private void RecordDrop(SecsMessage primary)
    {
        Interlocked.Increment(ref _droppedCount);
        _diagnostics.Emit(new(
            SecsDiagnosticKind.ApplicationError,
            $"SECS Primary S{primary.Stream.Value}F{primary.Function.Value} was claimed but dropped because the bounded dispatcher queue was unavailable."));
    }

    private void Remove(Registration registration)
    {
        lock (_registrationGate)
        {
            if (registration.IsFallback)
            {
                if (ReferenceEquals(_fallback, registration)) _fallback = null;
            }
            else if (registration.Dialogue is { } dialogue)
            {
                var key = new DialogueKey(dialogue.Stream.Value, dialogue.PrimaryFunction.Value);
                if (_registrations.TryGetValue(key, out var current) && ReferenceEquals(current, registration))
                    _registrations.Remove(key);
            }
        }
        registration.DisposeDetached();
    }

    private void ThrowIfStopping() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _stopping) != 0, this);

    private readonly record struct DialogueKey(byte Stream, byte Function);
    private readonly record struct DispatchWorkItem(
        Registration Registration,
        PrimaryContext Context,
        CancellationToken ConnectionCancellation);

    private sealed class Registration(
        SecsPrimaryDispatcher owner,
        SecsDialogueDefinition? dialogue,
        Func<ISecsPrimaryContext, CancellationToken, ValueTask> handler,
        bool isFallback) : IDisposable
    {
        private readonly CancellationTokenSource _lifetime = new();
        private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _active;
        private int _disposed;

        public SecsDialogueDefinition? Dialogue { get; } = dialogue;
        public Func<ISecsPrimaryContext, CancellationToken, ValueTask> Handler { get; } = handler;
        public bool IsFallback { get; } = isFallback;
        public CancellationToken CancellationToken => _lifetime.Token;

        public bool TryAcquire()
        {
            if (Volatile.Read(ref _disposed) != 0) return false;
            Interlocked.Increment(ref _active);
            if (Volatile.Read(ref _disposed) == 0) return true;
            Release();
            return false;
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _active) == 0 && Volatile.Read(ref _disposed) != 0)
                CompleteDrain();
        }

        public void Dispose() => owner.Remove(this);

        public void DisposeDetached()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            _lifetime.Cancel();
            if (Volatile.Read(ref _active) == 0) CompleteDrain();
        }

        private void CompleteDrain()
        {
            _drained.TrySetResult();
            if (_drained.Task.IsCompleted) _lifetime.Dispose();
        }
    }

    private sealed class PrimaryContext : ISecsPrimaryContext
    {
        private readonly SecsDialogueDefinition? _dialogue;
        private readonly Func<SecsDialogueDefinition, SecsMessage, SecsItem?, CancellationToken, ValueTask> _reply;
        private int _replyClaimed;

        public PrimaryContext(
            SecsConnectionIdentity connectionIdentity,
            SecsMessage primary,
            SecsDialogueDefinition? dialogue,
            Func<SecsDialogueDefinition, SecsMessage, SecsItem?, CancellationToken, ValueTask> reply)
        {
            ConnectionIdentity = connectionIdentity;
            Primary = primary;
            _dialogue = dialogue;
            _reply = reply;
        }

        public SecsConnectionIdentity ConnectionIdentity { get; }
        public SecsMessage Primary { get; }
        public bool CanReply => _dialogue is { ReplyExpected: true } && Primary.ReplyExpected;

        public ValueTask ReplyAsync(SecsItem? item = null, CancellationToken cancellationToken = default)
        {
            if (!CanReply || _dialogue is null)
                throw new InvalidOperationException("Only an exact W-bit Primary registration can send a normal Secondary reply.");
            if (Interlocked.CompareExchange(ref _replyClaimed, 1, 0) != 0)
                throw new InvalidOperationException("The Primary reply was already claimed.");
            return _reply(_dialogue, Primary, item, cancellationToken);
        }
    }
}

/// <summary>\if KO Primary dispatcher의 claim 결과입니다. \endif \if EN Identifies a Primary dispatcher claim result. \endif</summary>
internal enum PrimaryDispatchClaim
{
    Unclaimed,
    Claimed,
    ClaimedDropped
}
