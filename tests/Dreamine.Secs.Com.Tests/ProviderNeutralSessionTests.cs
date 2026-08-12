using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Diagnostics;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.Secs.Com.Hsms;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class ProviderNeutralSessionTests
{
    [Fact]
    public async Task ProviderKeepsLegacyFactoryAndAddsTypedSessionFactory()
    {
        var provider = new DreamineSecsCommunicationProvider(options => new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = ReservePort(),
            Mode = options.Mode,
            Role = options.Role,
            SessionId = new SecsSessionId(17)
        });
        var options = new SecsConnectionOptions
        {
            ProviderKey = provider.Key,
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host
        };

        var typedProvider = Assert.IsAssignableFrom<ISecsMessageSessionProvider>(provider);
        await using var typed = typedProvider.CreateSession(options);
        await using var legacy = provider.CreateConnection(options);

        Assert.IsType<HsmsSession>(typed);
        Assert.IsType<HsmsSession>(legacy);
        Assert.Equal(new SecsSessionId(17), typed.ConnectionIdentity.SessionId);
    }

    [Fact]
    public async Task ProviderPreservesPrimaryDispatcherOptionsWhenCloningHsmsOptions()
    {
        var dispatcherOptions = new SecsPrimaryDispatcherOptions
        {
            QueueCapacity = 7,
            MaximumConcurrency = 3
        };
        var provider = new DreamineSecsCommunicationProvider(options => new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = ReservePort(),
            Mode = options.Mode,
            Role = options.Role,
            SessionId = new SecsSessionId(19),
            PrimaryDispatcher = dispatcherOptions
        });
        var connectionOptions = new SecsConnectionOptions
        {
            ProviderKey = provider.Key,
            Mode = SecsConnectionMode.Passive,
            Role = SecsRole.Equipment
        };

        await using var session = Assert.IsType<HsmsSession>(
            Assert.IsAssignableFrom<ISecsMessageSessionProvider>(provider).CreateSession(connectionOptions));
        var optionsField = typeof(HsmsSession).GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic) ??
            throw new MissingFieldException(typeof(HsmsSession).FullName, "_options");
        var cloned = Assert.IsType<HsmsSessionOptions>(optionsField.GetValue(session));

        Assert.Same(dispatcherOptions, cloned.PrimaryDispatcher);
        Assert.Equal(7, cloned.PrimaryDispatcher.QueueCapacity);
        Assert.Equal(3, cloned.PrimaryDispatcher.MaximumConcurrency);
    }

    [Fact]
    public async Task ConnectionIdentityKeepsInstanceIdAndAdvancesEpochAfterReconnect()
    {
        await using var pair = await LoopbackPair.CreateAsync(sessionId: 27);
        var instanceId = pair.Equipment.ConnectionIdentity.SessionInstanceId;
        var firstEpoch = pair.Equipment.ConnectionIdentity.ConnectionEpoch;

        Assert.True(firstEpoch > 0);
        Assert.Equal(new SecsSessionId(27), pair.Equipment.ConnectionIdentity.SessionId);

        await pair.ReconnectAsync();

        Assert.Equal(instanceId, pair.Equipment.ConnectionIdentity.SessionInstanceId);
        Assert.True(pair.Equipment.ConnectionIdentity.ConnectionEpoch > firstEpoch);
    }

    [Fact]
    public async Task SafeW0UsesConfiguredSessionIdAndAutomaticSystemBytes()
    {
        await using var pair = await LoopbackPair.CreateAsync(sessionId: 31);
        var received = new TaskCompletionSource<SecsMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        pair.Equipment.MessageReceived += (_, message) => received.TrySetResult(message);

        await pair.Host.SendAsync(new SecsStream(2), new SecsFunction(3), new SecsAsciiItem("W0"), pair.Token);
        var message = await received.Task.WaitAsync(pair.Token);

        Assert.Equal(new SecsSessionId(31), message.SessionId);
        Assert.Equal((byte)2, message.Stream.Value);
        Assert.Equal((byte)3, message.Function.Value);
        Assert.False(message.ReplyExpected);
        Assert.NotEqual(0u, message.SystemBytes.Value);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 0)]
    [InlineData(1, 2)]
    public async Task SafeW0RejectsNonNormalPrimaryBeforeWriting(byte stream, byte function)
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var legacyCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);

        await Assert.ThrowsAnyAsync<ArgumentException>(() => pair.Host.SendAsync(
            new SecsStream(stream), new SecsFunction(function), cancellationToken: pair.Token));

        Assert.Equal(0, Volatile.Read(ref legacyCount));
    }

    [Fact]
    public async Task SafeW1UsesDeclaredNormalSecondaryAndConfiguredSessionId()
    {
        await using var pair = await LoopbackPair.CreateAsync(sessionId: 43);
        var dialogue = new SecsDialogueDefinition(new SecsStream(7), new SecsFunction(5), new SecsFunction(6));
        SecsConnectionIdentity? source = null;
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (context, cancellationToken) =>
        {
            source = context.ConnectionIdentity;
            await context.ReplyAsync(new SecsAsciiItem("OK"), cancellationToken);
        });

        var response = await pair.Host.RequestAsync(dialogue, new SecsAsciiItem("REQ"), pair.Token);

        Assert.Equal(new SecsSessionId(43), response.SessionId);
        Assert.Equal((byte)7, response.Stream.Value);
        Assert.Equal((byte)6, response.Function.Value);
        Assert.False(response.ReplyExpected);
        Assert.Equal("OK", Assert.IsType<SecsAsciiItem>(response.Item).Value);
        Assert.NotNull(source);
        Assert.True(source!.ConnectionEpoch > 0);
    }

    [Fact]
    public async Task ConcurrentSafeRequestsAllocateUniqueSystemBytesAndCorrelateOutOfOrder()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(3), new SecsFunction(1), new SecsFunction(2));
        var captured = new List<ISecsPrimaryContext>();
        var gate = new object();
        var allCaptured = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, (context, _) =>
        {
            lock (gate)
            {
                captured.Add(context);
                if (captured.Count == 16) allCaptured.TrySetResult();
            }
            return ValueTask.CompletedTask;
        });

        var pending = Enumerable.Range(0, 16)
            .Select(index => pair.Host.RequestAsync(dialogue, new SecsUInt16Item((ushort)index), pair.Token))
            .ToArray();
        await allCaptured.Task.WaitAsync(pair.Token);
        ISecsPrimaryContext[] contexts;
        lock (gate) contexts = captured.ToArray();
        foreach (var context in contexts.Reverse())
            await context.ReplyAsync(context.Primary.Item, pair.Token);

        var responses = await Task.WhenAll(pending);
        Assert.Equal(16, responses.Select(static response => response.SystemBytes.Value).Distinct().Count());
        Assert.Equal(Enumerable.Range(0, 16), responses.Select(static response => (int)Assert.IsType<SecsUInt16Item>(response.Item).Values.Span[0]));
    }

    [Fact]
    public async Task FunctionZeroStillTerminatesAnExactSafeRequest()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(1), new SecsFunction(1), new SecsFunction(2));
        pair.Equipment.MessageReceived += (_, primary) => pair.Equipment.SendAsync(new SecsMessage(
            primary.SessionId,
            primary.Stream,
            new SecsFunction(0),
            false,
            primary.SystemBytes)).GetAwaiter().GetResult();

        var response = await pair.Host.RequestAsync(dialogue, cancellationToken: pair.Token);

        Assert.Equal((byte)0, response.Function.Value);
    }

    [Fact]
    public async Task WrongSecondaryFunctionDoesNotConsumeSafeRequest()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(1), new SecsFunction(1), new SecsFunction(2));
        using var releaseCorrect = new ManualResetEventSlim();
        var wrongSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFailure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        pair.Equipment.MessageReceived += (_, primary) =>
        {
            try
            {
                pair.Equipment.SendAsync(new SecsMessage(primary.SessionId, primary.Stream, new SecsFunction(4), false, primary.SystemBytes))
                    .GetAwaiter().GetResult();
                wrongSent.TrySetResult();
                if (!releaseCorrect.Wait(TimeSpan.FromSeconds(5))) throw new TimeoutException("Correct reply release was not signaled.");
                pair.Equipment.SendAsync(new SecsMessage(primary.SessionId, primary.Stream, new SecsFunction(2), false, primary.SystemBytes))
                    .GetAwaiter().GetResult();
            }
            catch (Exception exception) { handlerFailure.TrySetResult(exception); }
        };

        var pending = pair.Host.RequestAsync(dialogue, cancellationToken: pair.Token);
        await wrongSent.Task.WaitAsync(pair.Token);
        Assert.False(pending.IsCompleted);
        releaseCorrect.Set();

        Assert.Equal((byte)2, (await pending).Function.Value);
        Assert.False(handlerFailure.Task.IsCompleted);
    }

    [Fact]
    public async Task ExactHandlerClaimsPrimaryAndReplyIsOneShotWithSourceMetadata()
    {
        await using var pair = await LoopbackPair.CreateAsync(sessionId: 53);
        var dialogue = new SecsDialogueDefinition(new SecsStream(5), new SecsFunction(9), new SecsFunction(10));
        var legacyCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        var duplicateFailure = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (context, cancellationToken) =>
        {
            Assert.True(context.CanReply);
            Assert.Equal(new SecsSessionId(53), context.ConnectionIdentity.SessionId);
            Assert.Equal(context.Primary.SessionId, context.ConnectionIdentity.SessionId);
            await context.ReplyAsync(new SecsBinaryItem(0), cancellationToken);
            duplicateFailure.TrySetResult(await Record.ExceptionAsync(() => context.ReplyAsync(cancellationToken: cancellationToken).AsTask()));
        });

        var response = await pair.Host.RequestAsync(dialogue, cancellationToken: pair.Token);

        Assert.Equal((byte)10, response.Function.Value);
        Assert.False(response.ReplyExpected);
        Assert.Equal(0, Volatile.Read(ref legacyCount));
        Assert.IsType<InvalidOperationException>(await duplicateFailure.Task.WaitAsync(pair.Token));
    }

    [Fact]
    public async Task ExactHandlerWinsOverFallback()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(6), new SecsFunction(1));
        var exact = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fallbackCount = 0;
        using var exactRegistration = pair.Equipment.PrimaryDispatcher.Register(dialogue, (_, _) =>
        {
            exact.TrySetResult();
            return ValueTask.CompletedTask;
        });
        using var fallback = pair.Equipment.PrimaryDispatcher.RegisterFallback((_, _) =>
        {
            Interlocked.Increment(ref fallbackCount);
            return ValueTask.CompletedTask;
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await exact.Task.WaitAsync(pair.Token);

        Assert.Equal(0, Volatile.Read(ref fallbackCount));
    }

    [Fact]
    public async Task FallbackClaimsUnregisteredPrimaryButCannotReply()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var failure = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var legacyCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        using var fallback = pair.Equipment.PrimaryDispatcher.RegisterFallback(async (context, cancellationToken) =>
        {
            Assert.False(context.CanReply);
            failure.TrySetResult(await Record.ExceptionAsync(() => context.ReplyAsync(cancellationToken: cancellationToken).AsTask()));
        });

        await pair.Host.SendAsync(new SecsStream(9), new SecsFunction(1), cancellationToken: pair.Token);

        Assert.IsType<InvalidOperationException>(await failure.Task.WaitAsync(pair.Token));
        Assert.Equal(0, Volatile.Read(ref legacyCount));
    }

    [Fact]
    public async Task W1RegistrationClaimsActualW0ButCannotReplyAndReportsMismatch()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(8), new SecsFunction(1), new SecsFunction(2));
        var handled = new TaskCompletionSource<(bool CanReply, bool ActualW, Exception? Failure)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var diagnostic = new TaskCompletionSource<SecsDiagnosticEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var legacyCount = 0;
        var fallbackCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        pair.Equipment.DiagnosticReceived += (_, value) =>
        {
            if (value.Kind == SecsDiagnosticKind.ApplicationError && value.Message.Contains("does not match", StringComparison.Ordinal))
                diagnostic.TrySetResult(value);
        };
        using var fallback = pair.Equipment.PrimaryDispatcher.RegisterFallback((_, _) =>
        {
            Interlocked.Increment(ref fallbackCount);
            return ValueTask.CompletedTask;
        });
        using var exact = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (context, _) =>
        {
            var failure = await Record.ExceptionAsync(() => context.ReplyAsync().AsTask());
            handled.TrySetResult((context.CanReply, context.Primary.ReplyExpected, failure));
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);

        var result = await handled.Task.WaitAsync(pair.Token);
        Assert.False(result.CanReply);
        Assert.False(result.ActualW);
        Assert.IsType<InvalidOperationException>(result.Failure);
        Assert.Equal(SecsDiagnosticKind.ApplicationError, (await diagnostic.Task.WaitAsync(pair.Token)).Kind);
        Assert.Equal(0, Volatile.Read(ref fallbackCount));
        Assert.Equal(0, Volatile.Read(ref legacyCount));
    }

    [Fact]
    public async Task W0RegistrationClaimsActualW1ButCannotReplyAndReportsMismatch()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(8), new SecsFunction(3));
        var handled = new TaskCompletionSource<(bool CanReply, bool ActualW)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var diagnostic = new TaskCompletionSource<SecsDiagnosticEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var legacyCount = 0;
        var fallbackCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        pair.Equipment.DiagnosticReceived += (_, value) =>
        {
            if (value.Kind == SecsDiagnosticKind.ApplicationError && value.Message.Contains("does not match", StringComparison.Ordinal))
                diagnostic.TrySetResult(value);
        };
        using var fallback = pair.Equipment.PrimaryDispatcher.RegisterFallback((_, _) =>
        {
            Interlocked.Increment(ref fallbackCount);
            return ValueTask.CompletedTask;
        });
        using var exact = pair.Equipment.PrimaryDispatcher.Register(dialogue, (context, _) =>
        {
            handled.TrySetResult((context.CanReply, context.Primary.ReplyExpected));
            return ValueTask.CompletedTask;
        });
        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(pair.Token);
        var request = new SecsMessage(
            pair.Host.ConnectionIdentity.SessionId,
            dialogue.Stream,
            dialogue.PrimaryFunction,
            replyExpected: true,
            pair.Host.AllocateSystemBytes());

        var pending = pair.Host.SendPrimaryAsync(request, requestCancellation.Token);
        var result = await handled.Task.WaitAsync(pair.Token);
        requestCancellation.Cancel();

        Assert.False(result.CanReply);
        Assert.True(result.ActualW);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(SecsDiagnosticKind.ApplicationError, (await diagnostic.Task.WaitAsync(pair.Token)).Kind);
        Assert.Equal(0, Volatile.Read(ref fallbackCount));
        Assert.Equal(0, Volatile.Read(ref legacyCount));
    }

    [Fact]
    public async Task FullDispatcherQueueDropsNewestWithoutLegacyFallthrough()
    {
        await using var pair = await LoopbackPair.CreateAsync(equipmentDispatcher: new SecsPrimaryDispatcherOptions
        {
            QueueCapacity = 1,
            MaximumConcurrency = 1
        });
        var dialogue = new SecsDialogueDefinition(new SecsStream(10), new SecsFunction(1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = 0;
        var legacyCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (_, cancellationToken) =>
        {
            if (Interlocked.Increment(ref handled) == 1)
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(cancellationToken);
            }
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await entered.Task.WaitAsync(pair.Token);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await WaitUntilAsync(() => pair.Equipment.PrimaryDispatcher.DroppedPrimaryCount == 1, pair.Token);

        Assert.Equal(0, Volatile.Read(ref legacyCount));
        release.TrySetResult();
        await WaitUntilAsync(() => Volatile.Read(ref handled) == 2, pair.Token);
    }

    [Fact]
    public async Task SlowPrimaryHandlerDoesNotBlockSecondaryCorrelationOrLinktest()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var slowDialogue = new SecsDialogueDefinition(new SecsStream(11), new SecsFunction(1));
        var requestDialogue = new SecsDialogueDefinition(new SecsStream(12), new SecsFunction(1), new SecsFunction(2));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var slow = pair.Equipment.PrimaryDispatcher.Register(slowDialogue, async (_, cancellationToken) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(cancellationToken);
        });
        using var reply = pair.Host.PrimaryDispatcher.Register(requestDialogue, (context, cancellationToken) =>
            context.ReplyAsync(new SecsAsciiItem("FREE"), cancellationToken));

        await pair.Host.SendAsync(slowDialogue.Stream, slowDialogue.PrimaryFunction, cancellationToken: pair.Token);
        await entered.Task.WaitAsync(pair.Token);

        var response = await pair.Equipment.RequestAsync(requestDialogue, cancellationToken: pair.Token);
        await pair.Host.LinktestAsync(pair.Token);

        Assert.Equal("FREE", Assert.IsType<SecsAsciiItem>(response.Item).Value);
        release.TrySetResult();
    }

    [Fact]
    public async Task AcceptedPrimariesRemainFifoAndOverflowDropsNewest()
    {
        await using var pair = await LoopbackPair.CreateAsync(equipmentDispatcher: new SecsPrimaryDispatcherOptions
        {
            QueueCapacity = 2,
            MaximumConcurrency = 1
        });
        var dialogue = new SecsDialogueDefinition(new SecsStream(9), new SecsFunction(3));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<ushort>();
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (context, cancellationToken) =>
        {
            var value = Assert.IsType<SecsUInt16Item>(context.Primary.Item).Values.Span[0];
            handled.Enqueue(value);
            if (value == 1)
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(cancellationToken);
            }
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(1), pair.Token);
        await entered.Task.WaitAsync(pair.Token);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(2), pair.Token);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(3), pair.Token);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(4), pair.Token);
        await WaitUntilAsync(() => pair.Equipment.PrimaryDispatcher.DroppedPrimaryCount == 1, pair.Token);

        release.TrySetResult();
        await WaitUntilAsync(() => handled.Count == 3, pair.Token);

        Assert.Equal(new ushort[] { 1, 2, 3 }, handled);
    }

    [Fact]
    public async Task ConfiguredConcurrencyIsReachedButNeverExceededAndOverflowStillDropsNewest()
    {
        await using var pair = await LoopbackPair.CreateAsync(equipmentDispatcher: new SecsPrimaryDispatcherOptions
        {
            QueueCapacity = 3,
            MaximumConcurrency = 2
        });
        var dialogue = new SecsDialogueDefinition(new SecsStream(9), new SecsFunction(11));
        var twoEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseInitial = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<ushort>();
        var active = 0;
        var maximumActive = 0;
        var handledCount = 0;
        var legacyCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (context, cancellationToken) =>
        {
            var currentActive = Interlocked.Increment(ref active);
            UpdateMaximum(ref maximumActive, currentActive);
            var value = Assert.IsType<SecsUInt16Item>(context.Primary.Item).Values.Span[0];
            handled.Enqueue(value);
            try
            {
                if (currentActive == 2) twoEntered.TrySetResult();
                if (value <= 2) await releaseInitial.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref active);
                if (Interlocked.Increment(ref handledCount) == 5) allHandled.TrySetResult();
            }
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(1), pair.Token);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(2), pair.Token);
        await twoEntered.Task.WaitAsync(pair.Token);
        for (ushort value = 3; value <= 6; value++)
            await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, new SecsUInt16Item(value), pair.Token);
        await WaitUntilAsync(() => pair.Equipment.PrimaryDispatcher.DroppedPrimaryCount == 1, pair.Token);

        releaseInitial.TrySetResult();
        await allHandled.Task.WaitAsync(pair.Token);

        Assert.Equal(2, Volatile.Read(ref maximumActive));
        Assert.Equal(new ushort[] { 1, 2, 3, 4, 5 }, handled.Order());
        Assert.DoesNotContain((ushort)6, handled);
        Assert.Equal(0, Volatile.Read(ref legacyCount));
    }

    [Fact]
    public async Task DisposedRegistrationCancelsActiveHandlerAndAllowsReplacement()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(9), new SecsFunction(5));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var replacementHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (_, cancellationToken) =>
        {
            entered.TrySetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { canceled.TrySetResult(); }
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await entered.Task.WaitAsync(pair.Token);
        first.Dispose();
        await canceled.Task.WaitAsync(pair.Token);

        using var replacement = pair.Equipment.PrimaryDispatcher.Register(dialogue, (_, _) =>
        {
            replacementHandled.TrySetResult();
            return ValueTask.CompletedTask;
        });
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);

        await replacementHandled.Task.WaitAsync(pair.Token);
    }

    [Fact]
    public async Task DuplicateExactAndFallbackRegistrationsAreRejectedUntilDisposed()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(9), new SecsFunction(7));
        static ValueTask Ignore(ISecsPrimaryContext _, CancellationToken __) => ValueTask.CompletedTask;
        var exact = pair.Equipment.PrimaryDispatcher.Register(dialogue, Ignore);
        var fallback = pair.Equipment.PrimaryDispatcher.RegisterFallback(Ignore);

        Assert.Throws<InvalidOperationException>(() => pair.Equipment.PrimaryDispatcher.Register(dialogue, Ignore));
        Assert.Throws<InvalidOperationException>(() => pair.Equipment.PrimaryDispatcher.RegisterFallback(Ignore));

        exact.Dispose();
        fallback.Dispose();
        using var replacementExact = pair.Equipment.PrimaryDispatcher.Register(dialogue, Ignore);
        using var replacementFallback = pair.Equipment.PrimaryDispatcher.RegisterFallback(Ignore);
    }

    [Fact]
    public async Task StoppingDispatcherClaimsAndDropsWithoutLegacyFallthrough()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(9), new SecsFunction(9));
        var legacyCount = 0;
        pair.Equipment.MessageReceived += (_, _) => Interlocked.Increment(ref legacyCount);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, (_, _) => ValueTask.CompletedTask);
        var stopMethod = pair.Equipment.PrimaryDispatcher.GetType().GetMethod(
            "StopAsync",
            BindingFlags.Instance | BindingFlags.NonPublic) ?? throw new MissingMethodException("Primary dispatcher StopAsync was not found.");
        var stop = Assert.IsAssignableFrom<Task>(stopMethod.Invoke(pair.Equipment.PrimaryDispatcher, new object[] { pair.Token }));
        await stop.WaitAsync(pair.Token);

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await WaitUntilAsync(() => pair.Equipment.PrimaryDispatcher.DroppedPrimaryCount == 1, pair.Token);

        Assert.Equal(0, Volatile.Read(ref legacyCount));
    }

    [Fact]
    public async Task StateChangedReportsActualTransitionsAndAllowsAsyncReentrantOperation()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var transitions = new ConcurrentQueue<SecsSessionStateChangedEventArgs>();
        var equipmentTransitions = new ConcurrentQueue<SecsSessionStateChangedEventArgs>();
        var reentrantLinktest = new TaskCompletionSource<Task>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var pair = await LoopbackPair.CreateAsync(beforeConnect: (equipment, host) =>
        {
            equipment.StateChanged += (_, transition) => equipmentTransitions.Enqueue(transition);
            host.StateChanged += (_, transition) =>
            {
                transitions.Enqueue(transition);
                if (transition.CurrentHsmsState == HsmsConnectionState.Selected)
                    reentrantLinktest.TrySetResult(host.LinktestAsync(timeout.Token));
            };
        });

        await (await reentrantLinktest.Task.WaitAsync(timeout.Token)).WaitAsync(timeout.Token);
        await pair.Host.DisconnectAsync(timeout.Token);
        await WaitUntilAsync(() => transitions.Any(static item =>
            item.CurrentConnectionState == ConnectionState.Disconnected), timeout.Token);

        Assert.All(transitions, static transition => Assert.False(
            transition.PreviousConnectionState == transition.CurrentConnectionState &&
            transition.PreviousHsmsState == transition.CurrentHsmsState));
        Assert.Contains(transitions, static transition =>
            transition.CurrentConnectionState == ConnectionState.Connected &&
            transition.ConnectionIdentity.ConnectionEpoch > 0);
        Assert.Contains(transitions, static transition =>
            transition.CurrentHsmsState == HsmsConnectionState.Selected &&
            transition.ConnectionIdentity.ConnectionEpoch > 0);
        Assert.Contains(equipmentTransitions, static transition =>
            transition.CurrentConnectionState == ConnectionState.Listening);
    }

    [Fact]
    public async Task StateChangedIsolatesThrowingSubscriberAndAllowsSynchronousDisconnectReentrancy()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var observed = new ConcurrentQueue<SecsSessionStateChangedEventArgs>();
        var disconnected = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reentered = 0;
        await using var pair = await LoopbackPair.CreateAsync(beforeConnect: (_, host) =>
        {
            host.StateChanged += static (_, _) => throw new InvalidOperationException("subscriber failure");
            host.StateChanged += (_, transition) =>
            {
                observed.Enqueue(transition);
                if (transition.CurrentHsmsState != HsmsConnectionState.Selected ||
                    Interlocked.Exchange(ref reentered, 1) != 0) return;
                disconnected.TrySetResult(Record.ExceptionAsync(
                    () => host.DisconnectAsync(timeout.Token)).GetAwaiter().GetResult());
            };
        });

        Assert.Null(await disconnected.Task.WaitAsync(timeout.Token));
        await WaitUntilAsync(() => observed.Any(static transition =>
            transition.CurrentConnectionState == ConnectionState.Disconnected), timeout.Token);
        Assert.Contains(observed, static transition => transition.CurrentHsmsState == HsmsConnectionState.Selected);
        Assert.Contains(observed, static transition => transition.CurrentConnectionState == ConnectionState.Disconnected);
    }

    [Fact]
    public async Task StateChangedReportsDeselectReselectAndSeparateTransitions()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var transitions = new ConcurrentQueue<SecsSessionStateChangedEventArgs>();
        pair.Host.StateChanged += (_, transition) => transitions.Enqueue(transition);

        await pair.Host.DeselectAsync(pair.Token);
        await WaitUntilAsync(() => transitions.Any(static transition =>
            transition.PreviousHsmsState == HsmsConnectionState.Selected &&
            transition.CurrentHsmsState == HsmsConnectionState.ConnectedNotSelected), pair.Token);
        await pair.Host.SelectAsync(pair.Token);
        await WaitUntilAsync(() => transitions.Any(static transition =>
            transition.PreviousHsmsState == HsmsConnectionState.ConnectedNotSelected &&
            transition.CurrentHsmsState == HsmsConnectionState.Selected), pair.Token);
        await pair.Host.SeparateAsync(pair.Token);
        await WaitUntilAsync(() => transitions.Any(static transition =>
            transition.CurrentConnectionState == ConnectionState.Disconnected), pair.Token);

        Assert.Contains(transitions, static transition =>
            transition.PreviousHsmsState == HsmsConnectionState.Selected &&
            transition.CurrentHsmsState == HsmsConnectionState.ConnectedNotSelected);
        Assert.Contains(transitions, static transition =>
            transition.PreviousHsmsState == HsmsConnectionState.ConnectedNotSelected &&
            transition.CurrentHsmsState == HsmsConnectionState.Selected);
        Assert.Contains(transitions, static transition =>
            transition.CurrentHsmsState == HsmsConnectionState.NotConnected);
        Assert.All(transitions, static transition => Assert.True(transition.ConnectionIdentity.ConnectionEpoch > 0));
    }

    [Fact]
    public async Task ThrowingHandlerIsIsolatedAndNextPrimaryRuns()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var failedDialogue = new SecsDialogueDefinition(new SecsStream(13), new SecsFunction(1));
        var healthyDialogue = new SecsDialogueDefinition(new SecsStream(13), new SecsFunction(3));
        var diagnostic = new TaskCompletionSource<SecsDiagnosticEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthy = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        pair.Equipment.DiagnosticReceived += (_, item) =>
        {
            if (item.Kind == SecsDiagnosticKind.ApplicationError) diagnostic.TrySetResult(item);
        };
        using var failed = pair.Equipment.PrimaryDispatcher.Register(failedDialogue, (_, _) =>
            throw new InvalidOperationException("handler failed"));
        using var succeeded = pair.Equipment.PrimaryDispatcher.Register(healthyDialogue, (_, _) =>
        {
            healthy.TrySetResult();
            return ValueTask.CompletedTask;
        });

        await pair.Host.SendAsync(failedDialogue.Stream, failedDialogue.PrimaryFunction, cancellationToken: pair.Token);
        await pair.Host.SendAsync(healthyDialogue.Stream, healthyDialogue.PrimaryFunction, cancellationToken: pair.Token);

        await healthy.Task.WaitAsync(pair.Token);
        Assert.Equal(SecsDiagnosticKind.ApplicationError, (await diagnostic.Task.WaitAsync(pair.Token)).Kind);
    }

    [Fact]
    public async Task DisconnectCancelsHandlerAndRegistrationSurvivesReconnect()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(14), new SecsFunction(1));
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var invocation = 0;
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (_, cancellationToken) =>
        {
            if (Interlocked.Increment(ref invocation) == 1)
            {
                firstEntered.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { firstCanceled.TrySetResult(); }
            }
            else secondHandled.TrySetResult();
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await firstEntered.Task.WaitAsync(pair.Token);
        await pair.Equipment.DisconnectAsync(pair.Token);
        await firstCanceled.Task.WaitAsync(pair.Token);

        await pair.ReconnectAsync(equipmentAlreadyDisconnected: true);
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await secondHandled.Task.WaitAsync(pair.Token);
    }

    [Fact]
    public async Task StaleContextCannotReplyOnReconnectedStreamAndFailureConsumesOwnership()
    {
        await using var pair = await LoopbackPair.CreateAsync();
        var dialogue = new SecsDialogueDefinition(new SecsStream(15), new SecsFunction(1), new SecsFunction(2));
        var captured = new TaskCompletionSource<ISecsPrimaryContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, (context, _) =>
        {
            captured.TrySetResult(context);
            return ValueTask.CompletedTask;
        });

        var pending = pair.Host.RequestAsync(dialogue, cancellationToken: pair.Token);
        var stale = await captured.Task.WaitAsync(pair.Token);
        await pair.Equipment.DisconnectAsync(pair.Token);
        await Assert.ThrowsAnyAsync<Exception>(() => pending);
        await pair.ReconnectAsync(equipmentAlreadyDisconnected: true);

        await Assert.ThrowsAnyAsync<Exception>(() => stale.ReplyAsync(cancellationToken: pair.Token).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => stale.ReplyAsync(cancellationToken: pair.Token).AsTask());
    }

    [Fact]
    public async Task DisposeCancelsCooperativeHandlerAndCompletesBoundedly()
    {
        var pair = await LoopbackPair.CreateAsync();
        await using var cleanup = pair;
        var dialogue = new SecsDialogueDefinition(new SecsStream(16), new SecsFunction(1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (_, cancellationToken) =>
        {
            entered.TrySetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { canceled.TrySetResult(); }
        });
        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);
        await entered.Task.WaitAsync(pair.Token);

        await pair.Equipment.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        await canceled.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task HandlerCanDisposeItsOwningSessionWithoutSelfDeadlock()
    {
        var pair = await LoopbackPair.CreateAsync();
        await using var cleanup = pair;
        var dialogue = new SecsDialogueDefinition(new SecsStream(17), new SecsFunction(1));
        var completed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = pair.Equipment.PrimaryDispatcher.Register(dialogue, async (_, _) =>
        {
            completed.TrySetResult(await Record.ExceptionAsync(() => pair.Equipment.DisposeAsync().AsTask()));
        });

        await pair.Host.SendAsync(dialogue.Stream, dialogue.PrimaryFunction, cancellationToken: pair.Token);

        Assert.Null(await completed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task CompletedWorkersLeaveLifetimeOwnedByStopAsync()
    {
        var session = new HsmsSession(new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = ReservePort(),
            Mode = SecsConnectionMode.Passive,
            Role = SecsRole.Equipment,
            SessionId = new SecsSessionId(0)
        });
        var dispatcher = session.PrimaryDispatcher;
        var dispatcherType = dispatcher.GetType();
        var queue = dispatcherType.GetField("_queue", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(dispatcher) ??
            throw new MissingFieldException(dispatcherType.FullName, "_queue");
        var writer = queue.GetType().GetProperty("Writer")?.GetValue(queue) ??
            throw new MissingMemberException(queue.GetType().FullName, "Writer");
        var tryComplete = writer.GetType().GetMethod(
            "TryComplete",
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            types: [typeof(Exception)],
            modifiers: null) ?? throw new MissingMethodException(writer.GetType().FullName, "TryComplete");
        var workers = Assert.IsType<Task[]>(dispatcherType.GetField(
            "_workers",
            BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(dispatcher));
        var stopAsync = dispatcherType.GetMethod(
            "StopAsync",
            BindingFlags.Instance | BindingFlags.NonPublic) ?? throw new MissingMethodException(dispatcherType.FullName, "StopAsync");

        Assert.True(Assert.IsType<bool>(tryComplete.Invoke(writer, [null])));
        await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(5));

        var stop = Assert.IsAssignableFrom<Task>(stopAsync.Invoke(dispatcher, [CancellationToken.None]));
        await stop.WaitAsync(TimeSpan.FromSeconds(5));
        await session.DisposeAsync();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
        }
    }

    private static void UpdateMaximum(ref int maximum, int candidate)
    {
        while (true)
        {
            var current = Volatile.Read(ref maximum);
            if (candidate <= current || Interlocked.CompareExchange(ref maximum, candidate, current) == current) return;
        }
    }

    private static int ReservePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class LoopbackPair : IAsyncDisposable
    {
        private readonly CancellationTokenSource _timeout = new(TimeSpan.FromSeconds(15));

        private LoopbackPair(HsmsSession equipment, HsmsSession host)
        {
            Equipment = equipment;
            Host = host;
        }

        public HsmsSession Equipment { get; }
        public HsmsSession Host { get; }
        public CancellationToken Token => _timeout.Token;

        public static async Task<LoopbackPair> CreateAsync(
            ushort sessionId = 7,
            SecsPrimaryDispatcherOptions? equipmentDispatcher = null,
            Action<HsmsSession, HsmsSession>? beforeConnect = null)
        {
            var port = ReservePort();
            var equipment = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, sessionId, equipmentDispatcher);
            var host = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, sessionId, null);
            var pair = new LoopbackPair(equipment, host);
            beforeConnect?.Invoke(equipment, host);
            try
            {
                await pair.ConnectAsync();
                return pair;
            }
            catch
            {
                await pair.DisposeAsync();
                throw;
            }
        }

        public async Task ReconnectAsync(bool equipmentAlreadyDisconnected = false)
        {
            if (!equipmentAlreadyDisconnected) await Equipment.DisconnectAsync(Token);
            await Host.DisconnectAsync(Token);
            await ConnectAsync();
        }

        private async Task ConnectAsync()
        {
            var passiveConnect = Equipment.ConnectAsync(Token);
            await WaitUntilAsync(() => Equipment.State == ConnectionState.Listening, Token);
            await Host.ConnectAsync(Token);
            await passiveConnect;
            await Host.SelectAsync(Token);
        }

        public async ValueTask DisposeAsync()
        {
            _timeout.Cancel();
            List<Exception>? errors = null;
            try { await Host.DisposeAsync(); }
            catch (Exception exception) { (errors ??= []).Add(exception); }
            try { await Equipment.DisposeAsync(); }
            catch (Exception exception) { (errors ??= []).Add(exception); }
            _timeout.Dispose();
            if (errors is { Count: > 0 }) throw new AggregateException(errors);
        }

        private static HsmsSession CreateSession(
            int port,
            SecsConnectionMode mode,
            SecsRole role,
            ushort sessionId,
            SecsPrimaryDispatcherOptions? dispatcher) => new(new HsmsSessionOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Mode = mode,
            Role = role,
            SessionId = new SecsSessionId(sessionId),
            PrimaryDispatcher = dispatcher ?? new SecsPrimaryDispatcherOptions()
        });
    }
}
