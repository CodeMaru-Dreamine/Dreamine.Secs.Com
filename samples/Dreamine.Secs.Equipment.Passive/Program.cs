using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.Secs.Com;
using Dreamine.SecsGem.Interop.Runtime.Logging;

try
{
    var requested = SampleOptions.Parse(args, SecsConnectionMode.Passive);
    if (requested.ShowHelp)
    {
        SampleOptions.PrintHelp("Dreamine.Secs.Equipment.Passive", "Equipment", "passive");
        return 0;
    }

    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(requested.TimeoutSeconds));
    var documents = await RuntimeSampleFeatures.LoadAsync(requested, SecsRole.Equipment, timeout.Token);
    var sample = documents.Options;
    sample.PrintConfiguration(SecsRole.Equipment);
    if (sample.ValidateOnly) return 0;
    ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        timeout.Cancel();
    };
    Console.CancelKeyPress += cancelHandler;
    try
    {
        ISecsMessageSessionProvider provider = new DreamineSecsCommunicationProvider(
            _ => RuntimeSampleFeatures.CreateSessionOptions(documents, SecsRole.Equipment));
        await using var session = provider.CreateSession(new SecsConnectionOptions
        {
            ProviderKey = provider.Key,
            Mode = sample.Mode,
            Role = SecsRole.Equipment
        });
        session.DiagnosticReceived += static (_, diagnostic) =>
            Console.Error.WriteLine($"Diagnostic: {diagnostic.Kind}: {diagnostic.Message}");
        var wireLog = documents.WireLogOptions is null
            ? null
            : InteropWireLogSession.Start(session, documents.WireLogOptions);

        var communication = new SecsDialogueDefinition(
            new SecsStream(1), new SecsFunction(13), new SecsFunction(14));
        var identity = new SecsDialogueDefinition(
            new SecsStream(1), new SecsFunction(1), new SecsFunction(2));
        var sampleW0 = new SecsDialogueDefinition(new SecsStream(127), new SecsFunction(1));

        using var communicationRegistration = session.PrimaryDispatcher.Register(
            communication,
            static async (context, cancellationToken) =>
            {
                Console.WriteLine(
                    $"Received S1F13 (SessionId={context.Primary.SessionId.Value}, epoch={context.ConnectionIdentity.ConnectionEpoch}).");
                await context.ReplyAsync(new SecsListItem([
                    new SecsBinaryItem([0]),
                    new SecsListItem([
                        new SecsAsciiItem("DREAMINE-SAMPLE"),
                        new SecsAsciiItem("1.0")
                    ])
                ]), cancellationToken);
                Console.WriteLine("Sent S1F14.");
            });
        using var identityRegistration = session.PrimaryDispatcher.Register(
            identity,
            static async (context, cancellationToken) =>
            {
                Console.WriteLine($"Received S1F1 (SessionId={context.Primary.SessionId.Value}).");
                await context.ReplyAsync(new SecsListItem([
                    new SecsAsciiItem("DREAMINE-SAMPLE"),
                    new SecsAsciiItem("1.0")
                ]), cancellationToken);
                Console.WriteLine("Sent S1F2.");
            });
        using var w0Registration = session.PrimaryDispatcher.Register(
            sampleW0,
            static (context, _) =>
            {
                Console.WriteLine(
                    $"Received sample-only W0 S127F1 (SystemBytes={context.Primary.SystemBytes.Value}).");
                return ValueTask.CompletedTask;
            });
        using var templateRegistration = RuntimeSampleFeatures.RegisterTemplateHandler(
            session, SecsRole.Equipment, documents);

        try
        {
            var scenarioExitCode = await RuntimeSampleFeatures.RunScenarioIfConfiguredAsync(
                session, documents, timeout.Token);
            if (scenarioExitCode is { } configuredExitCode) return configuredExitCode;

            for (var connection = 1; connection <= sample.ConnectionCount; connection++)
            {
                timeout.Token.ThrowIfCancellationRequested();
                Console.WriteLine(sample.Mode == SecsConnectionMode.Passive
                    ? $"Listening for connection {connection}/{sample.ConnectionCount} on {sample.Host}:{sample.Port}..."
                    : $"Connecting {connection}/{sample.ConnectionCount} to {sample.Host}:{sample.Port}...");

                await ConnectAndSelectAsync(session, sample.Mode, timeout.Token);

                Console.WriteLine(
                    $"Selected (SessionId={session.ConnectionIdentity.SessionId.Value}, epoch={session.ConnectionIdentity.ConnectionEpoch}).");
                await RuntimeSampleFeatures.ExecuteTemplateIfSenderAsync(
                    session, SecsRole.Equipment, documents, timeout.Token);
                await WaitForDisconnectAsync(session, timeout.Token);

                if (connection < sample.ConnectionCount)
                    await Task.Delay(sample.ReconnectDelayMilliseconds, timeout.Token);
            }
        }
        finally
        {
            await session.DisposeAsync();
            if (wireLog is not null)
            {
                await wireLog.StopAsync();
                Console.WriteLine(
                    $"Wire log: written={wireLog.Health.Written}, sourceDropped={wireLog.Health.SourceDropped}, " +
                    $"recorderDropped={wireLog.Health.RecorderDropped}, flush={wireLog.Health.FlushCompleted}.");
                if (!wireLog.Health.IsEvidenceEligible)
                    throw new InvalidOperationException(
                        $"Wire logging was unhealthy: {wireLog.Health.Failure ?? "a bounded queue dropped records"}.");
            }
        }
    }
    finally
    {
        Console.CancelKeyPress -= cancelHandler;
    }

    return 0;
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Sample canceled or its bounded run timeout elapsed. / 샘플이 취소되었거나 실행 제한 시간이 지났습니다.");
    return 2;
}
catch (Exception exception)
{
    Console.Error.WriteLine($"Sample failed: {exception.GetType().Name}: {exception.Message}");
    return 1;
}

static async Task ConnectAndSelectAsync(
    ISecsMessageSession session,
    SecsConnectionMode mode,
    CancellationToken cancellationToken)
{
    if (mode == SecsConnectionMode.Active)
    {
        await session.ConnectAsync(cancellationToken);
        await session.SelectAsync(cancellationToken);
        return;
    }

    var selected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    EventHandler<SecsSessionStateChangedEventArgs> stateHandler = (_, transition) =>
    {
        if (transition.CurrentHsmsState == HsmsConnectionState.Selected)
            selected.TrySetResult();
        else if (transition.CurrentConnectionState == ConnectionState.Disconnected &&
                 transition.PreviousConnectionState == ConnectionState.Connected)
            selected.TrySetException(new InvalidOperationException(
                "The peer disconnected before HSMS reached Selected."));
    };
    session.StateChanged += stateHandler;
    try
    {
        await session.ConnectAsync(cancellationToken);
        if (session.HsmsState == HsmsConnectionState.Selected) selected.TrySetResult();
        await selected.Task.WaitAsync(cancellationToken);
    }
    finally
    {
        session.StateChanged -= stateHandler;
    }
}

static async Task WaitForDisconnectAsync(
    ISecsMessageSession session,
    CancellationToken cancellationToken)
{
    while (session.State != ConnectionState.Disconnected)
        await Task.Delay(20, cancellationToken);
}
