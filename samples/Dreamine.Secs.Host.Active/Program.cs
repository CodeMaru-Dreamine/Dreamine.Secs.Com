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
    var requested = SampleOptions.Parse(args, SecsConnectionMode.Active);
    if (requested.ShowHelp)
    {
        SampleOptions.PrintHelp("Dreamine.Secs.Host.Active", "Host", "active");
        return 0;
    }

    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(requested.TimeoutSeconds));
    var documents = await RuntimeSampleFeatures.LoadAsync(requested, SecsRole.Host, timeout.Token);
    var sample = documents.Options;
    sample.PrintConfiguration(SecsRole.Host);
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
            _ => RuntimeSampleFeatures.CreateSessionOptions(documents, SecsRole.Host));
        await using var session = provider.CreateSession(new SecsConnectionOptions
        {
            ProviderKey = provider.Key,
            Mode = sample.Mode,
            Role = SecsRole.Host
        });
        session.DiagnosticReceived += static (_, diagnostic) =>
            Console.Error.WriteLine($"Diagnostic: {diagnostic.Kind}: {diagnostic.Message}");
        var wireLog = documents.WireLogOptions is null
            ? null
            : InteropWireLogSession.Start(session, documents.WireLogOptions);
        using var templateRegistration = RuntimeSampleFeatures.RegisterTemplateHandler(
            session, SecsRole.Host, documents);

        try
        {
            var scenarioExitCode = await RuntimeSampleFeatures.RunScenarioIfConfiguredAsync(
                session, documents, timeout.Token);
            if (scenarioExitCode is { } configuredExitCode)
            {
                if (configuredExitCode != 0) return configuredExitCode;

                await RuntimeSampleFeatures.ExecuteTemplateIfSenderAsync(
                    session, SecsRole.Host, documents, timeout.Token);
                if (session.State != ConnectionState.Disconnected)
                    await session.DisconnectAsync(timeout.Token);
                return 0;
            }

            var communication = new SecsDialogueDefinition(
                new SecsStream(1), new SecsFunction(13), new SecsFunction(14));
            var identity = new SecsDialogueDefinition(
                new SecsStream(1), new SecsFunction(1), new SecsFunction(2));

            for (var connection = 1; connection <= sample.ConnectionCount; connection++)
            {
            timeout.Token.ThrowIfCancellationRequested();
            Console.WriteLine(sample.Mode == SecsConnectionMode.Passive
                ? $"Listening for connection {connection}/{sample.ConnectionCount} on {sample.Host}:{sample.Port}..."
                : $"Connecting {connection}/{sample.ConnectionCount} to {sample.Host}:{sample.Port}...");

            await session.ConnectAsync(timeout.Token);
            if (sample.Mode == SecsConnectionMode.Active)
                await session.SelectAsync(timeout.Token);
            else
                await WaitForStateAsync(session, HsmsConnectionState.Selected, timeout.Token);

            var communicationReply = await session.RequestAsync(
                communication, new SecsListItem([]), timeout.Token);
            RequireReply(communicationReply, 1, 14);
            Console.WriteLine($"Received S1F14 (SessionId={communicationReply.SessionId.Value}, epoch={session.ConnectionIdentity.ConnectionEpoch}).");

            var identityReply = await session.RequestAsync(identity, cancellationToken: timeout.Token);
            RequireReply(identityReply, 1, 2);
            Console.WriteLine($"Received S1F2 (SessionId={identityReply.SessionId.Value}).");

            // S127F1 is local to these two samples and demonstrates the safe W0 API only.
            await session.SendAsync(new SecsStream(127), new SecsFunction(1),
                new SecsAsciiItem("DREAMINE-SAMPLE-W0"), timeout.Token);
            Console.WriteLine("Sent sample-only W0 S127F1.");

            await RuntimeSampleFeatures.ExecuteTemplateIfSenderAsync(
                session, SecsRole.Host, documents, timeout.Token);

            await session.LinktestAsync(timeout.Token);
            Console.WriteLine("Linktest completed.");

            if (connection < sample.ConnectionCount)
            {
                await session.SeparateAsync(timeout.Token);
                await Task.Delay(sample.ReconnectDelayMilliseconds, timeout.Token);
            }
            else
            {
                await session.DisconnectAsync(timeout.Token);
            }
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

static void RequireReply(SecsMessage reply, byte expectedStream, byte expectedFunction)
{
    if (reply.Stream.Value != expectedStream || reply.Function.Value != expectedFunction)
        throw new InvalidOperationException(
            $"Expected S{expectedStream}F{expectedFunction}, received S{reply.Stream.Value}F{reply.Function.Value}.");
}

static async Task WaitForStateAsync(
    ISecsMessageSession session,
    HsmsConnectionState expected,
    CancellationToken cancellationToken)
{
    while (session.HsmsState != expected)
    {
        if (session.State == ConnectionState.Disconnected)
            throw new InvalidOperationException($"The peer disconnected before HSMS reached {expected}.");
        await Task.Delay(20, cancellationToken);
    }
}
