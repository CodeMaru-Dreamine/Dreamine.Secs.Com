using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Interfaces;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.SecsGem.Interop.Runtime.Logging;
using Dreamine.SecsGem.Interop.Runtime.Profiles;
using Dreamine.SecsGem.Interop.Runtime.Scenarios;
using Dreamine.SecsGem.Interop.Runtime.Templates;

internal sealed record RuntimeSampleDocuments(
    SampleOptions Options,
    SingleConnectionProfileV1? Profile,
    MessageTemplateV1? Template,
    MessageTemplateV1? SecondaryTemplate,
    ScenarioDefinitionV1? Scenario,
    InteropWireLogSessionOptions? WireLogOptions);

internal static class RuntimeSampleFeatures
{
    internal static async Task<RuntimeSampleDocuments> LoadAsync(
        SampleOptions options,
        SecsRole role,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        SingleConnectionProfileV1? profile = null;
        if (options.ProfilePath is not null)
        {
            profile = await ConnectionProfileStore.Create()
                .LoadAsync(options.ProfilePath, cancellationToken)
                .ConfigureAwait(false);
            if (profile.Role != role)
                throw new InvalidOperationException(
                    $"Profile role {profile.Role} does not match this {role} sample.");
            if (profile.SessionId == 0)
                throw new InvalidOperationException("The public sample requires a configured non-zero Session ID.");
            options = options.ApplyProfile(profile);
            Console.WriteLine($"Loaded Connection Profile v1 ({Path.GetFileName(options.ProfilePath)}). / Connection Profile v1을 불러왔습니다.");
        }

        MessageTemplateV1? template = null;
        MessageTemplateV1? secondary = null;
        if (options.TemplateCatalogPath is not null)
        {
            var catalog = await MessageTemplateCatalogStore.Create()
                .LoadAsync(options.TemplateCatalogPath, cancellationToken)
                .ConfigureAwait(false);
            template = catalog.Templates.SingleOrDefault(value =>
                StringComparer.OrdinalIgnoreCase.Equals(value.Name, options.TemplateName)) ??
                throw new InvalidOperationException($"Template '{options.TemplateName}' was not found.");
            if (template.Kind != MessageTemplateKind.Primary)
                throw new InvalidOperationException("The selected sample template must be a Primary.");
            if (template.WaitBit)
            {
                var secondaryDirection = template.Direction == MessageTemplateDirection.HostToEquipment
                    ? MessageTemplateDirection.EquipmentToHost
                    : MessageTemplateDirection.HostToEquipment;
                secondary = catalog.Templates.SingleOrDefault(value =>
                    value.Kind == MessageTemplateKind.Secondary &&
                    value.Stream == template.Stream &&
                    value.Function == template.Function + 1 &&
                    value.Direction == secondaryDirection) ??
                    throw new InvalidOperationException(
                        $"Template '{template.Name}' requires a matching S{template.Stream}F{template.Function + 1} Secondary template.");
            }
            Console.WriteLine($"Loaded template '{template.Name}'. / 템플릿 '{template.Name}'을 불러왔습니다.");
        }

        ScenarioDefinitionV1? scenario = null;
        if (options.ScenarioPath is not null)
        {
            scenario = await new ScenarioFileStoreV1()
                .LoadAsync(options.ScenarioPath, cancellationToken)
                .ConfigureAwait(false);
            Console.WriteLine($"Loaded Scenario v1 ({Path.GetFileName(options.ScenarioPath)}). / Scenario v1을 불러왔습니다.");
        }

        InteropWireLogSessionOptions? wire = null;
        if (options.LogDirectory is not null)
        {
            wire = new InteropWireLogSessionOptions(options.LogDirectory)
            {
                LogPolicyId = profile?.LogPolicyId ?? ConnectionLogPolicyIds.HeaderOnlyV1
            };
            _ = wire.CreateObservationOptions();
            Console.WriteLine("Persistent bounded wire logging is enabled. / 제한 용량 영구 wire logging이 활성화되었습니다.");
        }

        return new RuntimeSampleDocuments(options, profile, template, secondary, scenario, wire);
    }

    internal static HsmsSessionOptions CreateSessionOptions(
        RuntimeSampleDocuments documents,
        SecsRole role)
    {
        ArgumentNullException.ThrowIfNull(documents);
        var sample = documents.Options;
        var profileOptions = documents.Profile?.ToHsmsSessionOptions();
        return new HsmsSessionOptions
        {
            Host = sample.Host,
            Port = sample.Port,
            Mode = sample.Mode,
            Role = role,
            SessionId = new SecsSessionId(sample.SessionId),
            AutoReconnect = profileOptions?.AutoReconnect ?? false,
            Timers = profileOptions?.Timers ?? new HsmsTimerOptions(),
            MaximumFrameLength = profileOptions?.MaximumFrameLength ?? 16 * 1024 * 1024,
            MaximumMessageLength = profileOptions?.MaximumMessageLength ?? 16 * 1024 * 1024 - 10,
            MaximumNestingDepth = profileOptions?.MaximumNestingDepth ?? 64,
            MaximumListItemCount = profileOptions?.MaximumListItemCount ?? 65_535,
            WireObservation = documents.WireLogOptions?.CreateObservationOptions(),
            PrimaryDispatcher = new SecsPrimaryDispatcherOptions
            {
                QueueCapacity = 32,
                MaximumConcurrency = 1
            }
        };
    }

    internal static IDisposable? RegisterTemplateHandler(
        ISecsMessageSession session,
        SecsRole localRole,
        RuntimeSampleDocuments documents)
    {
        var template = documents.Template;
        if (template is null || IsLocalSender(localRole, template.Direction)) return null;
        var dialogue = template.WaitBit
            ? new SecsDialogueDefinition(
                new SecsStream(template.Stream),
                new SecsFunction(template.Function),
                new SecsFunction(checked((byte)(template.Function + 1))))
            : new SecsDialogueDefinition(new SecsStream(template.Stream), new SecsFunction(template.Function));
        return session.PrimaryDispatcher.Register(dialogue, async (context, cancellationToken) =>
        {
            Console.WriteLine($"Received template {template.Name} as S{template.Stream}F{template.Function}.");
            if (template.WaitBit)
            {
                await context.ReplyAsync(
                    documents.SecondaryTemplate?.BuildItem(),
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"Replied with template {documents.SecondaryTemplate!.Name}.");
            }
        });
    }

    internal static async Task ExecuteTemplateIfSenderAsync(
        ISecsMessageSession session,
        SecsRole localRole,
        RuntimeSampleDocuments documents,
        CancellationToken cancellationToken)
    {
        var template = documents.Template;
        if (template is null || !IsLocalSender(localRole, template.Direction)) return;
        if (template.WaitBit)
        {
            var reply = await session.RequestAsync(
                new SecsDialogueDefinition(
                    new SecsStream(template.Stream),
                    new SecsFunction(template.Function),
                    new SecsFunction(checked((byte)(template.Function + 1)))),
                template.BuildItem(),
                cancellationToken).ConfigureAwait(false);
            Console.WriteLine($"Template {template.Name} received correlated S{reply.Stream.Value}F{reply.Function.Value}.");
        }
        else
        {
            await session.SendAsync(
                new SecsStream(template.Stream),
                new SecsFunction(template.Function),
                template.BuildItem(),
                cancellationToken).ConfigureAwait(false);
            Console.WriteLine($"Template {template.Name} was sent with W=0.");
        }
    }

    internal static async Task<int?> RunScenarioIfConfiguredAsync(
        ISecsMessageSession session,
        RuntimeSampleDocuments documents,
        CancellationToken cancellationToken)
    {
        if (documents.Scenario is null) return null;
        var result = await new ScenarioRunnerV1()
            .RunAsync(documents.Scenario, session, cancellationToken)
            .ConfigureAwait(false);
        Console.WriteLine(
            $"Scenario result: {result.Status}; steps={result.Steps.Count}; dropped={result.DroppedInboundMessageCount}; exit={result.ExitCode}.");
        return result.ExitCode;
    }

    private static bool IsLocalSender(SecsRole localRole, MessageTemplateDirection direction) =>
        direction switch
        {
            MessageTemplateDirection.HostToEquipment => localRole == SecsRole.Host,
            MessageTemplateDirection.EquipmentToHost => localRole == SecsRole.Equipment,
            _ => throw new InvalidOperationException($"Unsupported template direction {direction}.")
        };
}
