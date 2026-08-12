using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.SecsGem.Interop.Runtime.Profiles;

internal sealed record SampleOptions(
    string Host,
    int Port,
    SecsConnectionMode Mode,
    ushort SessionId,
    int ConnectionCount,
    int TimeoutSeconds,
    int ReconnectDelayMilliseconds,
    string? ProfilePath,
    string? TemplateCatalogPath,
    string? TemplateName,
    string? ScenarioPath,
    string? LogDirectory,
    bool ValidateOnly,
    bool ShowHelp)
{
    public static SampleOptions Parse(string[] arguments, SecsConnectionMode defaultMode)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        var host = "127.0.0.1";
        var port = 7000;
        var mode = defaultMode;
        ushort sessionId = 7;
        var connectionCount = 2;
        var timeoutSeconds = 120;
        var reconnectDelayMilliseconds = 250;
        string? profilePath = null;
        string? templateCatalogPath = null;
        string? templateName = null;
        string? scenarioPath = null;
        string? logDirectory = null;
        var validateOnly = false;
        var showHelp = false;
        var positional = 0;

        for (var index = 0; index < arguments.Length; index++)
        {
            var argument = arguments[index];
            switch (argument.ToLowerInvariant())
            {
                case "-h":
                case "--help": showHelp = true; break;
                case "--active": mode = SecsConnectionMode.Active; break;
                case "--passive": mode = SecsConnectionMode.Passive; break;
                case "--once": connectionCount = 1; break;
                case "--validate-only": validateOnly = true; break;
                case "--host": host = ReadValue(arguments, ref index, argument); break;
                case "--port":
                    port = ParseInt(ReadValue(arguments, ref index, argument), argument, 1, 65_535);
                    break;
                case "--mode": mode = ParseMode(ReadValue(arguments, ref index, argument)); break;
                case "--session-id":
                    sessionId = checked((ushort)ParseInt(
                        ReadValue(arguments, ref index, argument), argument, 1, SecsSessionId.MaximumValue));
                    break;
                case "--connections":
                    connectionCount = ParseInt(ReadValue(arguments, ref index, argument), argument, 1, 100);
                    break;
                case "--timeout-seconds":
                    timeoutSeconds = ParseInt(ReadValue(arguments, ref index, argument), argument, 1, 3_600);
                    break;
                case "--reconnect-delay-ms":
                    reconnectDelayMilliseconds = ParseInt(
                        ReadValue(arguments, ref index, argument), argument, 0, 60_000);
                    break;
                case "--profile": profilePath = ReadValue(arguments, ref index, argument); break;
                case "--template": templateCatalogPath = ReadValue(arguments, ref index, argument); break;
                case "--template-name": templateName = ReadValue(arguments, ref index, argument); break;
                case "--scenario": scenarioPath = ReadValue(arguments, ref index, argument); break;
                case "--log-directory": logDirectory = ReadValue(arguments, ref index, argument); break;
                default:
                    if (argument.StartsWith("-", StringComparison.Ordinal))
                        throw new ArgumentException($"Unknown option '{argument}'. Use --help.");
                    if (positional == 0) host = argument;
                    else if (positional == 1) port = ParseInt(argument, "port", 1, 65_535);
                    else throw new ArgumentException($"Unexpected positional argument '{argument}'. Use --help.");
                    positional++;
                    break;
            }
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(host);
        if ((templateCatalogPath is null) != (templateName is null))
            throw new ArgumentException("--template and --template-name must be supplied together.");
        return new SampleOptions(host, port, mode, sessionId, connectionCount, timeoutSeconds,
            reconnectDelayMilliseconds, profilePath, templateCatalogPath, templateName, scenarioPath,
            logDirectory, validateOnly, showHelp);
    }

    public void PrintConfiguration(SecsRole role) => Console.WriteLine(
        $"Validated sample configuration: Role={role}, Mode={Mode}, Endpoint={Host}:{Port}, " +
        $"SessionId={SessionId}, Connections={ConnectionCount}, Timeout={TimeoutSeconds}s, " +
        $"Profile={(ProfilePath is null ? "none" : Path.GetFileName(ProfilePath))}, " +
        $"Template={(TemplateName ?? "none")}, Scenario={(ScenarioPath is null ? "none" : Path.GetFileName(ScenarioPath))}, " +
        $"Logging={(LogDirectory is null ? "off" : "bounded-jsonl")}.");

    public static void PrintHelp(string executable, string role, string defaultMode) => Console.WriteLine($$"""
        Dreamine SECS {{role}} core sample / Dreamine SECS {{role}} 코어 샘플

        Usage / 사용법:
          {{executable}} [host] [port] [options]
          {{executable}} --host <address> --port <port> --mode active|passive [options]

        Options / 옵션:
          --mode active|passive        TCP direction / TCP 연결 방향 (default: {{defaultMode}})
          --active | --passive         Short mode switches / 모드 단축 스위치
          --session-id <1..32767>      Data SessionId (default: 7)
          --connections <1..100>       Bounded manual connection cycles (default: 2)
          --once                       Alias for --connections 1
          --timeout-seconds <1..3600>  Whole-run timeout (default: 120)
          --reconnect-delay-ms <0..60000>  Delay between manual cycles (default: 250)
          --profile <profile.json>     Load Connection Profile v1 / 연결 프로필 로드
          --template <catalog.json> --template-name <name>
                                      Send or handle a validated catalog template
          --scenario <scenario.json>  Run the shared bounded Scenario v1 runner
          --log-directory <directory> Persist bounded Header-Only JSONL wire logs
          --validate-only              Parse configuration without networking / 네트워크 없이 설정 검증
          -h | --help                  Show this help

        Examples / 예시:
          {{executable}} 127.0.0.1 7000 --active --session-id 7 --once
          {{executable}} --host 127.0.0.1 --port 7000 --passive --session-id 7 --once
          {{executable}} --profile host-profile.json --template messages.json --template-name SampleRequest --log-directory logs

        Locally verified core sample only: provider-neutral messaging, bounded cancellation,
        manual reconnect, and async disposal. This is not a standards-conformance certificate
        or a customer scenario.
        로컬 검증 코어 샘플입니다. 표준 적합성 인증서나 고객 시나리오가 아닙니다.
        """);

    public SampleOptions ApplyProfile(SingleConnectionProfileV1 profile)
    {
        ArgumentNullException.ThrowIfNull(profile);
        profile.Validate();
        return this with
        {
            Host = profile.Host,
            Port = profile.Port,
            Mode = profile.Mode,
            SessionId = profile.SessionId
        };
    }

    private static string ReadValue(string[] arguments, ref int index, string option)
    {
        if (++index >= arguments.Length)
            throw new ArgumentException($"Option '{option}' requires a value.");
        return arguments[index];
    }

    private static int ParseInt(string value, string name, int minimum, int maximum)
    {
        if (!int.TryParse(value, out var parsed) || parsed < minimum || parsed > maximum)
            throw new ArgumentOutOfRangeException(name, value,
                $"Expected an integer from {minimum} through {maximum}.");
        return parsed;
    }

    private static SecsConnectionMode ParseMode(string value) => value.ToLowerInvariant() switch
    {
        "active" => SecsConnectionMode.Active,
        "passive" => SecsConnectionMode.Passive,
        _ => throw new ArgumentException(
            $"Unknown mode '{value}'. Expected active or passive.", nameof(value))
    };
}
