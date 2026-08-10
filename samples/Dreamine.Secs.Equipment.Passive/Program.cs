using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Com.Hsms;

var options = new HsmsSessionOptions
{
    Host = args.ElementAtOrDefault(0) ?? "127.0.0.1",
    Port = int.TryParse(args.ElementAtOrDefault(1), out var port) ? port : 7000,
    Mode = SecsConnectionMode.Passive,
    Role = SecsRole.Equipment,
    SessionId = new SecsSessionId(0)
};

await using var session = new HsmsSession(options);
session.MessageReceived += (_, primary) => _ = RespondAsync(session, primary);

for (var connection = 0; connection < 2; connection++)
{
    Console.WriteLine($"Listening for connection {connection + 1} on {options.Host}:{options.Port}...");
    await session.ConnectAsync();
    while (session.State is not Dreamine.Communication.Abstractions.Enums.ConnectionState.Disconnected)
        await Task.Delay(100);
}

static async Task RespondAsync(HsmsSession session, SecsMessage primary)
{
    SecsItem? body = (primary.Stream.Value, primary.Function.Value) switch
    {
        (1, 1) => new SecsListItem([new SecsAsciiItem("DREAMINE-SAMPLE"), new SecsAsciiItem("1.0")]),
        (1, 13) => new SecsListItem([
            new SecsBinaryItem([0]),
            new SecsListItem([new SecsAsciiItem("DREAMINE-SAMPLE"), new SecsAsciiItem("1.0")])]),
        _ => null
    };
    if (primary.Stream.Value != 1 || primary.Function.Value is not (1 or 13)) return;
    var reply = new SecsMessage(primary.SessionId, primary.Stream,
        new SecsFunction((byte)(primary.Function.Value + 1)), false, primary.SystemBytes, body);
    await session.SendAsync(reply);
}
