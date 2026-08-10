using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Com.Hsms;

var options = new HsmsSessionOptions
{
    Host = args.ElementAtOrDefault(0) ?? "127.0.0.1",
    Port = int.TryParse(args.ElementAtOrDefault(1), out var port) ? port : 7000,
    Mode = SecsConnectionMode.Active,
    Role = SecsRole.Host,
    SessionId = new SecsSessionId(0)
};

await using var session = new HsmsSession(options);
for (var connection = 0; connection < 2; connection++)
{
    await session.ConnectAsync();
    await session.SelectAsync();

    var communicationReply = await session.SendPrimaryAsync(Primary(session, 1, 13, new SecsListItem([])));
    Console.WriteLine($"Received S{communicationReply.Stream.Value}F{communicationReply.Function.Value}.");

    var identityReply = await session.SendPrimaryAsync(Primary(session, 1, 1, null));
    Console.WriteLine($"Received S{identityReply.Stream.Value}F{identityReply.Function.Value}.");
    await session.LinktestAsync();

    if (connection == 0) await session.SeparateAsync();
    else await session.DisconnectAsync();
}

static SecsMessage Primary(HsmsSession session, byte stream, byte function, SecsItem? item) =>
    new(new SecsSessionId(0), new SecsStream(stream), new SecsFunction(function), true,
        session.AllocateSystemBytes(), item);
