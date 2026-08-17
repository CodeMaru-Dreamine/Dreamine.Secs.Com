using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Com.Hsms;

var port = ReservePort();
var sessionId = new SecsSessionId(7);
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

await using var equipment = CreateSession(port, SecsConnectionMode.Passive, SecsRole.Equipment, sessionId);
await using var host = CreateSession(port, SecsConnectionMode.Active, SecsRole.Host, sessionId);

var areYouThere = new SecsDialogueDefinition(
    new SecsStream(1),
    new SecsFunction(1),
    new SecsFunction(2));

using var registration = equipment.PrimaryDispatcher.Register(
    areYouThere,
    (context, cancellationToken) =>
        context.ReplyAsync(new SecsAsciiItem("Dreamine equipment online"), cancellationToken));

var equipmentConnect = equipment.ConnectAsync(timeout.Token);
await WaitUntilAsync(() => equipment.State == ConnectionState.Listening, timeout.Token);
await host.ConnectAsync(timeout.Token);
await equipmentConnect;
await host.SelectAsync(timeout.Token);

var response = await host.RequestAsync(
    areYouThere,
    new SecsAsciiItem("Are you there?"),
    timeout.Token);

Console.WriteLine($"S1F2: {((SecsAsciiItem)response.Item!).Value}");
await host.LinktestAsync(timeout.Token);
Console.WriteLine("Linktest: PASS");

await host.DisconnectAsync(timeout.Token);
await WaitUntilAsync(() => equipment.State == ConnectionState.Disconnected, timeout.Token);
Console.WriteLine("Package QuickStart: PASS");

static HsmsSession CreateSession(
    int port,
    SecsConnectionMode mode,
    SecsRole role,
    SecsSessionId sessionId) =>
    new(new HsmsSessionOptions
    {
        Host = "127.0.0.1",
        Port = port,
        Mode = mode,
        Role = role,
        SessionId = sessionId
    });

static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
{
    while (!condition())
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Delay(10, cancellationToken);
    }
}

static int ReservePort()
{
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
