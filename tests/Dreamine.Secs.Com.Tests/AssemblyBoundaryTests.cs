using Xunit;
using Dreamine.Secs.Abstractions.Enums;
using Dreamine.Secs.Abstractions.Hsms;
using Dreamine.Secs.Abstractions.Options;
using Dreamine.Secs.Com.Hsms;

namespace Dreamine.Secs.Com.Tests;

public sealed class AssemblyBoundaryTests
{
    [Fact]
    public void MarkerBelongsToExpectedAssembly()
    {
        Assert.Equal("Dreamine.Secs.Com", typeof(SecsComAssemblyMarker).Assembly.GetName().Name);
    }

    [Fact]
    public async Task NativeProviderComposesHsmsSessionWithoutChangingCommunicationApi()
    {
        var provider = new DreamineSecsCommunicationProvider(
            options => new HsmsSessionOptions { Host = "127.0.0.1", Port = 5001, Mode = options.Mode, Role = options.Role });
        var connection = provider.CreateConnection(new SecsConnectionOptions
        {
            ProviderKey = provider.Key,
            Mode = SecsConnectionMode.Active,
            Role = SecsRole.Host
        });
        var session = Assert.IsType<HsmsSession>(connection);
        Assert.Equal(provider.Key, session.ProviderKey);
        await connection.DisposeAsync();
    }
}
