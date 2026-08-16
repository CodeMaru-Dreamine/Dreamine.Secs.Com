using Xunit;

namespace Dreamine.Secs.Com.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class NetworkIsolationCollection
{
    public const string Name = "Process-wide TCP loopback isolation";
}
