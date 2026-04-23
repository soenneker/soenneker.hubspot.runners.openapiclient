using Soenneker.Tests.HostedUnit;

namespace Soenneker.HubSpot.Runners.OpenApiClient.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class HubSpotOpenApiClientRunnerTests : HostedUnitTest
{
    public HubSpotOpenApiClientRunnerTests(Host host) : base(host)
    {

    }

    [Test]
    public void Default()
    {

    }
}
