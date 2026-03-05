using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.HubSpot.Runners.OpenApiClient.Tests;

[Collection("Collection")]
public sealed class HubSpotOpenApiClientRunnerTests : FixturedUnitTest
{
    public HubSpotOpenApiClientRunnerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {

    }

    [Fact]
    public void Default()
    {

    }
}
