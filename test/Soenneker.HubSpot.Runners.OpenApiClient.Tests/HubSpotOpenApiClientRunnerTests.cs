using AwesomeAssertions;
using Soenneker.HubSpot.Runners.OpenApiClient.Utils;
using Soenneker.Tests.HostedUnit;
using System;
using System.IO;

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

    [Test]
    public void SpecMetadata_AcceptsReleasedDateBasedVersions()
    {
        string root = Path.Combine("specs", "PublicApiSpecs");
        string path = Path.Combine(root, "Auth", "Oauth", "Rollouts", "279897", "2026-03", "oauth.json");

        bool parsed = FileOperationsUtil.TryParseSpecMetadata(path, root, out string category, out FileOperationsUtil.SpecVersion version,
            out int rollout, new DateOnly(2026, 7, 1));

        parsed.Should().BeTrue();
        category.Should().Be("Auth/Oauth");
        rollout.Should().Be(279897);
        version.Segment.Should().Be("2026-03");
    }

    [Test]
    public void SpecMetadata_RejectsFutureDateBasedVersions()
    {
        string root = Path.Combine("specs", "PublicApiSpecs");
        string path = Path.Combine(root, "CRM", "Contacts", "Rollouts", "424", "2026-09", "contacts.json");

        bool parsed = FileOperationsUtil.TryParseSpecMetadata(path, root, out _, out _, out _, new DateOnly(2026, 7, 1));

        parsed.Should().BeFalse();
    }

    [Test]
    public void SpecMetadata_RejectsLegacyVersions()
    {
        string root = Path.Combine("specs", "PublicApiSpecs");
        string path = Path.Combine(root, "Auth", "Oauth", "Rollouts", "279897", "v3", "oauth.json");

        bool parsed = FileOperationsUtil.TryParseSpecMetadata(path, root, out _, out _, out _, new DateOnly(2026, 7, 1));

        parsed.Should().BeFalse();
    }

    [Test]
    public void NewerReleasedDateBasedVersion_Wins()
    {
        FileOperationsUtil.SpecVersion.TryParse("2025-09", new DateOnly(2026, 7, 1), out FileOperationsUtil.SpecVersion older).Should().BeTrue();
        FileOperationsUtil.SpecVersion.TryParse("2026-03", new DateOnly(2026, 7, 1), out FileOperationsUtil.SpecVersion newer).Should().BeTrue();

        newer.CompareTo(older).Should().BePositive();
    }
}
