using AwesomeAssertions;
using Soenneker.HubSpot.Runners.OpenApiClient.Utils;
using Soenneker.Tests.HostedUnit;
using System;
using System.IO;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Soenneker.HubSpot.Runners.OpenApiClient.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class HubSpotOpenApiClientRunnerTests : HostedUnitTest
{
    public HubSpotOpenApiClientRunnerTests(Host host) : base(host)
    {

    }

    [Test]
    public void SharedOperations_UseAssociationsOwnerForObjectAssociationRoutes()
    {
        const string route = "/crm/objects/2026-09/{fromObjectType}/{fromObjectId}/associations/default/{toObjectType}/{toObjectId}";
        var repeated = new JsonObject { ["paths"] = new JsonObject { [route] = new JsonObject
        {
            ["put"] = new JsonObject { ["operationId"] = "duplicate" },
            ["get"] = new JsonObject { ["operationId"] = "unique" }
        } } };
        var owner = new JsonObject { ["paths"] = new JsonObject { [route] = new JsonObject
        {
            ["put"] = new JsonObject { ["operationId"] = "owner" }
        } } };
        string originalOwner = owner.ToJsonString();
        var documents = new Dictionary<string, JsonObject>
        {
            ["CRM/Objects"] = JsonNode.Parse("""{"paths":{}}""")!.AsObject(),
            ["CRM/Custom Objects"] = JsonNode.Parse("""{"paths":{}}""")!.AsObject(),
            ["CRM/Associations"] = owner
        };

        FileOperationsUtil.RemoveSharedOperations("CRM/Appointments", repeated, documents).Should().Be(1);
        repeated["paths"]![route]!["put"].Should().BeNull();
        repeated["paths"]![route]!["get"]!["operationId"]!.GetValue<string>().Should().Be("unique");
        FileOperationsUtil.RemoveSharedOperations("CRM/Associations", owner, documents).Should().Be(0);
        owner.ToJsonString().Should().Be(originalOwner);
    }

    [Test]
    public void SharedOperations_UseCustomObjectsWhenGeneralObjectsLacksTheRoute()
    {
        const string route = "/crm/objects/2026-09/{fromObjectType}/gdpr-delete";
        var duplicate = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{fromObjectType}/gdpr-delete":{"post":{},"get":{}}}}""")!.AsObject();
        var custom = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{objectType}/gdpr-delete":{"post":{}}}}""")!.AsObject();
        var documents = new Dictionary<string, JsonObject>
        {
            ["CRM/Objects"] = JsonNode.Parse("""{"paths":{}}""")!.AsObject(),
            ["CRM/Custom Objects"] = custom
        };
        FileOperationsUtil.RemoveSharedOperations("CRM/Appointments", duplicate, documents).Should().Be(1);
        duplicate["paths"]![route]!["post"].Should().BeNull();
        duplicate["paths"]![route]!["get"].Should().NotBeNull();
        FileOperationsUtil.RemoveSharedOperations("CRM/Custom Objects", custom, documents).Should().Be(0);
    }

    [Test]
    public void SharedOperations_PreferOwnerAndPreserveUniqueOperations()
    {
        const string path = "/crm/objects/2026-09/{objectType}";
        var repeated = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{objectType}":{"get":{"operationId":"duplicate"},"post":{"operationId":"unique"}}}}""")!.AsObject();
        var owner = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{objectType}":{"get":{"operationId":"owner"}}}}""")!.AsObject();
        var documents = new Dictionary<string, JsonObject> { ["CRM/Objects"] = owner };

        FileOperationsUtil.RemoveSharedOperations("CRM/Appointments", repeated, documents).Should().Be(1);
        repeated["paths"]![path]!["get"].Should().BeNull();
        repeated["paths"]![path]!["post"]!["operationId"]!.GetValue<string>().Should().Be("unique");
        FileOperationsUtil.RemoveSharedOperations("CRM/Objects", owner, documents).Should().Be(0);
        owner["paths"]![path]!["get"]!["operationId"]!.GetValue<string>().Should().Be("owner");
    }

    [Test]
    public void SharedOperations_MatchTemplatesWithDifferentParameterNames()
    {
        const string path = "/crm/objects/2026-09/{fromObjectType}/gdpr-delete";
        var repeated = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{fromObjectType}/gdpr-delete":{"post":{"operationId":"duplicate"}}}}""")!.AsObject();
        var owner = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{objectType}/gdpr-delete":{"post":{"operationId":"owner"}}}}""")!.AsObject();
        string originalOwner = owner.ToJsonString();
        var documents = new Dictionary<string, JsonObject> { ["CRM/Objects"] = owner };

        FileOperationsUtil.RemoveSharedOperations("CRM/Appointments", repeated, documents).Should().Be(1);
        repeated["paths"]![path].Should().BeNull();
        owner.ToJsonString().Should().Be(originalOwner);
    }

    [Test]
    public void SharedOperations_TemplateMatchingPreservesUniqueMethodsAndLiteralPaths()
    {
        const string path = "/crm/objects/2026-09/{fromObjectType}/gdpr-delete";
        var repeated = JsonNode.Parse("""
            {"paths":{
              "/crm/objects/2026-09/{fromObjectType}/gdpr-delete":{"post":{},"get":{}},
              "/crm/objects/2026-03/{fromObjectType}/gdpr-delete":{"post":{}},
              "/crm/objects/2026-09/contacts/gdpr-delete":{"post":{}},
              "/crm/objects/2026-09/{fromObjectType}/other":{"post":{}}
            }}
            """)!.AsObject();
        var owner = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/{objectType}/gdpr-delete":{"post":{}}}}""")!.AsObject();
        var documents = new Dictionary<string, JsonObject> { ["CRM/Objects"] = owner };

        FileOperationsUtil.RemoveSharedOperations("CRM/Appointments", repeated, documents).Should().Be(1);
        repeated["paths"]![path]!["post"].Should().BeNull();
        repeated["paths"]![path]!["get"].Should().NotBeNull();
        repeated["paths"]!.AsObject().Count.Should().Be(4);
        repeated["paths"]!["/crm/objects/2026-03/{fromObjectType}/gdpr-delete"]!["post"].Should().NotBeNull();
        repeated["paths"]!["/crm/objects/2026-09/contacts/gdpr-delete"]!["post"].Should().NotBeNull();
        repeated["paths"]!["/crm/objects/2026-09/{fromObjectType}/other"]!["post"].Should().NotBeNull();
    }

    [Test]
    public void SharedOperations_KeepEndpointWhenOwnerDoesNotSupplyIt()
    {
        var document = JsonNode.Parse("""{"paths":{"/crm/objects/2026-09/unique":{"get":{}}}}""")!.AsObject();
        var documents = new Dictionary<string, JsonObject> { ["CRM/Objects"] = JsonNode.Parse("""{"paths":{}}""")!.AsObject() };

        FileOperationsUtil.RemoveSharedOperations("CRM/Appointments", document, documents).Should().Be(0);
        document["paths"]!.AsObject().Count.Should().Be(1);
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
