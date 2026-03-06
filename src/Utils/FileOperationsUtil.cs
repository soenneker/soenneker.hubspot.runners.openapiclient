using Microsoft.Extensions.Logging;
using Soenneker.Extensions.String;
using Soenneker.Git.Util.Abstract;
using Soenneker.HubSpot.Runners.OpenApiClient.Utils.Abstract;
using Soenneker.Utils.Dotnet.Abstract;
using Soenneker.Utils.Environment;
using Soenneker.Utils.Process.Abstract;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.File.Abstract;
using System.Collections.Generic;
using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Nodes;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Utils.File.Download.Abstract;

namespace Soenneker.HubSpot.Runners.OpenApiClient.Utils;

///<inheritdoc cref="IFileOperationsUtil"/>
public sealed class FileOperationsUtil : IFileOperationsUtil
{
    private static readonly string[] _componentSections =
    [
        "schemas",
        "parameters",
        "responses",
        "requestBodies",
        "headers",
        "securitySchemes",
        "links",
        "callbacks",
        "examples"
    ];

    private readonly ILogger<FileOperationsUtil> _logger;
    private readonly IGitUtil _gitUtil;
    private readonly IDotnetUtil _dotnetUtil;
    private readonly IProcessUtil _processUtil;
    private readonly IOpenApiFixer _openApiFixer;
    private readonly IFileUtil _fileUtil;
    private readonly IDirectoryUtil _directoryUtil;
    private readonly IFileDownloadUtil _fileDownloadUtil;


    public FileOperationsUtil(ILogger<FileOperationsUtil> logger, IGitUtil gitUtil, IDotnetUtil dotnetUtil, IProcessUtil processUtil,
        IOpenApiFixer openApiFixer, IFileUtil fileUtil, IDirectoryUtil directoryUtil, IFileDownloadUtil fileDownloadUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _dotnetUtil = dotnetUtil;
        _processUtil = processUtil;
        _openApiFixer = openApiFixer;
        _fileUtil = fileUtil;
        _directoryUtil = directoryUtil;
        _fileDownloadUtil = fileDownloadUtil;
    }

    public async ValueTask Process(CancellationToken cancellationToken = default)
    {
        string gitDirectory = await _gitUtil.CloneToTempDirectory($"https://github.com/soenneker/{Constants.Library.ToLowerInvariantFast()}",
            cancellationToken: cancellationToken);

        var tempDir = await _directoryUtil.CreateTempDirectory(cancellationToken);

        var zipFilePath = Path.Combine(tempDir, "main.zip");

        await _fileDownloadUtil.Download("https://github.com/HubSpot/HubSpot-public-api-spec-collection/archive/refs/heads/main.zip", zipFilePath,
            cancellationToken: cancellationToken);

        var specsDir = Path.Combine(tempDir, "main");

        await ZipFile.ExtractToDirectoryAsync(zipFilePath, specsDir, cancellationToken);

        string publicApiSpecsDirectory = Path.Combine(specsDir, "HubSpot-public-api-spec-collection-main", "PublicApiSpecs");

        if (!await _directoryUtil.Exists(publicApiSpecsDirectory, cancellationToken))
            throw new DirectoryNotFoundException($"PublicApiSpecs directory does not exist at {publicApiSpecsDirectory}");

        string targetFilePath = Path.Combine(gitDirectory, "merged.json");
        string fixedFilePath = Path.Combine(gitDirectory, "fixed.json");

        await _fileUtil.DeleteIfExists(targetFilePath, cancellationToken: cancellationToken);
        await _fileUtil.DeleteIfExists(fixedFilePath, cancellationToken: cancellationToken);

        List<SpecCandidate> latestSpecCandidates = await GetLatestSpecCandidates(publicApiSpecsDirectory, cancellationToken);

        if (latestSpecCandidates.Count == 0)
            throw new Exception("No valid HubSpot OpenAPI specs were found.");

        string mergedSpec = await MergeOpenApiSpecs(latestSpecCandidates, cancellationToken);
        await _fileUtil.Write(targetFilePath, mergedSpec, true, cancellationToken);

        await _processUtil.Start("dotnet", null, "tool update --global Microsoft.OpenApi.Kiota", waitForExit: true, cancellationToken: cancellationToken);
        await _openApiFixer.Fix(targetFilePath, fixedFilePath, cancellationToken)
                           .NoSync();

        string srcDirectory = Path.Combine(gitDirectory, "src");

        await DeleteAllExceptCsproj(srcDirectory, cancellationToken);

        await _processUtil.Start("kiota", gitDirectory,
                              $"kiota generate -l CSharp -d \"{fixedFilePath}\" -o src -c HubSpotOpenApiClient -n {Constants.Library}", waitForExit: true,
                              cancellationToken: cancellationToken)
                          .NoSync();

        await BuildAndPush(gitDirectory, cancellationToken)
            .NoSync();
    }

    private async ValueTask<List<SpecCandidate>> GetLatestSpecCandidates(string publicApiSpecsDirectory, CancellationToken cancellationToken)
    {
        List<string> allFiles = await _directoryUtil.GetFilesByExtension(publicApiSpecsDirectory, "", true, cancellationToken);
        var candidateByCategory = new Dictionary<string, SpecCandidate>(StringComparer.OrdinalIgnoreCase);

        foreach (string file in allFiles)
        {
            if (!file.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!TryParseSpecMetadata(file, publicApiSpecsDirectory, out string categoryKey, out int version, out int rollout))
                continue;

            bool isOpenApiSpec = await IsOpenApiSpec(file, cancellationToken);
            if (!isOpenApiSpec)
                continue;

            string componentPrefix = ToSafeIdentifier(categoryKey);
            var candidate = new SpecCandidate(categoryKey, file, version, rollout, componentPrefix);

            if (!candidateByCategory.TryGetValue(categoryKey, out SpecCandidate? existing))
            {
                candidateByCategory[categoryKey] = candidate;
                continue;
            }

            if (candidate.Version > existing.Version || (candidate.Version == existing.Version && candidate.Rollout > existing.Rollout))
                candidateByCategory[categoryKey] = candidate;
        }

        List<SpecCandidate> selected = candidateByCategory.Values.OrderBy(c => c.CategoryKey)
                                                          .ToList();

        _logger.LogInformation("Selected {Count} latest category specs out of {Total} files", selected.Count, allFiles.Count);

        return selected;
    }

    private async ValueTask<bool> IsOpenApiSpec(string filePath, CancellationToken cancellationToken)
    {
        try
        {
            string json = await File.ReadAllTextAsync(filePath, cancellationToken);
            JsonNode? node = JsonNode.Parse(json);

            if (node is not JsonObject root)
                return false;

            return root["openapi"] is JsonValue && root["paths"] is JsonObject;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Skipping unreadable or invalid JSON file: {FilePath}", filePath);
            return false;
        }
    }

    private static bool TryParseSpecMetadata(string filePath, string publicApiSpecsDirectory, out string categoryKey, out int version, out int rollout)
    {
        categoryKey = "";
        version = 0;
        rollout = 0;

        string relative = Path.GetRelativePath(publicApiSpecsDirectory, filePath);

        string[] segments = relative.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);

        int rolloutsIndex = Array.FindIndex(segments, s => s.Equals("Rollouts", StringComparison.OrdinalIgnoreCase));

        if (rolloutsIndex < 1 || rolloutsIndex + 2 >= segments.Length)
            return false;

        if (!int.TryParse(segments[rolloutsIndex + 1], out rollout))
            return false;

        string versionSegment = segments[rolloutsIndex + 2];
        if (versionSegment.Length < 2 || (versionSegment[0] != 'v' && versionSegment[0] != 'V'))
            return false;

        if (!int.TryParse(versionSegment[1..], out version))
            return false;

        categoryKey = string.Join("/", segments.Take(rolloutsIndex));
        return !string.IsNullOrWhiteSpace(categoryKey);
    }

    private async ValueTask<string> MergeOpenApiSpecs(List<SpecCandidate> candidates, CancellationToken cancellationToken)
    {
        var mergedRoot = new JsonObject
        {
            ["openapi"] = "3.0.1",
            ["info"] = new JsonObject
            {
                ["title"] = "HubSpot Public API",
                ["version"] = "1.0.0"
            },
            ["paths"] = new JsonObject(),
            ["components"] = new JsonObject(),
            ["servers"] = new JsonArray(),
            ["tags"] = new JsonArray()
        };

        var mergedPaths = mergedRoot["paths"]!.AsObject();
        var mergedComponents = mergedRoot["components"]!.AsObject();
        var mergedServers = mergedRoot["servers"]!.AsArray();
        var mergedTags = mergedRoot["tags"]!.AsArray();

        foreach (SpecCandidate candidate in candidates)
        {
            JsonObject? spec = await ReadJsonObject(candidate.FilePath, cancellationToken);

            if (spec == null)
                continue;

            RewriteComponentNamesAndRefs(spec, candidate.ComponentPrefix);
            MergePaths(spec, mergedPaths);
            MergeComponents(spec, mergedComponents);
            MergeServers(spec, mergedServers);
            MergeTags(spec, mergedTags);
        }

        return mergedRoot.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }

    private async ValueTask<JsonObject?> ReadJsonObject(string filePath, CancellationToken cancellationToken)
    {
        try
        {
            string json = await File.ReadAllTextAsync(filePath, cancellationToken);
            return JsonNode.Parse(json) as JsonObject;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse JSON file {FilePath}", filePath);
            return null;
        }
    }

    private static void RewriteComponentNamesAndRefs(JsonObject root, string componentPrefix)
    {
        if (root["components"] is not JsonObject components)
            return;

        var refMap = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (string section in _componentSections)
        {
            if (components[section] is not JsonObject sectionObject)
                continue;

            List<string> keys = sectionObject.Select(kvp => kvp.Key)
                                             .ToList();

            foreach (string key in keys)
            {
                JsonNode? value = sectionObject[key];
                sectionObject.Remove(key);

                string newKey = $"{componentPrefix}_{key}";
                while (sectionObject.ContainsKey(newKey))
                {
                    newKey = "_" + newKey;
                }

                sectionObject[newKey] = value?.DeepClone();
                refMap[$"#/components/{section}/{key}"] = $"#/components/{section}/{newKey}";
            }
        }

        if (refMap.Count > 0)
            RewriteRefs(root, refMap);
    }

    private static void RewriteRefs(JsonNode? node, Dictionary<string, string> refMap)
    {
        if (node is null)
            return;

        if (node is JsonObject obj)
        {
            foreach (KeyValuePair<string, JsonNode?> kvp in obj.ToList())
            {
                if (kvp.Key == "$ref" && kvp.Value is JsonValue refValue)
                {
                    string? current = refValue.GetValue<string>();
                    if (current != null && refMap.TryGetValue(current, out string? replacement))
                    {
                        obj[kvp.Key] = replacement;
                    }
                }
                else
                {
                    RewriteRefs(kvp.Value, refMap);
                }
            }

            return;
        }

        if (node is JsonArray array)
        {
            foreach (JsonNode? item in array)
            {
                RewriteRefs(item, refMap);
            }
        }
    }

    private static void MergePaths(JsonObject sourceSpec, JsonObject mergedPaths)
    {
        if (sourceSpec["paths"] is not JsonObject sourcePaths)
            return;

        foreach ((string path, JsonNode? pathNode) in sourcePaths)
        {
            if (pathNode is null)
                continue;

            if (mergedPaths[path] is not JsonObject existingPath || pathNode is not JsonObject incomingPath)
            {
                mergedPaths[path] = pathNode.DeepClone();
                continue;
            }

            foreach ((string method, JsonNode? operationNode) in incomingPath)
            {
                existingPath[method] = operationNode?.DeepClone();
            }
        }
    }

    private static void MergeComponents(JsonObject sourceSpec, JsonObject mergedComponents)
    {
        if (sourceSpec["components"] is not JsonObject sourceComponents)
            return;

        foreach ((string sectionName, JsonNode? sectionNode) in sourceComponents)
        {
            if (sectionNode is not JsonObject sourceSection)
                continue;

            JsonObject destinationSection;
            if (mergedComponents[sectionName] is JsonObject existingSection)
            {
                destinationSection = existingSection;
            }
            else
            {
                destinationSection = new JsonObject();
                mergedComponents[sectionName] = destinationSection;
            }

            foreach ((string key, JsonNode? value) in sourceSection)
            {
                destinationSection[key] = value?.DeepClone();
            }
        }
    }

    private static void MergeServers(JsonObject sourceSpec, JsonArray mergedServers)
    {
        if (sourceSpec["servers"] is not JsonArray sourceServers)
            return;

        var existingUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (JsonNode? node in mergedServers)
        {
            if (node is JsonObject server && server["url"] is JsonValue urlValue)
            {
                string? url = urlValue.GetValue<string>();
                if (!string.IsNullOrWhiteSpace(url))
                    existingUrls.Add(url);
            }
        }

        foreach (JsonNode? node in sourceServers)
        {
            if (node is not JsonObject server || server["url"] is not JsonValue urlValue)
                continue;

            string? url = urlValue.GetValue<string>();
            if (string.IsNullOrWhiteSpace(url) || existingUrls.Contains(url))
                continue;

            mergedServers.Add(server.DeepClone());
            existingUrls.Add(url);
        }
    }

    private static void MergeTags(JsonObject sourceSpec, JsonArray mergedTags)
    {
        if (sourceSpec["tags"] is not JsonArray sourceTags)
            return;

        var existingTagNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (JsonNode? node in mergedTags)
        {
            if (node is JsonObject tag && tag["name"] is JsonValue nameValue)
            {
                string? name = nameValue.GetValue<string>();
                if (!string.IsNullOrWhiteSpace(name))
                    existingTagNames.Add(name);
            }
        }

        foreach (JsonNode? node in sourceTags)
        {
            if (node is not JsonObject tag || tag["name"] is not JsonValue nameValue)
                continue;

            string? name = nameValue.GetValue<string>();
            if (string.IsNullOrWhiteSpace(name) || existingTagNames.Contains(name))
                continue;

            mergedTags.Add(tag.DeepClone());
            existingTagNames.Add(name);
        }
    }

    private static string ToSafeIdentifier(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return "spec";

        char[] chars = input.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_')
                            .ToArray();

        string result = new string(chars);

        result = result.Trim('_');

        if (result.Length == 0)
            return "spec";

        if (char.IsDigit(result[0]))
            result = "spec_" + result;

        return result;
    }

    public async ValueTask DeleteAllExceptCsproj(string directoryPath, CancellationToken cancellationToken = default)
    {
        if (!(await _directoryUtil.Exists(directoryPath, cancellationToken)))
        {
            _logger.LogWarning("Directory does not exist: {DirectoryPath}", directoryPath);
            return;
        }

        try
        {
            // Delete all files except .csproj
            List<string> files = await _directoryUtil.GetFilesByExtension(directoryPath, "", true, cancellationToken);
            foreach (string file in files)
            {
                if (!file.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        await _fileUtil.Delete(file, ignoreMissing: true, log: false, cancellationToken);
                        _logger.LogInformation("Deleted file: {FilePath}", file);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to delete file: {FilePath}", file);
                    }
                }
            }

            // Delete all empty subdirectories
            List<string> dirs = await _directoryUtil.GetAllDirectoriesRecursively(directoryPath, cancellationToken);
            foreach (string dir in dirs.OrderByDescending(d => d.Length)) // Sort by depth to delete from deepest first
            {
                try
                {
                    List<string> dirFiles = await _directoryUtil.GetFilesByExtension(dir, "", false, cancellationToken);
                    List<string> subDirs = await _directoryUtil.GetAllDirectories(dir, cancellationToken);
                    if (dirFiles.Count == 0 && subDirs.Count == 0)
                    {
                        await _directoryUtil.Delete(dir, cancellationToken);
                        _logger.LogInformation("Deleted empty directory: {DirectoryPath}", dir);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete directory: {DirectoryPath}", dir);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while cleaning the directory: {DirectoryPath}", directoryPath);
        }
    }

    private async ValueTask BuildAndPush(string gitDirectory, CancellationToken cancellationToken)
    {
        string projFilePath = Path.Combine(gitDirectory, "src", $"{Constants.Library}.csproj");

        await _dotnetUtil.Restore(projFilePath, cancellationToken: cancellationToken);

        bool successful = await _dotnetUtil.Build(projFilePath, true, "Release", false, cancellationToken: cancellationToken);

        if (!successful)
        {
            _logger.LogError("Build was not successful, exiting...");
            return;
        }

        string gitHubToken = EnvironmentUtil.GetVariableStrict("GH__TOKEN");

        await _gitUtil.CommitAndPush(gitDirectory, "Automated update", gitHubToken, "Jake Soenneker", "jake@soenneker.com", cancellationToken);
    }

    private sealed record SpecCandidate(string CategoryKey, string FilePath, int Version, int Rollout, string ComponentPrefix);
}