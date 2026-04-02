using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Soenneker.Extensions.String;
using Soenneker.Extensions.ValueTask;
using Soenneker.Kiota.Util.Abstract;
using Soenneker.Git.Util.Abstract;
using Soenneker.HubSpot.Runners.OpenApiClient.Utils.Abstract;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Merger.Abstract;
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.Dotnet.Abstract;
using Soenneker.Utils.Environment;
using Soenneker.Utils.File.Abstract;
using Soenneker.Utils.File.Download.Abstract;
using Soenneker.Utils.Process.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.HubSpot.Runners.OpenApiClient.Utils;

///<inheritdoc cref="IFileOperationsUtil"/>
public sealed class FileOperationsUtil : IFileOperationsUtil
{
    private readonly ILogger<FileOperationsUtil> _logger;
    private readonly IGitUtil _gitUtil;
    private readonly IDotnetUtil _dotnetUtil;
    private readonly IProcessUtil _processUtil;
    private readonly IKiotaUtil _kiotaUtil;
    private readonly IOpenApiFixer _openApiFixer;
    private readonly IOpenApiMerger _openApiMerger;
    private readonly IFileUtil _fileUtil;
    private readonly IDirectoryUtil _directoryUtil;
    private readonly IFileDownloadUtil _fileDownloadUtil;

    public FileOperationsUtil(ILogger<FileOperationsUtil> logger, IGitUtil gitUtil, IDotnetUtil dotnetUtil, IProcessUtil processUtil,
        IOpenApiFixer openApiFixer, IOpenApiMerger openApiMerger, IFileUtil fileUtil, IDirectoryUtil directoryUtil, IFileDownloadUtil fileDownloadUtil, IKiotaUtil kiotaUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _dotnetUtil = dotnetUtil;
        _processUtil = processUtil;
        _kiotaUtil = kiotaUtil;
        _openApiFixer = openApiFixer;
        _openApiMerger = openApiMerger;
        _fileUtil = fileUtil;
        _directoryUtil = directoryUtil;
        _fileDownloadUtil = fileDownloadUtil;
    }

    public async ValueTask Process(CancellationToken cancellationToken = default)
    {
        string gitDirectory = await _gitUtil.CloneToTempDirectory($"https://github.com/soenneker/{Constants.Library.ToLowerInvariantFast()}",
            cancellationToken: cancellationToken);

        string tempDir = await _directoryUtil.CreateTempDirectory(cancellationToken);
        string zipFilePath = Path.Combine(tempDir, "main.zip");

        await _fileDownloadUtil.Download("https://github.com/HubSpot/HubSpot-public-api-spec-collection/archive/refs/heads/main.zip", zipFilePath,
            cancellationToken: cancellationToken);

        string specsDir = Path.Combine(tempDir, "main");

        await ZipFile.ExtractToDirectoryAsync(zipFilePath, specsDir, cancellationToken);

        string publicApiSpecsDirectory = Path.Combine(specsDir, "HubSpot-public-api-spec-collection-main", "PublicApiSpecs");

        if (!await _directoryUtil.Exists(publicApiSpecsDirectory, cancellationToken))
            throw new DirectoryNotFoundException($"PublicApiSpecs directory does not exist at {publicApiSpecsDirectory}");

        string targetFilePath = Path.Combine(gitDirectory, "merged.json");
        string fixedFilePath = Path.Combine(gitDirectory, "fixed.json");

        await _fileUtil.DeleteIfExists(targetFilePath, cancellationToken: cancellationToken);
        await _fileUtil.DeleteIfExists(fixedFilePath, cancellationToken: cancellationToken);

        List<SpecCandidate> latestSpecCandidates = await GetLatestSpecCandidates(publicApiSpecsDirectory, cancellationToken).ConfigureAwait(false);

        if (latestSpecCandidates.Count == 0)
            throw new InvalidOperationException("No valid HubSpot OpenAPI specs were found.");

        var mergeInputs = new List<(string prefix, string filePath)>(latestSpecCandidates.Count);

        foreach (SpecCandidate candidate in latestSpecCandidates)
        {
            string mergePrefix = await GetMergePrefix(candidate, cancellationToken).ConfigureAwait(false);
            mergeInputs.Add((mergePrefix, candidate.FilePath));
        }

        _logger.LogInformation("Selected {Count} latest HubSpot category specs. Merging with IOpenApiMerger...", mergeInputs.Count);

        OpenApiDocument mergedOpenApiDocument = await _openApiMerger.MergeOpenApis(mergeInputs, cancellationToken).ConfigureAwait(false);
        string mergedOpenApiJson = _openApiMerger.ToJson(mergedOpenApiDocument);

        await _fileUtil.Write(targetFilePath, mergedOpenApiJson, true, cancellationToken).ConfigureAwait(false);

        await _kiotaUtil.EnsureInstalled(cancellationToken);
        await _openApiFixer.Fix(targetFilePath, fixedFilePath, cancellationToken)
                           .NoSync();

        string srcDirectory = Path.Combine(gitDirectory, "src", Constants.Library);

        await DeleteAllExceptCsproj(srcDirectory, cancellationToken);

        await _kiotaUtil.Generate(fixedFilePath, "HubSpotOpenApiClient", Constants.Library, gitDirectory, cancellationToken).NoSync();

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

            bool isOpenApiSpec = await IsOpenApiSpec(file, cancellationToken).ConfigureAwait(false);
            if (!isOpenApiSpec)
                continue;

            var candidate = new SpecCandidate(categoryKey, file, version, rollout);

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
            string json = await _fileUtil.Read(filePath, log: false, cancellationToken);
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

    private async ValueTask<string> GetMergePrefix(SpecCandidate candidate, CancellationToken cancellationToken)
    {
        JsonObject? spec = await ReadJsonObject(candidate.FilePath, cancellationToken).ConfigureAwait(false);

        if (spec?["paths"] is JsonObject paths)
        {
            foreach (string pathKey in paths.Select(kvp => kvp.Key))
            {
                string? mergePrefix = TryGetTopLevelPathSegment(pathKey);

                if (!string.IsNullOrWhiteSpace(mergePrefix))
                    return mergePrefix;
            }
        }

        string fallbackPrefix = NormalizePathPrefix(candidate.CategoryKey);

        _logger.LogWarning("Falling back to category-derived merge prefix '{MergePrefix}' for {FilePath}", fallbackPrefix, candidate.FilePath);

        return fallbackPrefix;
    }

    private async ValueTask<JsonObject?> ReadJsonObject(string filePath, CancellationToken cancellationToken)
    {
        try
        {
            string json = await _fileUtil.Read(filePath, log: false, cancellationToken);
            return JsonNode.Parse(json) as JsonObject;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse JSON file {FilePath}", filePath);
            return null;
        }
    }

    private static string? TryGetTopLevelPathSegment(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        string[] segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);

        return segments.Length == 0 ? null : segments[0];
    }

    private static string NormalizePathPrefix(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return "spec";

        string firstSegment = input.Split(['/', '\\'], StringSplitOptions.RemoveEmptyEntries)
                                   .FirstOrDefault() ?? input;

        char[] chars = firstSegment.Select(ch => char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '-')
                                   .ToArray();

        string result = new string(chars).Trim('-');

        return result.Length == 0 ? "spec" : result;
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
        string projFilePath = Path.Combine(gitDirectory, "src", Constants.Library, $"{Constants.Library}.csproj");

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

    private sealed record SpecCandidate(string CategoryKey, string FilePath, int Version, int Rollout);
}
