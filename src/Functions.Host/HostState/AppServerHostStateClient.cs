// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Rpc.Client;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Extensions.Configuration;

namespace Azure.Functions.Host.HostState;

internal enum AppServerHostStatePublishResult
{
    Applied,
    CapacityBlocked,
    Unsupported,
    TransientFailure,
    Rejected,
}

internal enum RuntimeLifecycleStatus
{
    Draining,
    RemovalRequested,
}

/// <summary>
/// Publishes Host capacity through AppServer's existing Host-state transaction.
/// </summary>
internal sealed class AppServerHostStateClient(
    IHttpClientFactory httpClientFactory,
    IHttpWorkerScaleInManager scaleInManager,
    IRuntimeRetirementWorkerManager retirementWorkerManager,
    TimeProvider timeProvider,
    IConfiguration configuration)
{
    internal const string CapacityFenceHeader = "x-ms-functions-host-state-capacity-fence";
    internal const string CapacityFenceApplied = "applied";
    internal const string CapacityFenceRequired = "required";
    internal const string DefaultEndpoint = "http://localhost:6060/";
    internal const string DrainingStatus = "Draining";
    internal const string HostStatePath = "admin/infra/host/state";
    internal const string HttpClientName = "AppServerHostState";
    internal const string LifecycleCapacityFenceRequired = "lifecycle-required";
    internal const string RemovalRequestedStatus = "RemovalRequested";

    private readonly IConfiguration _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    private readonly string? _containerName = configuration[EnvironmentSettingNames.ContainerName];
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
    private readonly SemaphoreSlim _publicationGate = new(1, 1);
    private readonly IHttpWorkerScaleInManager _scaleInManager =
        scaleInManager ?? throw new ArgumentNullException(nameof(scaleInManager));

    private readonly IRuntimeRetirementWorkerManager _retirementWorkerManager =
        retirementWorkerManager ?? throw new ArgumentNullException(nameof(retirementWorkerManager));

    private readonly TimeProvider _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    private DateTimeOffset _lastCreatedTime;

    public async Task<AppServerHostStatePublishResult> PublishAsync(
        HttpWorkerScaleInState state,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        return await PublishAsync(
            state,
            status: null,
            acquireClosed: null,
            allowDuringRetirement: false,
            cancellationToken);
    }

    internal Task<AppServerHostStatePublishResult> BindRuntimeIncarnationAsync(
        CancellationToken cancellationToken)
        => PublishAsync(
            state: null,
            status: null,
            acquireClosed: null,
            allowDuringRetirement: true,
            cancellationToken);

    internal Task<AppServerHostStatePublishResult> PublishLifecycleAsync(
        RuntimeLifecycleStatus status,
        HttpWorkerScaleInState state,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        return PublishAsync(
            state,
            status,
            acquireClosed: true,
            allowDuringRetirement: true,
            cancellationToken);
    }

    private async Task<AppServerHostStatePublishResult> PublishAsync(
        HttpWorkerScaleInState? state,
        RuntimeLifecycleStatus? status,
        bool? acquireClosed,
        bool allowDuringRetirement,
        CancellationToken cancellationToken)
    {
        await _publicationGate.WaitAsync(cancellationToken);
        try
        {
            if (string.IsNullOrWhiteSpace(_containerName) ||
                (!allowDuringRetirement && _retirementWorkerManager.IsRetirementRequested))
            {
                return AppServerHostStatePublishResult.Unsupported;
            }

            state ??= _scaleInManager.Current;
            DateTimeOffset createdTime = status is null
                ? state.CreatedTime
                : GetNextCreatedTime(state.CreatedTime);
            if (createdTime > _lastCreatedTime)
            {
                _lastCreatedTime = createdTime;
            }

            using HttpRequestMessage request = new(HttpMethod.Put, GetHostStateUri())
            {
                Content = JsonContent.Create(
                    new HostStateRequest(
                        createdTime,
                        state.LinkedWorkerCount,
                        status is null ? state.HttpCapacity : 0,
                        _retirementWorkerManager.Incarnation.Value,
                        GetLifecycleStatusValue(status),
                        acquireClosed,
                        state.OutstandingWorkerRemovals),
                    AppServerHostStateJsonContext.Default.HostStateRequest),
            };
            request.Headers.Add(ScriptConstants.ContainerInstanceHeader, _containerName);
            request.Headers.Add(
                CapacityFenceHeader,
                status is null ? CapacityFenceRequired : LifecycleCapacityFenceRequired);

            using HttpResponseMessage response = await _httpClientFactory.CreateClient(HttpClientName).SendAsync(
                request,
                cancellationToken);

            if (response.StatusCode is HttpStatusCode.PreconditionFailed)
            {
                return AppServerHostStatePublishResult.CapacityBlocked;
            }

            if (response.IsSuccessStatusCode)
            {
                return response.Headers.TryGetValues(CapacityFenceHeader, out var values) &&
                    values.Any(value => string.Equals(value, CapacityFenceApplied, StringComparison.OrdinalIgnoreCase))
                        ? AppServerHostStatePublishResult.Applied
                        : AppServerHostStatePublishResult.Unsupported;
            }

            return response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden ||
                (int)response.StatusCode >= 500
                    ? AppServerHostStatePublishResult.TransientFailure
                    : AppServerHostStatePublishResult.Rejected;
        }
        finally
        {
            _publicationGate.Release();
        }
    }

    private DateTimeOffset GetNextCreatedTime(DateTimeOffset stateCreatedTime)
    {
        DateTimeOffset createdTime = _timeProvider.GetUtcNow();
        DateTimeOffset minimumCreatedTime = stateCreatedTime > _lastCreatedTime
            ? stateCreatedTime
            : _lastCreatedTime;

        return createdTime > minimumCreatedTime
            ? createdTime
            : minimumCreatedTime.AddTicks(1);
    }

    private static string? GetLifecycleStatusValue(RuntimeLifecycleStatus? status)
        => status switch
        {
            RuntimeLifecycleStatus.Draining => DrainingStatus,
            RuntimeLifecycleStatus.RemovalRequested => RemovalRequestedStatus,
            null => null,
            _ => throw new ArgumentOutOfRangeException(nameof(status)),
        };

    private Uri GetHostStateUri()
    {
        string? endpoint = _configuration[EnvironmentSettingNames.MeshInitURI];
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            endpoint = DefaultEndpoint;
        }

        return new Uri(new Uri(endpoint.EndsWith('/') ? endpoint : endpoint + "/", UriKind.Absolute), HostStatePath);
    }

    internal sealed record HostStateRequest(
        DateTimeOffset CreatedTime,
        int LinkedWorkerCount,
        long HttpCapacity,
        Guid RuntimeIncarnationId,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        string? Status,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        bool? AcquireClosed,
        IReadOnlyList<FunctionsSplitWorkerRemovalAction> OutstandingWorkerRemovals);
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(AppServerHostStateClient.HostStateRequest))]
internal sealed partial class AppServerHostStateJsonContext : JsonSerializerContext;
