// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.Controllers;
using Azure.Functions.Host.HostState;
using Azure.Functions.Host.Models;
using Azure.Functions.Host.WorkerLink;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace Azure.Functions.Rpc.Client.Tests;

public sealed partial class WorkerChannelRegistryTests
{
    private static readonly TimeSpan RemovalUnlinkTimeout = TimeSpan.FromMilliseconds(250);

    [Fact]
    public async Task UncertainUnlink_ExactAcknowledgementClearsActionAndPublishesUpdatedState()
    {
        using CancellationTokenSource timeout = new(TestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateRemovalRegistry(clientFactory);
        IHttpWorkerScaleInManager scaleInManager = registry;
        FunctionsSplitWorkerRemovalAction action = await CreateFallbackAsync(
            registry,
            server,
            "worker-pod-first",
            timeout.Token);
        CapturingHostStateHandler handler = new();
        using HttpClient httpClient = new(handler);
        Mock<IHttpClientFactory> httpClientFactory = new();
        httpClientFactory
            .Setup(factory => factory.CreateClient(AppServerHostStateClient.HttpClientName))
            .Returns(httpClient);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [EnvironmentSettingNames.ContainerName] = "test-container",
                [EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
            })
            .Build();
        IRuntimeRetirementWorkerManager retirementWorkerManager = registry;
        HttpOnDemandWorkerScaleInService service = new(
            scaleInManager,
            retirementWorkerManager,
            new AppServerHostStateClient(
                httpClientFactory.Object,
                scaleInManager,
                retirementWorkerManager,
                TimeProvider.System,
                configuration),
            TimeProvider.System,
            NullLogger<HttpOnDemandWorkerScaleInService>.Instance);

        await service.RunScaleInPassAsync(timeout.Token);
        WorkerRemovalAcknowledgementController controller = new(scaleInManager);
        Assert.IsType<NoContentResult>(controller.AcknowledgeWorkerRemoval(new(action)));
        await service.RunScaleInPassAsync(timeout.Token);

        Assert.Equal(2, handler.PublishedStates.Count);
        Assert.Equal(action, Assert.Single(handler.PublishedStates[0].OutstandingWorkerRemovals));
        Assert.Empty(handler.PublishedStates[1].OutstandingWorkerRemovals);
        Assert.True(handler.PublishedStates[1].CreatedTime > handler.PublishedStates[0].CreatedTime);
        JsonElement publishedAction = handler.PublishedJson[0]
            .GetProperty("outstandingWorkerRemovals")[0];
        Assert.Equal("RuntimeUnlink", publishedAction.GetProperty("reason").GetString());
        JsonElement publishedWorker = publishedAction.GetProperty("worker");
        Assert.Equal(
            new[] { "podName", "resourceNamespace" },
            publishedWorker.EnumerateObject().Select(property => property.Name).Order(StringComparer.Ordinal));
        Assert.Equal(action.Worker.ResourceNamespace, publishedWorker.GetProperty("resourceNamespace").GetString());
        Assert.Equal(action.Worker.PodName, publishedWorker.GetProperty("podName").GetString());
    }

    [Fact]
    public async Task AcknowledgeWorkerRemoval_IdenticalReplaySucceedsWithoutReopeningRemoval()
    {
        using CancellationTokenSource timeout = new(TestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateRemovalRegistry(clientFactory);
        IHttpWorkerScaleInManager scaleInManager = registry;
        FunctionsSplitWorkerRemovalAction action = await CreateFallbackAsync(
            registry,
            server,
            "worker-pod-replay",
            timeout.Token);
        WorkerRemovalAcknowledgementController controller = new(scaleInManager);

        Assert.IsType<NoContentResult>(controller.AcknowledgeWorkerRemoval(new(action)));
        long acceptedVersion = scaleInManager.Version;
        Assert.IsType<NoContentResult>(controller.AcknowledgeWorkerRemoval(new(action)));

        Assert.Empty(scaleInManager.Current.OutstandingWorkerRemovals);
        Assert.Equal(acceptedVersion, scaleInManager.Version);
    }

    [Theory]
    [InlineData("changed-reason")]
    [InlineData("stale-worker")]
    [InlineData("different-pod")]
    [InlineData("unknown")]
    [InlineData("stale-runtime")]
    [InlineData("name-only")]
    [InlineData("malformed")]
    public async Task AcknowledgeWorkerRemoval_NonExactActionFailsWithoutClearing(
        string mismatch)
    {
        using CancellationTokenSource timeout = new(TestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateRemovalRegistry(clientFactory);
        IHttpWorkerScaleInManager scaleInManager = registry;
        FunctionsSplitWorkerRemovalAction action = await CreateFallbackAsync(
            registry,
            server,
            "worker-pod-match",
            timeout.Token);
        FunctionsSplitWorkerRemovalAction candidate = mismatch switch
        {
            "changed-reason" => action with { Reason = "ChangedReason" },
            "stale-worker" => action with
            {
                Worker = action.Worker with { ResourceNamespace = "other-resource-namespace" },
            },
            "different-pod" => action with
            {
                Worker = action.Worker with { PodName = "worker-pod-other" },
            },
            "unknown" => action with
            {
                Worker = new("other-resource-namespace", "worker-pod-unknown"),
            },
            "name-only" => action with
            {
                Worker = new(
                    string.Empty,
                    action.Worker.PodName),
            },
            "malformed" => action with { Reason = string.Empty },
            "stale-runtime" => action,
            _ => throw new ArgumentOutOfRangeException(nameof(mismatch)),
        };
        IActionResult result;

        if (string.Equals(mismatch, "stale-runtime", StringComparison.Ordinal))
        {
            RegistryHarness staleHarness = new();
            await using WorkerChannelRegistry staleRegistry = staleHarness.Registry;
            result = new WorkerRemovalAcknowledgementController(staleRegistry)
                .AcknowledgeWorkerRemoval(new(candidate));
        }
        else
        {
            result = new WorkerRemovalAcknowledgementController(scaleInManager)
                .AcknowledgeWorkerRemoval(new(candidate));
        }

        if (string.Equals(mismatch, "name-only", StringComparison.Ordinal) ||
            string.Equals(mismatch, "malformed", StringComparison.Ordinal))
        {
            Assert.IsType<BadRequestResult>(result);
        }
        else
        {
            Assert.IsType<NotFoundResult>(result);
        }

        Assert.Equal(action, Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals));
    }

    [Fact]
    public async Task AcknowledgeWorkerRemoval_MultipleActionsClearIndependently()
    {
        using CancellationTokenSource timeout = new(TestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateRemovalRegistry(clientFactory);
        IHttpWorkerScaleInManager scaleInManager = registry;
        FunctionsSplitWorkerRemovalAction first = await CreateFallbackAsync(
            registry,
            server,
            "worker-pod-one",
            timeout.Token);
        FunctionsSplitWorkerRemovalAction second = await CreateFallbackAsync(
            registry,
            server,
            "worker-pod-two",
            timeout.Token);
        WorkerRemovalAcknowledgementController controller = new(scaleInManager);

        Assert.IsType<NoContentResult>(controller.AcknowledgeWorkerRemoval(new(first)));
        Assert.Equal(second, Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals));
        Assert.IsType<NoContentResult>(controller.AcknowledgeWorkerRemoval(new(second)));

        Assert.Empty(scaleInManager.Current.OutstandingWorkerRemovals);
    }

    [Fact]
    public async Task UnlinkFallback_RequiresMatchingIdentityAndUncertainPrimaryHandoff()
    {
        Mock<IWorkerChannelRegistry> linkRegistry = new(MockBehavior.Strict);
        WorkerLinkController linkController = new(
            NullLogger<WorkerLinkController>.Instance,
            linkRegistry.Object);
        WorkerLinkRequest mismatchedRequest = new()
        {
            WorkerGrpcEndpoint = "http://worker-proxy:50053",
            WorkerNamespace = "resource-namespace",
            WorkerPodName = "different-worker",
        };

        BadRequestObjectResult mismatch = Assert.IsType<BadRequestObjectResult>(
            await linkController.LinkWorker("route-worker", mismatchedRequest));
        RequestValidationResponse validation = Assert.IsType<RequestValidationResponse>(mismatch.Value);
        Assert.Contains(validation.Errors, error =>
            string.Equals(error.Code, "WorkerIdentityMismatch", StringComparison.Ordinal));
        WorkerLinkRequest placeholderRequest = new()
        {
            WorkerGrpcEndpoint = "http://worker-proxy:50053",
            WorkerNamespace = new string('a', 257),
            WorkerPodName = "route-worker",
        };
        BadRequestObjectResult placeholder = Assert.IsType<BadRequestObjectResult>(
            await linkController.LinkWorker("route-worker", placeholderRequest));
        RequestValidationResponse placeholderValidation =
            Assert.IsType<RequestValidationResponse>(placeholder.Value);
        Assert.Contains(placeholderValidation.Errors, error =>
            string.Equals(error.Code, "InvalidWorkerIdentity", StringComparison.Ordinal));
        linkRegistry.VerifyNoOtherCalls();

        using CancellationTokenSource timeout = new(TestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        FakeWorkerProxyGrpcServer.TestWorker legacyWorker = server.AddWorker("worker-pod-legacy");
        legacyWorker.HoldCloseResponse();
        FakeWorkerProxyGrpcServer.TestWorker exactWorker = server.AddWorker("worker-pod-primary");
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateRemovalRegistry(clientFactory);
        IHttpWorkerScaleInManager scaleInManager = registry;
        await registry.LinkAsync("worker-pod-legacy", server.Endpoint, timeout.Token);
        FunctionsSplitPodIdentity primaryIdentity = CreateWorkerIdentity("worker-pod-primary");
        await registry.LinkAsync(
            "worker-pod-primary",
            server.Endpoint,
            primaryIdentity,
            timeout.Token);
        WorkerLinkException legacyIdentityConflict = await Assert.ThrowsAsync<WorkerLinkException>(
            () => registry.LinkAsync(
                "worker-pod-legacy",
                server.Endpoint,
                CreateWorkerIdentity("worker-pod-legacy"),
                timeout.Token));
        Assert.Equal(WorkerLinkFailureReason.Conflict, legacyIdentityConflict.Reason);
        WorkerLinkException primaryIdentityConflict = await Assert.ThrowsAsync<WorkerLinkException>(
            () => registry.LinkAsync(
                "worker-pod-primary",
                server.Endpoint,
                primaryIdentity with { ResourceNamespace = "other-resource-namespace" },
                timeout.Token));
        Assert.Equal(WorkerLinkFailureReason.Conflict, primaryIdentityConflict.Reason);
        WorkerLinkResult primaryReplay = await registry.LinkAsync(
            "worker-pod-primary",
            server.Endpoint,
            primaryIdentity,
            timeout.Token);
        Assert.False(primaryReplay.IsNewLink);

        Task<bool> legacyUnlink = registry.UnlinkAsync("worker-pod-legacy", timeout.Token);
        await legacyWorker.RequestsCompleted.WaitAsync(timeout.Token);
        await Assert.ThrowsAsync<TimeoutException>(() => legacyUnlink.WaitAsync(timeout.Token));
        Assert.Empty(scaleInManager.Current.OutstandingWorkerRemovals);

        Assert.True(await registry.UnlinkAsync("worker-pod-primary", timeout.Token));
        await exactWorker.RequestsCompleted.WaitAsync(timeout.Token);
        await exactWorker.Disconnected.WaitAsync(timeout.Token);
        Assert.Empty(scaleInManager.Current.OutstandingWorkerRemovals);
    }

    private static WorkerChannelRegistry CreateRemovalRegistry(RpcClientFactory clientFactory)
    {
        FunctionRpcDuplexChannelFactory duplexFactory =
            new(clientFactory, NullLogger<FunctionRpcDuplexChannelFactory>.Instance);
        RpcClientWorkerChannelFactory channelFactory = CreateRealChannelFactory();

        return new(
            duplexFactory,
            channelFactory,
            NullLogger<WorkerChannelRegistry>.Instance,
            linkTimeout: TimeSpan.FromSeconds(30),
            timeProvider: null,
            unlinkTimeout: RemovalUnlinkTimeout);
    }

    private static async Task<FunctionsSplitWorkerRemovalAction> CreateFallbackAsync(
        WorkerChannelRegistry registry,
        FakeWorkerProxyGrpcServer server,
        string workerId,
        CancellationToken cancellationToken)
    {
        FakeWorkerProxyGrpcServer.TestWorker worker = server.AddWorker(workerId);
        worker.HoldCloseResponse();
        await registry.LinkAsync(
            workerId,
            server.Endpoint,
            CreateWorkerIdentity(workerId),
            cancellationToken);

        Task<bool> unlink = registry.UnlinkAsync(workerId, cancellationToken);
        await worker.RequestsCompleted.WaitAsync(cancellationToken);
        await Assert.ThrowsAsync<TimeoutException>(() => unlink.WaitAsync(cancellationToken));
        await worker.Disconnected.WaitAsync(cancellationToken);

        return Assert.Single(((IHttpWorkerScaleInManager)registry).Current.OutstandingWorkerRemovals.Where(
            action => string.Equals(action.Worker.PodName, workerId, StringComparison.Ordinal)));
    }

    private static FunctionsSplitPodIdentity CreateWorkerIdentity(string workerId)
        => new(
            "resource-namespace",
            workerId);

    private sealed class CapturingHostStateHandler : HttpMessageHandler
    {
        public List<JsonElement> PublishedJson { get; } = [];

        public List<AppServerHostStateClient.HostStateRequest> PublishedStates { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            string json = await request.Content!.ReadAsStringAsync(cancellationToken);
            using JsonDocument document = JsonDocument.Parse(json);
            PublishedJson.Add(document.RootElement.Clone());
            AppServerHostStateClient.HostStateRequest state = Assert.IsType<AppServerHostStateClient.HostStateRequest>(
                JsonSerializer.Deserialize(json, AppServerHostStateJsonContext.Default.HostStateRequest));
            PublishedStates.Add(state);

            HttpResponseMessage response = new(HttpStatusCode.OK);
            response.Headers.TryAddWithoutValidation(
                AppServerHostStateClient.CapacityFenceHeader,
                AppServerHostStateClient.CapacityFenceApplied);

            return response;
        }
    }
}
