// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.Controllers;
using Azure.Functions.Host.HostState;
using Azure.Functions.Host.RuntimeDrain;
using Azure.Functions.Rpc.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Azure.WebJobs.Script.WebHost;
using Microsoft.Azure.WebJobs.Script.WebHost.Security.Authorization.Policies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace Azure.Functions.Rpc.Client.Tests;

public sealed partial class WorkerChannelRegistryTests
{
    private static readonly TimeSpan AppVersionUnlinkTimeout = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task PostRuntimeDrain_PhasedCommitsFenceReturnsStateReceiptAndValidatesKnownFields()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("DurableFunction");
        ChannelControl worker = new(
            "worker",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            workerRemovalHandoffConfirmed: false);
        ChannelControl readinessPending = new(
            "readiness-pending",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false);
        foreach (ChannelControl linkedWorker in new[] { worker, readinessPending })
        {
            harness.Enqueue(linkedWorker);
            await LinkAsync(registry, linkedWorker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, worker, readinessPending);
        readinessPending.Channel.SetScaleInReady(false);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);
        Assert.True(worker.Channel.TryDispatchInvocation(invocation));
        await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);

        using AppVersionDrainHostStateHandler handler = new(blockFirstDrainFence: true);
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        Task<HttpResponseMessage> request = host.PostAsync(
            """{"mode":"Phased","reason":"ConfigurationChange","extra":true}""");
        await handler.DrainFenceObserved.Task.WaitAsync(TestTimeout);

        Assert.False(request.IsCompleted);
        WorkerLinkException linkFailure = await Assert.ThrowsAsync<WorkerLinkException>(
            () => registry.LinkAsync("late-worker", CreateEndpoint("late-worker")));
        Assert.Equal(WorkerLinkFailureReason.RuntimeStopping, linkFailure.Reason);
        Assert.Collection(
            handler.PublishedStates,
            state =>
            {
                Assert.Null(state.Status);
                Assert.Null(state.AcquireClosed);
                Assert.Equal(((IRuntimeRetirementWorkerManager)registry).Incarnation.Value, state.RuntimeIncarnationId);
            },
            state =>
            {
                Assert.Equal(AppServerHostStateClient.DrainingStatus, state.Status);
                Assert.True(state.AcquireClosed);
                Assert.Equal(0, state.HttpCapacity);
            });

        handler.AllowDrainFence.TrySetResult();
        using HttpResponseMessage accepted = await request.WaitAsync(TestTimeout);
        DrainReceipt firstReceipt = await ReadDrainReceiptAsync(accepted);
        Assert.Equal("Phased", firstReceipt.Mode);
        Assert.Equal(timeProvider.GetUtcNow(), firstReceipt.AcceptedAtUtc);
        await WaitUntilAsync(() => readinessPending.DisposeCount == 1);

        using HttpResponseMessage repeatedMode = await host.PostAsync(CreateDrainJson(
            "Phased",
            "Restart"));
        Assert.Equal(firstReceipt, await ReadDrainReceiptAsync(repeatedMode));

        using HttpResponseMessage unsupportedMode = await host.PostAsync(CreateDrainJson(
            "Rolling",
            "ConfigurationChange"));
        Assert.Equal(HttpStatusCode.BadRequest, unsupportedMode.StatusCode);

        using HttpResponseMessage caseVariant = await host.PostAsync(
            """{"Mode":"Phased","reason":"ConfigurationChange"}""");
        Assert.Equal(HttpStatusCode.BadRequest, caseVariant.StatusCode);

        using HttpResponseMessage duplicateProperty = await host.PostAsync(
            """{"mode":"Phased","mode":"Immediate","reason":"ConfigurationChange"}""");
        Assert.Equal(HttpStatusCode.BadRequest, duplicateProperty.StatusCode);

        using HttpResponseMessage invalidTokenType = await host.PostAsync(
            """{"mode":1,"reason":"ConfigurationChange"}""");
        Assert.Equal(HttpStatusCode.BadRequest, invalidTokenType.StatusCode);

        using HttpResponseMessage missingProperty = await host.PostAsync(
            """{"mode":"Phased"}""");
        Assert.Equal(HttpStatusCode.BadRequest, missingProperty.StatusCode);

        using HttpResponseMessage invalidReason = await host.PostAsync(
            """{"mode":"Phased","reason":"configurationchange"}""");
        Assert.Equal(HttpStatusCode.BadRequest, invalidReason.StatusCode);

        using HttpResponseMessage malformed = await host.PostAsync("""{"mode":""");
        Assert.Equal(HttpStatusCode.BadRequest, malformed.StatusCode);

        DrainManualTimeProvider rejectedTimeProvider = new();
        RegistryHarness rejectedHarness = new(rejectedTimeProvider);
        await using WorkerChannelRegistry rejectedRegistry = rejectedHarness.Registry;
        using AppVersionDrainHostStateHandler rejectedHandler = new(
            drainFenceStatus: HttpStatusCode.Conflict);
        await using AppVersionDrainApiHost rejectedHost = await AppVersionDrainApiHost.StartAsync(
            rejectedRegistry,
            rejectedHandler,
            rejectedTimeProvider);
        using HttpResponseMessage rejected = await rejectedHost.PostAsync(CreateDrainJson(
            "Immediate",
            "Stop"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, rejected.StatusCode);
        Assert.DoesNotContain(
            "acceptedAtUtc",
            await rejected.Content.ReadAsStringAsync(),
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Phased")]
    [InlineData("Immediate")]
    public async Task PostRuntimeDrain_PreSelectionReaderFaultRetainsExactFallback(string mode)
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("PreSelectionFaultFunction");
        ChannelControl worker = new(
            "worker",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            workerRemovalHandoffConfirmed: false);
        harness.Enqueue(worker);
        await registry.LinkAsync(
            worker.Id,
            CreateEndpoint(worker.Id),
            CreateWorkerIdentity(worker.Id));
        await MakeWorkersReadyAsync(registry, function, worker);

        using AppVersionDrainHostStateHandler handler = new(blockFirstDrainFence: true);
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        Task<HttpResponseMessage> request = host.PostAsync(CreateDrainJson(
            mode,
            "ConfigurationChange"));
        await handler.DrainFenceObserved.Task.WaitAsync(TestTimeout);

        worker.Complete(new IOException("reader failed before selection"));
        IHttpWorkerScaleInManager scaleInManager = registry;
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 1);
        FunctionsSplitWorkerRemovalAction fallback =
            Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals);
        Assert.Equal(worker.Id, fallback.Worker.PodName);
        Assert.Equal("RuntimeUnlink", fallback.Reason);

        handler.AllowDrainFence.TrySetResult();
        using HttpResponseMessage accepted = await request.WaitAsync(TestTimeout);
        accepted.EnsureSuccessStatusCode();
        Assert.False(handler.RemovalRequestedObserved.Task.IsCompleted);

        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(fallback));
        if (string.Equals(mode, "Phased", StringComparison.Ordinal))
        {
            timeProvider.Advance(TimeSpan.FromSeconds(40));
        }

        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task PostRuntimeDrain_PreSelectionFunctionLoadFailureRetainsExactFallback()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("PreSelectionFunctionLoadFailure");
        ChannelControl worker = new(
            "worker",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false);
        harness.Enqueue(worker);
        await registry.LinkAsync(
            worker.Id,
            CreateEndpoint(worker.Id),
            CreateWorkerIdentity(worker.Id));
        await MakeWorkersReadyAsync(registry, function, worker);

        using AppVersionDrainHostStateHandler handler = new(blockFirstDrainFence: true);
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        Task<HttpResponseMessage> request = host.PostAsync(CreateDrainJson(
            "Immediate",
            "ConfigurationChange"));
        await handler.DrainFenceObserved.Task.WaitAsync(TestTimeout);

        await worker.FailFunctionLoadAsync();
        Assert.False(worker.Channel.Completion.IsCompleted);
        IHttpWorkerScaleInManager scaleInManager = registry;
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 1);
        FunctionsSplitWorkerRemovalAction fallback =
            Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals);
        Assert.Equal(worker.Id, fallback.Worker.PodName);
        Assert.Equal("RuntimeUnlink", fallback.Reason);

        handler.AllowDrainFence.TrySetResult();
        using HttpResponseMessage accepted = await request.WaitAsync(TestTimeout);
        accepted.EnsureSuccessStatusCode();
        Assert.False(handler.RemovalRequestedObserved.Task.IsCompleted);

        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(fallback));
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task PostRuntimeDrain_TriggerDiscoveryCompleteRequiresImmediateAndRemovesEmptyRuntime()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);

        using HttpResponseMessage phased = await host.PostAsync(CreateDrainJson(
            "Phased",
            "TriggerDiscoveryComplete"));
        Assert.Equal(HttpStatusCode.BadRequest, phased.StatusCode);

        using HttpResponseMessage accepted = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "TriggerDiscoveryComplete"));
        DrainReceipt receipt = await ReadDrainReceiptAsync(accepted);
        Assert.Equal("Immediate", receipt.Mode);
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);

        using HttpResponseMessage repeatedMode = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Stop"));
        Assert.Equal(receipt, await ReadDrainReceiptAsync(repeatedMode));
        Assert.Contains(handler.PublishedStates, state =>
            string.Equals(state.Status, AppServerHostStateClient.DrainingStatus, StringComparison.Ordinal));
        Assert.Contains(handler.PublishedStates, state =>
            string.Equals(state.Status, AppServerHostStateClient.RemovalRequestedStatus, StringComparison.Ordinal));
    }

    [Fact]
    public async Task PostRuntimeDrain_TriggerDiscoveryCompleteWaitsForWorkerFallbackAcknowledgement()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("DiscoveryFunction");
        ChannelControl discoveryWorker = new(
            "discovery-worker",
            functionGroupName: "trigger-discovery",
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false);
        harness.Enqueue(discoveryWorker);
        await registry.LinkAsync(
            discoveryWorker.Id,
            CreateEndpoint(discoveryWorker.Id),
            CreateWorkerIdentity(discoveryWorker.Id));
        await MakeWorkersReadyAsync(registry, function, discoveryWorker);

        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage accepted = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "TriggerDiscoveryComplete"));
        accepted.EnsureSuccessStatusCode();

        IHttpWorkerScaleInManager scaleInManager = registry;
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 1);
        FunctionsSplitWorkerRemovalAction fallback =
            Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals);
        Assert.Equal(discoveryWorker.Id, fallback.Worker.PodName);
        Assert.False(handler.RemovalRequestedObserved.Task.IsCompleted);

        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(fallback));
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);
        Assert.Equal(1, discoveryWorker.DisposeCount);
    }

    [Fact]
    public async Task PostRuntimeDrain_PhasedSelectsDeterministicBatchesAtOriginalOffsetsAndHttpAtThirtyFiveSeconds()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("MixedFunction");
        ChannelControl[] nonHttpWorkers = Enumerable.Range(0, 10)
            .Select(index => new ChannelControl(
                $"non-http-{index:D2}",
                functionGroupName: FunctionGroups.Durable,
                isAlwaysReady: index % 2 == 0))
            .ToArray();
        ChannelControl[] httpWorkers = Enumerable.Range(0, 2)
            .Select(index => new ChannelControl(
                $"http-{index:D2}",
                functionGroupName: FunctionGroups.Http,
                isAlwaysReady: index % 2 == 0))
            .ToArray();
        ChannelControl[] workers = [.. nonHttpWorkers, .. httpWorkers];
        foreach (ChannelControl worker in workers)
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, workers);
        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage accepted = await host.PostAsync(CreateDrainJson(
            "Phased",
            "ConfigurationChange"));
        DrainReceipt receipt = await ReadDrainReceiptAsync(accepted);

        await WaitUntilAsync(() => nonHttpWorkers.Take(2).All(worker => worker.DisposeCount == 1));
        Assert.Equal(2, nonHttpWorkers.Count(worker => worker.DisposeCount == 1));
        Assert.All(httpWorkers, worker => Assert.Equal(0, worker.DisposeCount));

        timeProvider.Advance(TimeSpan.FromSeconds(9));
        using HttpResponseMessage repeatedMode = await host.PostAsync(CreateDrainJson(
            "Phased",
            "Restart"));
        Assert.Equal(receipt, await ReadDrainReceiptAsync(repeatedMode));
        Assert.Equal(2, nonHttpWorkers.Count(worker => worker.DisposeCount == 1));

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(() => nonHttpWorkers.Count(worker => worker.DisposeCount == 1) == 4);
        timeProvider.Advance(TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => nonHttpWorkers.Count(worker => worker.DisposeCount == 1) == 6);
        timeProvider.Advance(TimeSpan.FromSeconds(10));
        await WaitUntilAsync(() => nonHttpWorkers.Count(worker => worker.DisposeCount == 1) == 8);

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => httpWorkers.All(worker => worker.DisposeCount == 1));
        Assert.Equal(8, nonHttpWorkers.Count(worker => worker.DisposeCount == 1));

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => nonHttpWorkers.All(worker => worker.DisposeCount == 1));
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task PostRuntimeDrain_PhasedDeadlinePreservesPreExistingDrainAndWaitsForBothFallbackHandoffs()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("DrainFunction");
        ChannelControl preExisting = new(
            "pre-existing",
            functionGroupName: "pre-existing-group",
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false);
        ChannelControl phased = new(
            "phased",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false);
        foreach (ChannelControl worker in new[] { preExisting, phased })
        {
            harness.Enqueue(worker);
            await registry.LinkAsync(
                worker.Id,
                CreateEndpoint(worker.Id),
                CreateWorkerIdentity(worker.Id));
        }

        await MakeWorkersReadyAsync(registry, function, preExisting, phased);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);
        Assert.True(phased.Channel.TryDispatchInvocation(invocation));
        await phased.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);

        WorkerCountScaleInResult preExistingResult =
            ((IWorkerCountScaleInManager)registry).ScaleInWorkers("pre-existing-group", false, 0);
        Assert.Equal(1, preExistingResult.NewlyDrainingWorkerCount);

        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage accepted = await host.PostAsync(CreateDrainJson(
            "Phased",
            "ConfigurationChange"));
        accepted.EnsureSuccessStatusCode();

        IHttpWorkerScaleInManager scaleInManager = registry;
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 1);
        FunctionsSplitWorkerRemovalAction preExistingFallback =
            Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals);
        Assert.Equal(preExisting.Id, preExistingFallback.Worker.PodName);

        timeProvider.Advance(TimeSpan.FromMinutes(59));
        Assert.Equal(1, scaleInManager.Current.OutstandingWorkerRemovals.Count);
        Assert.Contains(phased.Channel, registry.GetInitializedChannels());

        timeProvider.Advance(TimeSpan.FromMinutes(1));
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 2);
        FunctionsSplitWorkerRemovalAction phasedFallback = scaleInManager.Current.OutstandingWorkerRemovals
            .Single(action => string.Equals(action.Worker.PodName, phased.Id, StringComparison.Ordinal));

        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(preExistingFallback));
        Assert.False(handler.RemovalRequestedObserved.Task.IsCompleted);
        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(phasedFallback));
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task PostRuntimeDrain_AdmissionFailureKeepsPublishingExistingDrainFallback()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("ExistingDrainFunction");
        ChannelControl worker = new(
            "existing-drain",
            functionGroupName: "existing-drain-group",
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false,
            workerRemovalHandoffConfirmed: false);
        harness.Enqueue(worker);
        await registry.LinkAsync(
            worker.Id,
            CreateEndpoint(worker.Id),
            CreateWorkerIdentity(worker.Id));
        await MakeWorkersReadyAsync(registry, function, worker);

        using AppVersionDrainHostStateHandler handler = new(
            drainFenceStatus: HttpStatusCode.Conflict);
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        WorkerCountScaleInResult scaleIn =
            ((IWorkerCountScaleInManager)registry).ScaleInWorkers(
                "existing-drain-group",
                false,
                0);
        Assert.Equal(1, scaleIn.NewlyDrainingWorkerCount);
        await worker.Transport.Requests.Completion.WaitAsync(TestTimeout);

        using HttpResponseMessage rejected = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Stop"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, rejected.StatusCode);

        worker.Complete();
        await WaitUntilAsync(() => handler.PublishedStates.Any(state =>
            state.Status is null &&
            state.OutstandingWorkerRemovals.Any(action =>
                string.Equals(action.Worker.PodName, worker.Id, StringComparison.Ordinal))));
        FunctionsSplitWorkerRemovalAction fallback = Assert.Single(
            ((IHttpWorkerScaleInManager)registry).Current.OutstandingWorkerRemovals);
        Assert.Equal("RuntimeUnlink", fallback.Reason);
        AppServerHostStateClient.HostStateRequest failedFence = Assert.Single(
            handler.PublishedStates,
            state => string.Equals(
                state.Status,
                AppServerHostStateClient.DrainingStatus,
                StringComparison.Ordinal));
        AppServerHostStateClient.HostStateRequest publishedFallback = handler.PublishedStates.Last(state =>
            state.Status is null &&
            state.OutstandingWorkerRemovals.Any(action =>
                string.Equals(action.Worker.PodName, worker.Id, StringComparison.Ordinal)));
        Assert.True(publishedFallback.CreatedTime > failedFence.CreatedTime);
    }

    [Fact]
    public async Task PostRuntimeDrain_ImmediateSelectsAllClassesReturnsExistingReceiptAndRejectsDowngrade()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("ImmediateFunction");
        ChannelControl[] workers =
        [
            new("http-on-demand", functionGroupName: FunctionGroups.Http, isAlwaysReady: false),
            new("http-always-ready", functionGroupName: FunctionGroups.Http, isAlwaysReady: true),
            new("durable-on-demand", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false),
            new("other-always-ready", functionGroupName: "other", isAlwaysReady: true),
        ];
        foreach (ChannelControl worker in workers)
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, workers);
        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage accepted = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Delete"));
        DrainReceipt receipt = await ReadDrainReceiptAsync(accepted);

        await WaitUntilAsync(() => workers.All(worker => worker.DisposeCount == 1));
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);

        using HttpResponseMessage repeatedMode = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Restart"));
        Assert.Equal(receipt, await ReadDrainReceiptAsync(repeatedMode));

        using HttpResponseMessage downgrade = await host.PostAsync(CreateDrainJson(
            "Phased",
            "Delete"));
        Assert.Equal(HttpStatusCode.Conflict, downgrade.StatusCode);
    }

    [Fact]
    public async Task PostRuntimeDrain_PhasedToImmediateEscalationPreservesExistingDeadlineAndUsesEscalationDeadlineForNewWorkers()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("EscalationFunction");
        ChannelControl oldDeadline = new(
            "a-old-deadline",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false);
        ChannelControl newDeadline = new(
            "b-new-deadline",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false);
        ChannelControl[] idleWorkers =
        [
            new("c-idle", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false),
            new("d-idle", functionGroupName: FunctionGroups.Durable, isAlwaysReady: true),
            new("e-idle", functionGroupName: "other", isAlwaysReady: false),
        ];
        ChannelControl[] workers = [oldDeadline, newDeadline, .. idleWorkers];
        foreach (ChannelControl worker in workers)
        {
            harness.Enqueue(worker);
            FunctionsSplitPodIdentity identity = CreateWorkerIdentity(worker.Id);
            await registry.LinkAsync(worker.Id, CreateEndpoint(worker.Id), identity);
        }

        await MakeWorkersReadyAsync(registry, function, workers);
        foreach (ChannelControl worker in new[] { oldDeadline, newDeadline })
        {
            ScriptInvocationContext invocation = CreateScaleInInvocation(function);
            Assert.True(worker.Channel.TryDispatchInvocation(invocation));
            await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);
        }

        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage phasedAccepted = await host.PostAsync(CreateDrainJson(
            "Phased",
            "ConfigurationChange"));
        DrainReceipt phasedReceipt = await ReadDrainReceiptAsync(phasedAccepted);

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        using HttpResponseMessage immediateAccepted = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Restart"));
        DrainReceipt immediateReceipt = await ReadDrainReceiptAsync(immediateAccepted);
        Assert.Equal(phasedReceipt.AcceptedAtUtc.AddSeconds(5), immediateReceipt.AcceptedAtUtc);
        await WaitUntilAsync(() => idleWorkers.All(worker => worker.DisposeCount == 1));

        using HttpResponseMessage repeatedImmediate = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Delete"));
        Assert.Equal(immediateReceipt, await ReadDrainReceiptAsync(repeatedImmediate));
        using HttpResponseMessage downgrade = await host.PostAsync(CreateDrainJson(
            "Phased",
            "ConfigurationChange"));
        Assert.Equal(HttpStatusCode.Conflict, downgrade.StatusCode);

        IHttpWorkerScaleInManager scaleInManager = registry;
        timeProvider.Advance(TimeSpan.FromMinutes(59) + TimeSpan.FromSeconds(55));
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 1);
        FunctionsSplitWorkerRemovalAction oldFallback =
            Assert.Single(scaleInManager.Current.OutstandingWorkerRemovals);
        Assert.Equal(oldDeadline.Id, oldFallback.Worker.PodName);
        Assert.Contains(newDeadline.Channel, registry.GetInitializedChannels());

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => scaleInManager.Current.OutstandingWorkerRemovals.Count == 2);
        FunctionsSplitWorkerRemovalAction newFallback = scaleInManager.Current.OutstandingWorkerRemovals
            .Single(action => string.Equals(action.Worker.PodName, newDeadline.Id, StringComparison.Ordinal));
        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(oldFallback));
        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            scaleInManager.AcknowledgeWorkerRemoval(newFallback));
        await handler.RemovalRequestedObserved.Task.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task PostRuntimeDrain_LifecycleFailureRejectsRepeatedModeAndEscalation()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("LifecycleFailureFunction");
        ChannelControl worker = new(
            "worker",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: false);
        harness.Enqueue(worker);
        await registry.LinkAsync(
            worker.Id,
            CreateEndpoint(worker.Id),
            CreateWorkerIdentity(worker.Id));
        await MakeWorkersReadyAsync(registry, function, worker);
        Assert.True(worker.Channel.TryDispatchInvocation(CreateScaleInInvocation(function)));
        await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);

        using AppVersionDrainHostStateHandler handler = new(failDrainProgress: true);
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage phased = await host.PostAsync(CreateDrainJson(
            "Phased",
            "ConfigurationChange"));
        phased.EnsureSuccessStatusCode();
        await handler.DrainProgressFailureObserved.Task.WaitAsync(TestTimeout);
        timeProvider.Advance(TimeSpan.FromMinutes(60));
        await handler.FallbackRequestObserved.Task.WaitAsync(TestTimeout);
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        using HttpResponseMessage repeatedMode = await host.PostAsync(CreateDrainJson(
            "Phased",
            "Restart"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, repeatedMode.StatusCode);
        Assert.DoesNotContain(
            "acceptedAtUtc",
            await repeatedMode.Content.ReadAsStringAsync(),
            StringComparison.Ordinal);

        using HttpResponseMessage escalation = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Restart"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, escalation.StatusCode);
        Assert.DoesNotContain(
            "acceptedAtUtc",
            await escalation.Content.ReadAsStringAsync(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task PostRuntimeDrain_SelectedWorkerFailureRejectsRetryAndEscalationBeforeScheduleCompletes()
    {
        DrainManualTimeProvider timeProvider = new();
        RegistryHarness harness = new(timeProvider, AppVersionUnlinkTimeout);
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("WorkerFailureFunction");
        ChannelControl failed = new(
            "a-failed",
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false,
            workerRemovalHandoffConfirmed: false);
        ChannelControl[] pending = Enumerable.Range(0, 4)
            .Select(index => new ChannelControl(
                $"b-pending-{index}",
                functionGroupName: FunctionGroups.Durable,
                isAlwaysReady: false))
            .ToArray();
        ChannelControl[] workers = [failed, .. pending];
        foreach (ChannelControl worker in workers)
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, workers);
        using AppVersionDrainHostStateHandler handler = new();
        await using AppVersionDrainApiHost host = await AppVersionDrainApiHost.StartAsync(
            registry,
            handler,
            timeProvider);
        using HttpResponseMessage accepted = await host.PostAsync(CreateDrainJson(
            "Phased",
            "ConfigurationChange"));
        accepted.EnsureSuccessStatusCode();
        await failed.Channel.Completion.WaitAsync(TestTimeout);

        HttpResponseMessage repeatedMode;
        using (CancellationTokenSource timeout = new(TestTimeout))
        {
            while (true)
            {
                repeatedMode = await host.PostAsync(CreateDrainJson(
                    "Phased",
                    "Restart"));
                if (repeatedMode.StatusCode is HttpStatusCode.ServiceUnavailable)
                {
                    break;
                }

                repeatedMode.Dispose();
                await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
            }
        }

        using (repeatedMode)
        {
            Assert.DoesNotContain(
                "acceptedAtUtc",
                await repeatedMode.Content.ReadAsStringAsync(),
                StringComparison.Ordinal);
        }

        using HttpResponseMessage escalation = await host.PostAsync(CreateDrainJson(
            "Immediate",
            "Restart"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, escalation.StatusCode);
        Assert.DoesNotContain(
            "acceptedAtUtc",
            await escalation.Content.ReadAsStringAsync(),
            StringComparison.Ordinal);
        Assert.All(pending, worker => Assert.Equal(0, worker.DisposeCount));
    }

    [Fact]
    public async Task RuntimeDrainScheduleFailureKeepsPublishingLateFallback()
    {
        DrainManualTimeProvider timeProvider = new();
        SelectionFailureWorkerManager workerManager = new(timeProvider.GetUtcNow());
        using AppVersionDrainHostStateHandler handler = new();
        using HttpClient httpClient = new(handler);
        Mock<IHttpClientFactory> factory = new();
        factory.Setup(value => value.CreateClient(AppServerHostStateClient.HttpClientName))
            .Returns(httpClient);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                [EnvironmentSettingNames.ContainerName] = "test-container",
                [EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
            })
            .Build();
        AppServerHostStateClient client = new(
            factory.Object,
            workerManager,
            workerManager,
            timeProvider,
            configuration);
        using RuntimeRetirementStatePublisher publisher = new(
            workerManager,
            client,
            timeProvider,
            NullLogger<RuntimeRetirementStatePublisher>.Instance);
        AppVersionRuntimeDrainManager manager = new(
            workerManager,
            workerManager,
            client,
            publisher,
            timeProvider,
            NullLogger<AppVersionRuntimeDrainManager>.Instance);

        await manager.AcceptAsync(
            RuntimeDrainMode.Phased,
            RuntimeDrainReason.ConfigurationChange);
        timeProvider.Advance(TimeSpan.FromSeconds(10));
        await workerManager.SelectionFailureTriggered.Task.WaitAsync(TestTimeout);

        using CancellationTokenSource timeout = new(TestTimeout);
        while (true)
        {
            try
            {
                await manager.AcceptAsync(
                    RuntimeDrainMode.Phased,
                    RuntimeDrainReason.Restart);
            }
            catch (InvalidOperationException)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
        }

        workerManager.PublishFallback();
        await handler.FallbackRequestObserved.Task.WaitAsync(TestTimeout);

        AppServerHostStateClient.HostStateRequest fallback = handler.PublishedStates.Last(state =>
            state.OutstandingWorkerRemovals.Count != 0);
        Assert.Equal(AppServerHostStateClient.DrainingStatus, fallback.Status);
        Assert.Equal(workerManager.FallbackAction, Assert.Single(fallback.OutstandingWorkerRemovals));
    }

    [Theory]
    [InlineData(RuntimeDrainMode.Phased, "a-worker", false)]
    [InlineData(RuntimeDrainMode.Immediate, null, true)]
    public async Task RuntimeDrainInitialSelectionFailureKeepsPublishingLateFallback(
        RuntimeDrainMode mode,
        string failingWorkerId,
        bool failAllRemainingSelection)
    {
        DrainManualTimeProvider timeProvider = new();
        SelectionFailureWorkerManager workerManager = new(
            timeProvider.GetUtcNow(),
            failingWorkerId,
            failAllRemainingSelection);
        using AppVersionDrainHostStateHandler handler = new();
        using HttpClient httpClient = new(handler);
        Mock<IHttpClientFactory> factory = new();
        factory.Setup(value => value.CreateClient(AppServerHostStateClient.HttpClientName))
            .Returns(httpClient);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                [EnvironmentSettingNames.ContainerName] = "test-container",
                [EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
            })
            .Build();
        AppServerHostStateClient client = new(
            factory.Object,
            workerManager,
            workerManager,
            timeProvider,
            configuration);
        using RuntimeRetirementStatePublisher publisher = new(
            workerManager,
            client,
            timeProvider,
            NullLogger<RuntimeRetirementStatePublisher>.Instance);
        AppVersionRuntimeDrainManager manager = new(
            workerManager,
            workerManager,
            client,
            publisher,
            timeProvider,
            NullLogger<AppVersionRuntimeDrainManager>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => manager.AcceptAsync(
            mode,
            RuntimeDrainReason.ConfigurationChange));
        await workerManager.SelectionFailureTriggered.Task.WaitAsync(TestTimeout);

        workerManager.PublishFallback();
        await handler.FallbackRequestObserved.Task.WaitAsync(TestTimeout);

        AppServerHostStateClient.HostStateRequest fallback = handler.PublishedStates.Last(state =>
            state.OutstandingWorkerRemovals.Count != 0);
        Assert.Equal(AppServerHostStateClient.DrainingStatus, fallback.Status);
        Assert.Equal(workerManager.FallbackAction, Assert.Single(fallback.OutstandingWorkerRemovals));
        await Assert.ThrowsAsync<RuntimeDrainAdmissionException>(() => manager.AcceptAsync(
            mode,
            RuntimeDrainReason.Restart));
    }

    [Fact]
    public async Task RuntimeDrainEscalationSelectionFailureKeepsPublishingLateFallback()
    {
        DrainManualTimeProvider timeProvider = new();
        SelectionFailureWorkerManager workerManager = new(
            timeProvider.GetUtcNow(),
            failingWorkerId: null,
            failAllRemainingSelection: true);
        using AppVersionDrainHostStateHandler handler = new();
        using HttpClient httpClient = new(handler);
        Mock<IHttpClientFactory> factory = new();
        factory.Setup(value => value.CreateClient(AppServerHostStateClient.HttpClientName))
            .Returns(httpClient);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                [EnvironmentSettingNames.ContainerName] = "test-container",
                [EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
            })
            .Build();
        AppServerHostStateClient client = new(
            factory.Object,
            workerManager,
            workerManager,
            timeProvider,
            configuration);
        using RuntimeRetirementStatePublisher publisher = new(
            workerManager,
            client,
            timeProvider,
            NullLogger<RuntimeRetirementStatePublisher>.Instance);
        AppVersionRuntimeDrainManager manager = new(
            workerManager,
            workerManager,
            client,
            publisher,
            timeProvider,
            NullLogger<AppVersionRuntimeDrainManager>.Instance);

        await manager.AcceptAsync(
            RuntimeDrainMode.Phased,
            RuntimeDrainReason.ConfigurationChange);
        await Assert.ThrowsAsync<InvalidOperationException>(() => manager.AcceptAsync(
            RuntimeDrainMode.Immediate,
            RuntimeDrainReason.Restart));
        await workerManager.SelectionFailureTriggered.Task.WaitAsync(TestTimeout);

        workerManager.PublishFallback();
        await handler.FallbackRequestObserved.Task.WaitAsync(TestTimeout);

        AppServerHostStateClient.HostStateRequest fallback = handler.PublishedStates.Last(state =>
            state.OutstandingWorkerRemovals.Count != 0);
        Assert.Equal(AppServerHostStateClient.DrainingStatus, fallback.Status);
        Assert.Equal(workerManager.FallbackAction, Assert.Single(fallback.OutstandingWorkerRemovals));
        await Assert.ThrowsAsync<RuntimeDrainAdmissionException>(() => manager.AcceptAsync(
            RuntimeDrainMode.Immediate,
            RuntimeDrainReason.Stop));
    }

    private static string CreateDrainJson(string mode, string reason)
        => $$"""{"mode":"{{mode}}","reason":"{{reason}}"}""";

    private static async Task<DrainReceipt> ReadDrainReceiptAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        using JsonDocument body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(2, body.RootElement.EnumerateObject().Count());
        return new(
            body.RootElement.GetProperty("mode").GetString(),
            body.RootElement.GetProperty("acceptedAtUtc").GetDateTimeOffset());
    }

    private sealed record DrainReceipt(
        string Mode,
        DateTimeOffset AcceptedAtUtc);

    private sealed class SelectionFailureWorkerManager :
        IAppVersionRuntimeDrainWorkerManager,
        IHttpWorkerScaleInManager,
        IRuntimeRetirementWorkerManager
    {
        private readonly bool _failAllRemainingSelection;
        private readonly string _failingWorkerId;
        private readonly Lock _stateLock = new();
        private TaskCompletionSource<long> _stateChanged =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private HttpWorkerScaleInState _state;

        internal SelectionFailureWorkerManager(
            DateTimeOffset createdTime,
            string failingWorkerId = "b-worker",
            bool failAllRemainingSelection = false)
        {
            _failingWorkerId = failingWorkerId;
            _failAllRemainingSelection = failAllRemainingSelection;
            _state = new(createdTime, 0, 5, 0);
        }

        public FunctionsSplitWorkerRemovalAction FallbackAction { get; } = new(
            new("test-namespace", "a-worker"),
            "RuntimeUnlink");

        public RuntimeIncarnation Incarnation { get; } = RuntimeIncarnation.Create();

        public bool IsRetirementRequested { get; private set; }

        public TaskCompletionSource SelectionFailureTriggered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public HttpWorkerScaleInState Current
        {
            get
            {
                lock (_stateLock)
                {
                    return _state;
                }
            }
        }

        public long Version => Current.Version;

        public WorkerRemovalAcknowledgementResult AcknowledgeWorkerRemoval(
            FunctionsSplitWorkerRemovalAction action)
            => WorkerRemovalAcknowledgementResult.NotFound;

        public bool CancelReservation(HttpWorkerScaleInReservation reservation) => false;

        public Task<RuntimeRetirementDrainResult> DrainAllWorkersAsync(
            RuntimeIncarnation incarnation,
            CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public void PublishFallback()
        {
            lock (_stateLock)
            {
                _state = new(
                    _state.CreatedTime.AddTicks(1),
                    0,
                    0,
                    _state.Version + 1)
                {
                    OutstandingWorkerRemovals = [FallbackAction],
                };
                TaskCompletionSource<long> changed = _stateChanged;
                _stateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
                changed.TrySetResult(_state.Version);
            }
        }

        public AppVersionRuntimeDrainSelection SelectAllRemainingWorkers(
            RuntimeIncarnation incarnation,
            DateTimeOffset invocationGraceDeadline)
        {
            Assert.Equal(Incarnation, incarnation);
            if (_failAllRemainingSelection)
            {
                SelectionFailureTriggered.TrySetResult();
                throw new InvalidOperationException("all remaining selection failed");
            }

            return new([]);
        }

        public AppVersionRuntimeDrainSelection SelectWorkers(
            RuntimeIncarnation incarnation,
            IReadOnlyList<string> workerIds,
            DateTimeOffset invocationGraceDeadline)
        {
            Assert.Equal(Incarnation, incarnation);
            if (_failingWorkerId is not null &&
                workerIds.Contains(_failingWorkerId, StringComparer.Ordinal))
            {
                SelectionFailureTriggered.TrySetResult();
                throw new InvalidOperationException("worker selection failed");
            }

            return new([]);
        }

        public bool TryCloseAdmission(SplitScriptHostActivationFaulted condition) => false;

        public bool TryCommitReservation(
            HttpWorkerScaleInReservation reservation,
            out Task<bool> drainCompletion)
        {
            drainCompletion = null;
            return false;
        }

        public HttpWorkerScaleInReservation TryReserveIdleWorker(
            TimeSpan minimumIdleDuration)
            => null;

        public bool TryStartAppVersionDrain(out AppVersionRuntimeDrainStart start)
        {
            IsRetirementRequested = true;
            start = new(
                Incarnation,
                ["a-worker", "b-worker", "c-worker", "d-worker", "e-worker"],
                [],
                [],
                []);
            return true;
        }

        public Task<long> WaitForChangeAsync(
            long version,
            CancellationToken cancellationToken = default)
        {
            lock (_stateLock)
            {
                return version != _state.Version
                    ? Task.FromResult(_state.Version)
                    : _stateChanged.Task.WaitAsync(cancellationToken);
            }
        }
    }

    private sealed class AppVersionDrainApiHost : IAsyncDisposable
    {
        private readonly HttpClient _appServerClient;
        private readonly IHost _host;

        private AppVersionDrainApiHost(IHost host, HttpClient appServerClient)
        {
            _host = host;
            _appServerClient = appServerClient;
            Client = host.GetTestClient();
        }

        internal HttpClient Client { get; }

        internal static async Task<AppVersionDrainApiHost> StartAsync(
            WorkerChannelRegistry registry,
            AppVersionDrainHostStateHandler handler,
            TimeProvider timeProvider)
        {
            HttpClient appServerClient = new(handler, disposeHandler: false);
            Mock<IHttpClientFactory> factory = new();
            factory.Setup(value => value.CreateClient(AppServerHostStateClient.HttpClientName))
                .Returns(appServerClient);
            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    [EnvironmentSettingNames.ContainerName] = "test-container",
                    [EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
                })
                .Build();
            IHost host = new HostBuilder()
                .ConfigureWebHost(webBuilder =>
                {
                    webBuilder
                        .UseSetting(WebHostDefaults.ApplicationKey, typeof(Startup).Assembly.GetName().Name)
                        .UseTestServer()
                        .ConfigureLogging(logging => logging.ClearProviders())
                        .ConfigureServices(services =>
                        {
                            IMvcBuilder mvcBuilder = services.AddMvc(options =>
                            {
                                options.EnableEndpointRouting = false;
                            }).AddNewtonsoftJson();
                            mvcBuilder.ConfigureApplicationPartManager(parts =>
                            {
                                parts.ApplicationParts.Clear();
                                parts.ApplicationParts.Add(new AssemblyPart(typeof(RuntimeDrainController).Assembly));
                            });

                            services.AddSingleton<IWorkerChannelRegistry>(registry);
                            services.AddSingleton<IWorkerCountScaleInManager>(registry);
                            services.AddSingleton<IHttpWorkerScaleInManager>(registry);
                            services.AddSingleton<IRuntimeRetirementWorkerManager>(registry);
                            services.AddSingleton<IAppVersionRuntimeDrainWorkerManager>(registry);
                            services.AddSingleton(factory.Object);
                            services.AddSingleton(configuration);
                            services.AddSingleton<TimeProvider>(timeProvider);
                            services.AddSingleton<AppServerHostStateClient>();
                            services.AddSingleton<RuntimeRetirementStatePublisher>();
                            services.AddSingleton<IHostedService>(
                                provider => provider.GetRequiredService<RuntimeRetirementStatePublisher>());
                            services.AddSingleton<AppVersionRuntimeDrainManager>();
                            services.AddSingleton<IAppVersionRuntimeDrainManager>(
                                provider => provider.GetRequiredService<AppVersionRuntimeDrainManager>());
                            services.AddAuthentication();
                            services.AddAuthorization(options =>
                            {
                                options.AddScriptPolicies();
                                options.AddPolicy(
                                    PolicyNames.AdminAuthLevel,
                                    policy => policy.RequireAssertion(_ => true));
                            });
                        })
                        .Configure(app => app.UseMvc());
                })
                .Build();

            try
            {
                await host.StartAsync();
                return new AppVersionDrainApiHost(host, appServerClient);
            }
            catch
            {
                appServerClient.Dispose();
                await DisposeHostAsync(host);
                throw;
            }
        }

        internal async Task<HttpResponseMessage> PostAsync(string json)
        {
            using HttpRequestMessage request = new(HttpMethod.Post, "/admin/functions/drain");
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
            return await Client.SendAsync(request);
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            using CancellationTokenSource timeout = new(TestTimeout);
            try
            {
                await _host.StopAsync(timeout.Token);
            }
            finally
            {
                _appServerClient.Dispose();
                await DisposeHostAsync(_host);
            }
        }

        private static async Task DisposeHostAsync(IHost host)
        {
            if (host is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync().AsTask().WaitAsync(TestTimeout);
            }
            else
            {
                host.Dispose();
            }
        }
    }

    private sealed class AppVersionDrainHostStateHandler(
        bool blockFirstDrainFence = false,
        HttpStatusCode drainFenceStatus = HttpStatusCode.OK,
        bool failDrainProgress = false) : HttpMessageHandler
    {
        private bool _drainFenceBlocked;
        private bool _drainProgressFailed;
        private int _drainingPublishCount;

        internal TaskCompletionSource AllowDrainFence { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource DrainFenceObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource DrainProgressFailureObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource FallbackRequestObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal List<AppServerHostStateClient.HostStateRequest> PublishedStates { get; } = [];

        internal TaskCompletionSource RemovalRequestedObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            string json = await request.Content!.ReadAsStringAsync(cancellationToken);
            AppServerHostStateClient.HostStateRequest state =
                JsonSerializer.Deserialize(json, AppServerHostStateJsonContext.Default.HostStateRequest)!;
            PublishedStates.Add(state);

            if (string.Equals(state.Status, AppServerHostStateClient.DrainingStatus, StringComparison.Ordinal))
            {
                _drainingPublishCount++;
                DrainFenceObserved.TrySetResult();
                if (blockFirstDrainFence && !_drainFenceBlocked)
                {
                    _drainFenceBlocked = true;
                    await AllowDrainFence.Task.WaitAsync(cancellationToken);
                }

                if (failDrainProgress &&
                    _drainingPublishCount > 1 &&
                    !_drainProgressFailed)
                {
                    _drainProgressFailed = true;
                    DrainProgressFailureObserved.TrySetResult();
                    return new(HttpStatusCode.Conflict);
                }

                if (state.OutstandingWorkerRemovals.Count != 0)
                {
                    FallbackRequestObserved.TrySetResult();
                }

                if (drainFenceStatus != HttpStatusCode.OK)
                {
                    return new(drainFenceStatus);
                }
            }

            if (string.Equals(
                state.Status,
                AppServerHostStateClient.RemovalRequestedStatus,
                StringComparison.Ordinal))
            {
                RemovalRequestedObserved.TrySetResult();
            }

            HttpResponseMessage response = new(HttpStatusCode.OK);
            response.Headers.Add(
                AppServerHostStateClient.CapacityFenceHeader,
                AppServerHostStateClient.CapacityFenceApplied);
            return response;
        }
    }

    private sealed class DrainManualTimeProvider : TimeProvider
    {
        private readonly Lock _lock = new();
        private readonly List<ManualTimer> _timers = [];
        private long _timestamp;
        private DateTimeOffset _utcNow = new(2026, 10, 30, 0, 0, 0, TimeSpan.Zero);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
        {
            lock (_lock)
            {
                return _utcNow;
            }
        }

        public override long GetTimestamp()
        {
            lock (_lock)
            {
                return _timestamp;
            }
        }

        public override ITimer CreateTimer(
            TimerCallback callback,
            object state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            ArgumentNullException.ThrowIfNull(callback);
            ManualTimer timer = new(this, callback, state, dueTime, period);
            lock (_lock)
            {
                _timers.Add(timer);
            }

            return timer;
        }

        internal void Advance(TimeSpan duration)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);
            lock (_lock)
            {
                _utcNow += duration;
                _timestamp += duration.Ticks;
            }

            while (true)
            {
                ManualTimer[] due;
                lock (_lock)
                {
                    due = [.. _timers.Where(timer => timer.IsDue(_timestamp))];
                }

                if (due.Length == 0)
                {
                    return;
                }

                foreach (ManualTimer timer in due)
                {
                    timer.Fire();
                }
            }
        }

        private sealed class ManualTimer : ITimer
        {
            private readonly TimerCallback _callback;
            private readonly DrainManualTimeProvider _owner;
            private readonly object _state;
            private bool _disposed;
            private long _dueTimestamp;
            private TimeSpan _period;

            public ManualTimer(
                DrainManualTimeProvider owner,
                TimerCallback callback,
                object state,
                TimeSpan dueTime,
                TimeSpan period)
            {
                _owner = owner;
                _callback = callback;
                _state = state;
                Change(dueTime, period);
            }

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (_owner._lock)
                {
                    if (_disposed)
                    {
                        return false;
                    }

                    _dueTimestamp = dueTime == Timeout.InfiniteTimeSpan
                        ? long.MaxValue
                        : _owner._timestamp + Math.Max(0, dueTime.Ticks);
                    _period = period;
                    return true;
                }
            }

            public void Dispose()
            {
                lock (_owner._lock)
                {
                    _disposed = true;
                    _owner._timers.Remove(this);
                }
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }

            internal void Fire()
            {
                lock (_owner._lock)
                {
                    if (_disposed || _dueTimestamp > _owner._timestamp)
                    {
                        return;
                    }

                    _dueTimestamp = _period == Timeout.InfiniteTimeSpan
                        ? long.MaxValue
                        : _owner._timestamp + Math.Max(0, _period.Ticks);
                }

                _callback(_state);
            }

            internal bool IsDue(long timestamp)
            {
                lock (_owner._lock)
                {
                    return !_disposed && _dueTimestamp <= timestamp;
                }
            }
        }
    }
}
