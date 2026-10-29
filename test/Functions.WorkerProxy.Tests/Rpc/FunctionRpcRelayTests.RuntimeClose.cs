// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.Rpc;
using Azure.Functions.WorkerProxy.State;
using Grpc.Core;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public async Task PeerClose_InitializedAttachment_PublishesStickyRemovalAndClosesIntake(bool assigned, bool runtimeSide)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);
        if (assigned)
        {
            using HttpResponseMessage assignment = await PutManagementJsonAsync(
                management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
            Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
        }

        SplitWorkerOriginState accepted = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(accepted, await ReadSplitManagementStateAsync(management, timeout.Token));
        Task<HttpResponseMessage> poll = PollSplitManagementStateAsync(management, accepted, timeout.Token);
        await WaitForManagementPollAsync(factory.Services.GetRequiredService<WorkerPodStateManager>(), timeout.Token);

        await (runtimeSide ? runtime : worker).CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        Assert.Equal(StatusCode.Unavailable, await worker.WaitForTerminationAsync(timeout.Token));
        using HttpResponseMessage changed = await poll;
        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(changed, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(accepted.RevisionId + 1, removed.RevisionId);
        Assert.Equal(accepted.StartupTime, removed.StartupTime);
        Assert.Equal(accepted.Link, removed.Link);
        await AssertReadinessAsync(management, "/admin/instance/ready", HttpStatusCode.OK, timeout.Token);
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.ServiceUnavailable, timeout.Token);
        using HttpResponseMessage lateAssignment = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
        await AssertManagementErrorAsync(lateAssignment, HttpStatusCode.Conflict, "RemovalRequested", timeout.Token);
        await using RelayClient replacement = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        GrpcRpcException rejected = await replacement.WriteAndReadRejectionAsync(
            new StreamingMessage { WorkerInitRequest = new() { HostVersion = "replacement" } }, timeout.Token);
        Assert.Equal(StatusCode.Unavailable, rejected.StatusCode);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(removed, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public async Task PeerClose_BeforeInitialization_AllowsRetryWithoutResettingNeverLinkedDeadline(
        bool failedInitialization, bool runtimeSide)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(4));
        await using (RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token))
        await using (RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token))
        {
            if (failedInitialization)
            {
                await worker.WriteAsync(CreateStartStream(), timeout.Token);
                await runtime.ReadAsync(timeout.Token);
                await runtime.WriteAsync(new StreamingMessage
                {
                    WorkerInitRequest = new() { HostVersion = "test-host" }
                }, timeout.Token);
                await worker.ReadAsync(timeout.Token);
                await worker.WriteAsync(new StreamingMessage
                {
                    WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Failure } }
                }, timeout.Token);
                await runtime.ReadAsync(timeout.Token);
            }
            else
            {
                await ExchangeAsync(runtime, worker, "not-initialized", timeout.Token);
            }

            await (runtimeSide ? runtime : worker).CompleteRequestAsync(timeout.Token);
            await runtime.WaitForTerminationAsync(timeout.Token);
            await worker.WaitForTerminationAsync(timeout.Token);
        }

        await WaitForReleaseAsync(relay, timeout.Token);
        Assert.Equal(SplitWorkerStatus.Starting, (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await ExchangeAsync(replacementRuntime, replacementWorker, "retry-before-initialization", timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(1));
        SplitWorkerOriginState expired = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, expired.Status);
        Assert.Equal(initial.StartupTime, expired.StartupTime);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PeerClose_CanceledCall_DoesNotSelectANewLossPolicy(bool runtimeSide)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);
        await (runtimeSide ? runtime : worker).DisposeAsync();

        await (runtimeSide ? worker : runtime).WaitForTerminationAsync(timeout.Token);
        await WaitForReleaseAsync(factory.Services.GetRequiredService<FunctionRpcRelay>(), timeout.Token);
        SplitWorkerOriginState disconnected = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.Starting, disconnected.Status);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(disconnected, await ReadSplitManagementStateAsync(management, timeout.Token));
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.ServiceUnavailable, timeout.Token);
    }

    [Fact]
    public async Task RuntimeClose_RacingInitialization_RemovesOnlyIfInitializationWon()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        await runtime.WriteAsync(new StreamingMessage { WorkerInitRequest = new() { HostVersion = "test-host" } }, timeout.Token);
        await worker.ReadAsync(timeout.Token);
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        async Task TryInitializeAsync()
        {
            try
            {
                await worker.WriteAsync(new StreamingMessage
                {
                    WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Success } }
                }, timeout.Token);
            }
            catch (GrpcRpcException error) when (error.StatusCode == StatusCode.Unavailable)
            {
            }
        }

        await Task.WhenAll(TryInitializeAsync(), runtime.CompleteRequestAsync(timeout.Token));
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        WorkerPodState state = factory.Services.GetRequiredService<WorkerPodStateManager>().State;
        SplitWorkerOriginState after = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(state.HasAcceptedLink ? SplitWorkerStatus.RemovalRequested : SplitWorkerStatus.Starting, after.Status);
        Assert.Equal(before.RevisionId + (state.HasAcceptedLink ? 2 : 1), after.RevisionId);
        Assert.Equal(before.StartupTime, after.StartupTime);
        Assert.Equal(before.Link, after.Link);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PeerClose_HttpOverride_ClosesAfterInitializedPeerCompletion(bool runtimeSide)
    {
        int forwardedRequests = 0;
        await using var destination = await WorkerHttpForwardingTests.StartWorkerAsync(_ =>
        {
            Interlocked.Increment(ref forwardedRequests);
            return Task.CompletedTask;
        });
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(
            clock, httpEndpoint: WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient http = factory.CreateHttpForwardingClient();
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);

        using HttpResponseMessage before = await http.GetAsync("/before-runtime-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);
        await (runtimeSide ? runtime : worker).CompleteRequestAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        using HttpResponseMessage after = await http.GetAsync("/after-runtime-close", timeout.Token);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, after.StatusCode);
        Assert.Equal(1, Volatile.Read(ref forwardedRequests));
        Assert.Equal(SplitWorkerStatus.RemovalRequested,
            (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
    }

    [Fact]
    public async Task WorkerClose_InitializedAttachment_DoesNotRemoveAnotherProxy()
    {
        int forwardedRequests = 0;
        await using var destination = await WorkerHttpForwardingTests.StartWorkerAsync(_ =>
        {
            Interlocked.Increment(ref forwardedRequests);
            return Task.CompletedTask;
        });
        string httpEndpoint = WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri;
        await using WorkerProxyWebApplicationFactory closingFactory = CreateTimeoutFactory(new LifecycleClock(), httpEndpoint: httpEndpoint);
        await using WorkerProxyWebApplicationFactory healthyFactory = CreateTimeoutFactory(new LifecycleClock(), httpEndpoint: httpEndpoint);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient closingManagement = closingFactory.CreateWorkerProxyClient();
        using HttpClient healthyManagement = healthyFactory.CreateWorkerProxyClient();
        using HttpClient closingHttp = closingFactory.CreateHttpForwardingClient();
        using HttpClient healthyHttp = healthyFactory.CreateHttpForwardingClient();
        await using RelayClient closingWorker = CreateClient(closingFactory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient closingRuntime = CreateClient(closingFactory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await using RelayClient healthyWorker = CreateClient(healthyFactory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient healthyRuntime = CreateClient(healthyFactory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(closingWorker, closingRuntime, string.Empty, timeout.Token);
        await InitializeLinkAsync(healthyWorker, healthyRuntime, string.Empty, timeout.Token);
        SplitWorkerOriginState healthyState = await ReadSplitManagementStateAsync(healthyManagement, timeout.Token);
        using HttpResponseMessage before = await healthyHttp.GetAsync("/before-worker-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);

        await closingWorker.CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await closingRuntime.WaitForTerminationAsync(timeout.Token));
        Assert.Equal(StatusCode.Unavailable, await closingWorker.WaitForTerminationAsync(timeout.Token));

        Assert.Equal(SplitWorkerStatus.RemovalRequested,
            (await ReadSplitManagementStateAsync(closingManagement, timeout.Token)).Status);
        using HttpResponseMessage rejected = await closingHttp.GetAsync("/removed-worker", timeout.Token);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, rejected.StatusCode);
        using HttpResponseMessage after = await healthyHttp.GetAsync("/after-worker-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
        await AssertReadinessAsync(healthyManagement, "/admin/worker/ready", HttpStatusCode.OK, timeout.Token);
        Assert.Equal(healthyState, await ReadSplitManagementStateAsync(healthyManagement, timeout.Token));
        StreamingMessage message = CreateMessage("healthy-runtime");
        await healthyRuntime.WriteAsync(message, timeout.Token);
        Assert.Equal(message, await healthyWorker.ReadAsync(timeout.Token));
        message = CreateMessage("healthy-worker");
        await healthyWorker.WriteAsync(message, timeout.Token);
        Assert.Equal(message, await healthyRuntime.ReadAsync(timeout.Token));
        Assert.Equal(2, Volatile.Read(ref forwardedRequests));
    }
}
