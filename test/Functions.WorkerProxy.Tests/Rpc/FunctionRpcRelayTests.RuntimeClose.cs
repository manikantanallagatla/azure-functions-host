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
    [InlineData(false)]
    [InlineData(true)]
    public async Task RuntimeClose_AcceptedAttachment_PublishesStickyRemovalAndClosesIntake(bool assigned)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        var request = CreateLinkRequest("accepted-runtime-close");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        if (assigned)
        {
            using HttpResponseMessage assignment = await PutManagementJsonAsync(
                management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
            Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
        }

        await AcceptLinkAsync(factory, request, timeout.Token);
        SplitWorkerOriginState accepted = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(accepted, await ReadSplitManagementStateAsync(management, timeout.Token));
        Task<HttpResponseMessage> poll = PollSplitManagementStateAsync(management, accepted, timeout.Token);
        await WaitForManagementPollAsync(factory.Services.GetRequiredService<WorkerPodStateManager>(), timeout.Token);

        await runtime.CompleteRequestAsync(timeout.Token);
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
        GrpcRpcException lateAcceptance = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, request, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, lateAcceptance.StatusCode);
        await using RelayClient replacement = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        GrpcRpcException rejected = await replacement.WriteAndReadRejectionAsync(
            new StreamingMessage { WorkerInitRequest = new() { HostVersion = "replacement" } }, timeout.Token);
        Assert.Equal(StatusCode.Unavailable, rejected.StatusCode);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(removed, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RuntimeClose_BeforeAcceptance_AllowsRetryWithoutResettingNeverLinkedDeadline(bool initialized)
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
            if (initialized)
            {
                await InitializeLinkAsync(worker, runtime, "not-accepted", timeout.Token);
            }
            else
            {
                await ExchangeAsync(runtime, worker, "not-initialized", timeout.Token);
            }

            await runtime.CompleteRequestAsync(timeout.Token);
            await runtime.WaitForTerminationAsync(timeout.Token);
            await worker.WaitForTerminationAsync(timeout.Token);
        }

        await WaitForReleaseAsync(relay, timeout.Token);
        Assert.Equal(SplitWorkerStatus.Starting, (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(replacementWorker, replacementRuntime, "retry-init", timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(1));
        SplitWorkerOriginState expired = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, expired.Status);
        Assert.Equal(initial.StartupTime, expired.StartupTime);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task RuntimeClose_CancellationOrWorkerClose_DoesNotSelectANewLossPolicy(bool runtimeSide, bool canceled)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        var request = CreateLinkRequest("excluded-close");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        await AcceptLinkAsync(factory, request, timeout.Token);
        RelayClient closingPeer = runtimeSide ? runtime : worker;
        if (canceled)
        {
            await closingPeer.DisposeAsync();
        }
        else
        {
            await closingPeer.CompleteRequestAsync(timeout.Token);
        }

        await (runtimeSide ? worker : runtime).WaitForTerminationAsync(timeout.Token);
        await WaitForReleaseAsync(factory.Services.GetRequiredService<FunctionRpcRelay>(), timeout.Token);
        SplitWorkerOriginState disconnected = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.Starting, disconnected.Status);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(disconnected, await ReadSplitManagementStateAsync(management, timeout.Token));
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.ServiceUnavailable, timeout.Token);
    }

    [Fact]
    public async Task RuntimeClose_RacingAcceptance_RemovesOnlyIfAcceptanceWon()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        var request = CreateLinkRequest("close-acceptance-race");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        bool accepted = false;
        async Task TryAcceptAsync()
        {
            try
            {
                await AcceptLinkAsync(factory, request, timeout.Token);
                accepted = true;
            }
            catch (GrpcRpcException error) when (error.StatusCode == StatusCode.FailedPrecondition)
            {
            }
        }

        await Task.WhenAll(TryAcceptAsync(), runtime.CompleteRequestAsync(timeout.Token));
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        SplitWorkerOriginState after = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(accepted ? SplitWorkerStatus.RemovalRequested : SplitWorkerStatus.Starting, after.Status);
        Assert.Equal(before.RevisionId + (accepted ? 2 : 1), after.RevisionId);
        Assert.Equal(before.StartupTime, after.StartupTime);
        Assert.Equal(before.Link, after.Link);
    }

    [Fact]
    public async Task RuntimeClose_HttpOverride_ClosesAfterAcceptedRuntimeCompletion()
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
        var request = CreateLinkRequest("http-runtime-close");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        await AcceptLinkAsync(factory, request, timeout.Token);

        using HttpResponseMessage before = await http.GetAsync("/before-runtime-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);
        await runtime.CompleteRequestAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        using HttpResponseMessage after = await http.GetAsync("/after-runtime-close", timeout.Token);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, after.StatusCode);
        Assert.Equal(1, Volatile.Read(ref forwardedRequests));
        Assert.Equal(SplitWorkerStatus.RemovalRequested,
            (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
    }
}
