// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.Rpc;
using Azure.Functions.WorkerProxy.State;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    [Theory]
    [InlineData(false, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(true, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(false, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(true, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(false, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(true, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(false, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(true, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(false, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(true, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(false, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(true, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    public async Task PeerTermination_InitializedAttachment_PublishesStickyRemovalAndClosesIntake(
        bool assigned, bool runtimeSide, string termination)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateFaultableClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateFaultableClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
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

        await TerminatePeerAsync(runtimeSide ? runtime : worker, termination, timeout.Token);
        StatusCode siblingStatus = await (runtimeSide ? worker : runtime).WaitForTerminationAsync(timeout.Token);
        bool canceled = string.Equals(termination, nameof(FunctionRpcRelayTerminationReason.Canceled), StringComparison.Ordinal);
        Assert.Equal(canceled
            ? StatusCode.Cancelled : StatusCode.Unavailable, siblingStatus);
        if (!canceled)
        {
            Assert.Equal(StatusCode.Unavailable, await (runtimeSide ? runtime : worker).WaitForTerminationAsync(timeout.Token));
        }

        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        await WaitForReleaseAsync(relay, timeout.Token);
        Assert.Equal(termination, relay.LastTerminalState?.Reason.ToString());
        Assert.Equal(runtimeSide ? FunctionRpcRelaySide.Runtime : FunctionRpcRelaySide.Worker, relay.LastTerminalState?.Side);
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
    [InlineData(false, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(true, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(false, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(true, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(false, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(true, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(false, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(true, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(false, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(true, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(false, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(true, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    public async Task PeerTermination_BeforeInitialization_AllowsRetryWithoutResettingNeverLinkedDeadline(
        bool failedInitialization, bool runtimeSide, string termination)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(4));
        await using (RelayClient worker = CreateFaultableClient(factory, FunctionRpcRelaySide.Worker, timeout.Token))
        await using (RelayClient runtime = CreateFaultableClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token))
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

            await TerminatePeerAsync(runtimeSide ? runtime : worker, termination, timeout.Token);
            await (runtimeSide ? worker : runtime).WaitForTerminationAsync(timeout.Token);
        }

        await WaitForReleaseAsync(relay, timeout.Token);
        Assert.Equal(termination, relay.LastTerminalState?.Reason.ToString());
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
    public async Task PeerTermination_ProxyShutdown_DoesNotRequestRemoval(bool initialized)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        if (initialized)
        {
            await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);
        }
        else
        {
            await ExchangeAsync(runtime, worker, "before-shutdown", timeout.Token);
        }

        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        await relay.StopAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        Assert.Equal(FunctionRpcRelayTerminationReason.Shutdown, relay.LastTerminalState?.Reason);
        SplitWorkerOriginState disconnected = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.Starting, disconnected.Status);
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
    [InlineData(true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    public async Task PeerTermination_HttpOverride_ClosesAfterInitializedAttachmentLoss(bool runtimeSide, string termination)
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
        await using RelayClient worker = CreateFaultableClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateFaultableClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);

        using HttpResponseMessage before = await http.GetAsync("/before-runtime-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);
        await TerminatePeerAsync(runtimeSide ? runtime : worker, termination, timeout.Token);
        await (runtimeSide ? worker : runtime).WaitForTerminationAsync(timeout.Token);
        using HttpResponseMessage after = await http.GetAsync("/after-runtime-close", timeout.Token);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, after.StatusCode);
        Assert.Equal(1, Volatile.Read(ref forwardedRequests));
        Assert.Equal(SplitWorkerStatus.RemovalRequested,
            (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
    }

    [Theory]
    [InlineData(true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    public async Task PeerTermination_InitializedAttachment_DoesNotRemoveAnotherProxy(bool runtimeSide, string termination)
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
        await using RelayClient closingWorker = CreateFaultableClient(closingFactory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient closingRuntime = CreateFaultableClient(closingFactory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await using RelayClient healthyWorker = CreateClient(healthyFactory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient healthyRuntime = CreateClient(healthyFactory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(closingWorker, closingRuntime, string.Empty, timeout.Token);
        await InitializeLinkAsync(healthyWorker, healthyRuntime, string.Empty, timeout.Token);
        SplitWorkerOriginState healthyState = await ReadSplitManagementStateAsync(healthyManagement, timeout.Token);
        using HttpResponseMessage before = await healthyHttp.GetAsync("/before-worker-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);

        await TerminatePeerAsync(runtimeSide ? closingRuntime : closingWorker, termination, timeout.Token);
        await (runtimeSide ? closingWorker : closingRuntime).WaitForTerminationAsync(timeout.Token);

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

    private static RelayClient CreateFaultableClient(
        WorkerProxyWebApplicationFactory factory, FunctionRpcRelaySide side, CancellationToken cancellationToken)
    {
        Marshaller<StreamingMessage> marshaller = Marshallers.Create(
            static message => string.Equals(message.RequestId, "malformed-frame", StringComparison.Ordinal)
                ? [0x80]
                : message.ToByteArray(),
            static payload => StreamingMessage.Parser.ParseFrom(payload));
        Method<StreamingMessage, StreamingMessage> method = new(
            MethodType.DuplexStreaming, FunctionRpcServiceName, EventStreamMethodName, marshaller, StreamingMessageMarshaller);
        return CreateClient(factory.GetFunctionRpcAddress(side), method, cancellationToken);
    }

    private static async Task TerminatePeerAsync(RelayClient peer, string termination, CancellationToken cancellationToken)
    {
        switch (termination)
        {
            case nameof(FunctionRpcRelayTerminationReason.PeerClosed):
                await peer.CompleteRequestAsync(cancellationToken);
                break;
            case nameof(FunctionRpcRelayTerminationReason.Canceled):
                await peer.DisposeAsync();
                break;
            case nameof(FunctionRpcRelayTerminationReason.Faulted):
                await peer.WriteAsync(CreateMessage("malformed-frame"), cancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(termination));
        }
    }
}
