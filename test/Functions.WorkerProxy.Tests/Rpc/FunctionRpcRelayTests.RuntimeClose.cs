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
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    public enum RelayLifecycleScenario
    {
        Initialized,
        Assigned,
        Unassigned
    }

    public enum RemovalAdmissionScenario
    {
        StickyAdmission,
        SiblingIsolation,
        Shutdown,
        InitializationCloseRace
    }

    [Fact]
    public async Task RuntimeClose_ConfirmsStickyRemovalOwnershipInGrpcTrailer()
    {
        await using WorkerProxyWebApplicationFactory factory =
            CreateTimeoutFactory(new LifecycleClock());
        using CancellationTokenSource timeout = new(TestTimeout);
        await using RelayClient worker = CreateClient(
            factory,
            FunctionRpcRelaySide.Worker,
            timeout.Token);
        await using RelayClient runtime = CreateClient(
            factory,
            FunctionRpcRelaySide.Runtime,
            timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);

        await runtime.CompleteRequestAsync(timeout.Token);
        GrpcRpcException termination =
            await runtime.WaitForTerminationExceptionAsync(timeout.Token);

        Assert.Equal(StatusCode.Unavailable, termination.StatusCode);
        Assert.Contains(
            termination.Trailers,
            trailer =>
                string.Equals(
                    trailer.Key,
                    FunctionRpcRelayConstants.WorkerRemovalRequestedTrailerName,
                    StringComparison.Ordinal) &&
                string.Equals(
                    trailer.Value,
                    FunctionRpcRelayConstants.WorkerRemovalRequestedTrailerValue,
                    StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(RelayLifecycleScenario.Initialized, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(RelayLifecycleScenario.Initialized, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(RelayLifecycleScenario.Initialized, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(RelayLifecycleScenario.Initialized, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(RelayLifecycleScenario.Initialized, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(RelayLifecycleScenario.Initialized, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(RelayLifecycleScenario.Assigned, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(RelayLifecycleScenario.Assigned, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(RelayLifecycleScenario.Assigned, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(RelayLifecycleScenario.Assigned, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(RelayLifecycleScenario.Assigned, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(RelayLifecycleScenario.Assigned, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(RelayLifecycleScenario.Unassigned, true, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(RelayLifecycleScenario.Unassigned, false, nameof(FunctionRpcRelayTerminationReason.PeerClosed))]
    [InlineData(RelayLifecycleScenario.Unassigned, true, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(RelayLifecycleScenario.Unassigned, false, nameof(FunctionRpcRelayTerminationReason.Canceled))]
    [InlineData(RelayLifecycleScenario.Unassigned, true, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    [InlineData(RelayLifecycleScenario.Unassigned, false, nameof(FunctionRpcRelayTerminationReason.Faulted))]
    public async Task PeerTermination_AssignedInitializedAndUnassignedSessionsFollowLifecyclePolicy(
        RelayLifecycleScenario lifecycle,
        bool runtimeSide,
        string termination)
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateFaultableClient(
            factory,
            FunctionRpcRelaySide.Worker,
            timeout.Token);
        await using RelayClient runtime = CreateFaultableClient(
            factory,
            FunctionRpcRelaySide.Runtime,
            timeout.Token);

        switch (lifecycle)
        {
            case RelayLifecycleScenario.Initialized:
                await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);
                clock.Advance(TimeSpan.FromHours(1));
                break;
            case RelayLifecycleScenario.Assigned:
                clock.Advance(TimeSpan.FromMinutes(4));
                await worker.WriteAsync(CreateStartStream(), timeout.Token);
                await runtime.ReadAsync(timeout.Token);
                using (HttpResponseMessage assignment = await PutManagementJsonAsync(
                    management,
                    "/admin/worker/assignment",
                    ManagementAssignment,
                    timeout.Token))
                {
                    Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
                }

                break;
            case RelayLifecycleScenario.Unassigned:
                clock.Advance(TimeSpan.FromMinutes(4));
                await ExchangeAsync(runtime, worker, "not-initialized", timeout.Token);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(lifecycle));
        }

        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        await TerminatePeerAsync(runtimeSide ? runtime : worker, termination, timeout.Token);
        StatusCode siblingStatus = await (runtimeSide ? worker : runtime).WaitForTerminationAsync(timeout.Token);
        bool canceled = string.Equals(
            termination,
            nameof(FunctionRpcRelayTerminationReason.Canceled),
            StringComparison.Ordinal);
        Assert.Equal(canceled ? StatusCode.Cancelled : StatusCode.Unavailable, siblingStatus);
        if (!canceled)
        {
            Assert.Equal(
                StatusCode.Unavailable,
                await (runtimeSide ? runtime : worker).WaitForTerminationAsync(timeout.Token));
        }

        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        await WaitForReleaseAsync(relay, timeout.Token);
        Assert.Equal(termination, relay.LastTerminalState?.Reason.ToString());
        Assert.Equal(
            runtimeSide ? FunctionRpcRelaySide.Runtime : FunctionRpcRelaySide.Worker,
            relay.LastTerminalState?.Side);
        SplitWorkerOriginState after = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(before.StartupTime, after.StartupTime);

        if (lifecycle is RelayLifecycleScenario.Unassigned)
        {
            Assert.Equal(SplitWorkerStatus.Starting, after.Status);
            await using RelayClient replacementWorker = CreateClient(
                factory,
                FunctionRpcRelaySide.Worker,
                timeout.Token);
            await using RelayClient replacementRuntime = CreateClient(
                factory,
                FunctionRpcRelaySide.Runtime,
                timeout.Token);
            await ExchangeAsync(
                replacementRuntime,
                replacementWorker,
                "retry-before-initialization",
                timeout.Token);
            clock.Advance(TimeSpan.FromMinutes(1));
            SplitWorkerOriginState expired = await ReadSplitManagementStateAsync(management, timeout.Token);
            Assert.Equal(SplitWorkerStatus.RemovalRequested, expired.Status);
            Assert.Equal(before.StartupTime, expired.StartupTime);
            return;
        }

        Assert.Equal(SplitWorkerStatus.RemovalRequested, after.Status);
        Assert.Equal(before.RevisionId + 1, after.RevisionId);
        Assert.Equal(before.Link, after.Link);
        await AssertReadinessAsync(management, "/admin/instance/ready", HttpStatusCode.OK, timeout.Token);
        await AssertReadinessAsync(
            management,
            "/admin/worker/ready",
            HttpStatusCode.ServiceUnavailable,
            timeout.Token);
        using HttpResponseMessage lateAssignment = await PutManagementJsonAsync(
            management,
            "/admin/worker/assignment",
            ManagementAssignment,
            timeout.Token);
        await AssertManagementErrorAsync(
            lateAssignment,
            HttpStatusCode.Conflict,
            "RemovalRequested",
            timeout.Token);
        await using RelayClient replacement = CreateClient(
            factory,
            FunctionRpcRelaySide.Runtime,
            timeout.Token);
        GrpcRpcException rejected = await replacement.WriteAndReadRejectionAsync(
            new StreamingMessage { WorkerInitRequest = new() { HostVersion = "replacement" } },
            timeout.Token);
        Assert.Equal(StatusCode.Unavailable, rejected.StatusCode);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(after, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Theory]
    [InlineData(RemovalAdmissionScenario.StickyAdmission)]
    [InlineData(RemovalAdmissionScenario.SiblingIsolation)]
    [InlineData(RemovalAdmissionScenario.Shutdown)]
    [InlineData(RemovalAdmissionScenario.InitializationCloseRace)]
    public async Task RemovalAdmission_IsStickyAcrossPublicSurfacesAndIsolatedPerProxy(
        RemovalAdmissionScenario scenario)
    {
        switch (scenario)
        {
            case RemovalAdmissionScenario.StickyAdmission:
                await VerifyStickyAdmissionAsync();
                break;
            case RemovalAdmissionScenario.SiblingIsolation:
                await VerifySiblingIsolationAsync();
                break;
            case RemovalAdmissionScenario.Shutdown:
                await VerifyShutdownDoesNotRequestRemovalAsync();
                break;
            case RemovalAdmissionScenario.InitializationCloseRace:
                await VerifyInitializationCloseRaceAsync();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario));
        }
    }

    private static async Task VerifyStickyAdmissionAsync()
    {
        int forwardedRequests = 0;
        await using var destination = await WorkerHttpForwardingTests.StartWorkerAsync(_ =>
        {
            Interlocked.Increment(ref forwardedRequests);
            return Task.CompletedTask;
        });
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(
            clock,
            httpEndpoint: WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        using HttpClient http = factory.CreateHttpForwardingClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);

        using HttpResponseMessage before = await http.GetAsync("/before-removal", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);
        await runtime.CompleteRequestAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await WaitForReleaseAsync(factory.Services.GetRequiredService<FunctionRpcRelay>(), timeout.Token);

        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        using HttpResponseMessage after = await http.GetAsync("/after-removal", timeout.Token);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, after.StatusCode);
        Assert.Equal(1, Volatile.Read(ref forwardedRequests));
        using HttpResponseMessage assignment = await PutManagementJsonAsync(
            management,
            "/admin/worker/assignment",
            ManagementAssignment,
            timeout.Token);
        await AssertManagementErrorAsync(
            assignment,
            HttpStatusCode.Conflict,
            "RemovalRequested",
            timeout.Token);
        foreach (FunctionRpcRelaySide side in new[]
        {
            FunctionRpcRelaySide.Runtime,
            FunctionRpcRelaySide.Worker
        })
        {
            await using RelayClient replacement = CreateClient(factory, side, timeout.Token);
            GrpcRpcException rejected = await replacement.WriteAndReadRejectionAsync(
                side is FunctionRpcRelaySide.Worker
                    ? CreateStartStream()
                    : CreateMessage("replacement"),
                timeout.Token);
            Assert.Equal(StatusCode.Unavailable, rejected.StatusCode);
        }

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(removed, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    private static async Task VerifySiblingIsolationAsync()
    {
        int forwardedRequests = 0;
        await using var destination = await WorkerHttpForwardingTests.StartWorkerAsync(_ =>
        {
            Interlocked.Increment(ref forwardedRequests);
            return Task.CompletedTask;
        });
        string httpEndpoint = WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri;
        await using WorkerProxyWebApplicationFactory closingFactory = CreateTimeoutFactory(
            new LifecycleClock(),
            httpEndpoint: httpEndpoint);
        await using WorkerProxyWebApplicationFactory healthyFactory = CreateTimeoutFactory(
            new LifecycleClock(),
            httpEndpoint: httpEndpoint);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient closingManagement = closingFactory.CreateWorkerProxyClient();
        using HttpClient healthyManagement = healthyFactory.CreateWorkerProxyClient();
        using HttpClient closingHttp = closingFactory.CreateHttpForwardingClient();
        using HttpClient healthyHttp = healthyFactory.CreateHttpForwardingClient();
        await using RelayClient closingWorker = CreateFaultableClient(
            closingFactory,
            FunctionRpcRelaySide.Worker,
            timeout.Token);
        await using RelayClient closingRuntime = CreateFaultableClient(
            closingFactory,
            FunctionRpcRelaySide.Runtime,
            timeout.Token);
        await using RelayClient healthyWorker = CreateClient(
            healthyFactory,
            FunctionRpcRelaySide.Worker,
            timeout.Token);
        await using RelayClient healthyRuntime = CreateClient(
            healthyFactory,
            FunctionRpcRelaySide.Runtime,
            timeout.Token);
        await InitializeLinkAsync(closingWorker, closingRuntime, string.Empty, timeout.Token);
        await InitializeLinkAsync(healthyWorker, healthyRuntime, string.Empty, timeout.Token);
        SplitWorkerOriginState healthyState = await ReadSplitManagementStateAsync(
            healthyManagement,
            timeout.Token);
        using HttpResponseMessage before = await healthyHttp.GetAsync("/before-worker-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, before.StatusCode);

        await TerminatePeerAsync(
            closingWorker,
            nameof(FunctionRpcRelayTerminationReason.Faulted),
            timeout.Token);
        await closingRuntime.WaitForTerminationAsync(timeout.Token);
        await WaitForReleaseAsync(
            closingFactory.Services.GetRequiredService<FunctionRpcRelay>(),
            timeout.Token);

        Assert.Equal(
            SplitWorkerStatus.RemovalRequested,
            (await ReadSplitManagementStateAsync(closingManagement, timeout.Token)).Status);
        using HttpResponseMessage rejected = await closingHttp.GetAsync("/removed-worker", timeout.Token);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, rejected.StatusCode);
        using HttpResponseMessage after = await healthyHttp.GetAsync("/after-worker-close", timeout.Token);
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
        await AssertReadinessAsync(
            healthyManagement,
            "/admin/worker/ready",
            HttpStatusCode.OK,
            timeout.Token);
        Assert.Equal(
            healthyState,
            await ReadSplitManagementStateAsync(healthyManagement, timeout.Token));
        StreamingMessage message = CreateMessage("healthy-runtime");
        await healthyRuntime.WriteAsync(message, timeout.Token);
        Assert.Equal(message, await healthyWorker.ReadAsync(timeout.Token));
        message = CreateMessage("healthy-worker");
        await healthyWorker.WriteAsync(message, timeout.Token);
        Assert.Equal(message, await healthyRuntime.ReadAsync(timeout.Token));
        Assert.Equal(2, Volatile.Read(ref forwardedRequests));
    }

    private static async Task VerifyShutdownDoesNotRequestRemovalAsync()
    {
        foreach ((bool initialized, bool assigned) in new[]
        {
            (true, true),
            (false, true),
            (true, false),
            (false, false)
        })
        {
            LifecycleClock clock = new();
            await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
            using CancellationTokenSource timeout = new(TestTimeout);
            using HttpClient management = factory.CreateWorkerProxyClient();
            await using RelayClient worker = CreateClient(
                factory,
                FunctionRpcRelaySide.Worker,
                timeout.Token);
            await using RelayClient runtime = CreateClient(
                factory,
                FunctionRpcRelaySide.Runtime,
                timeout.Token);
            if (initialized)
            {
                await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);
            }
            else
            {
                await ExchangeAsync(runtime, worker, "before-shutdown", timeout.Token);
            }

            if (assigned)
            {
                using HttpResponseMessage assignment = await PutManagementJsonAsync(
                    management,
                    "/admin/worker/assignment",
                    ManagementAssignment,
                    timeout.Token);
                Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
            }

            FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
            await relay.StopAsync(timeout.Token);
            await runtime.WaitForTerminationAsync(timeout.Token);
            await worker.WaitForTerminationAsync(timeout.Token);
            Assert.Equal(FunctionRpcRelayTerminationReason.Shutdown, relay.LastTerminalState?.Reason);
            SplitWorkerOriginState disconnected = await ReadSplitManagementStateAsync(
                management,
                timeout.Token);
            Assert.Equal(SplitWorkerStatus.Starting, disconnected.Status);
            await AssertReadinessAsync(
                management,
                "/admin/worker/ready",
                HttpStatusCode.ServiceUnavailable,
                timeout.Token);
        }
    }

    private static async Task VerifyInitializationCloseRaceAsync()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        await runtime.WriteAsync(
            new StreamingMessage { WorkerInitRequest = new() { HostVersion = "test-host" } },
            timeout.Token);
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
        Assert.Equal(
            state.HasAcceptedLink ? SplitWorkerStatus.RemovalRequested : SplitWorkerStatus.Starting,
            after.Status);
        Assert.Equal(before.RevisionId + (state.HasAcceptedLink ? 2 : 1), after.RevisionId);
        Assert.Equal(before.StartupTime, after.StartupTime);
        Assert.Equal(before.Link, after.Link);
    }

    private static RelayClient CreateFaultableClient(
        WorkerProxyWebApplicationFactory factory,
        FunctionRpcRelaySide side,
        CancellationToken cancellationToken)
    {
        Marshaller<StreamingMessage> marshaller = Marshallers.Create(
            static message => string.Equals(
                message.RequestId,
                "malformed-frame",
                StringComparison.Ordinal)
                ? [0x80]
                : message.ToByteArray(),
            static payload => StreamingMessage.Parser.ParseFrom(payload));
        Method<StreamingMessage, StreamingMessage> method = new(
            MethodType.DuplexStreaming,
            FunctionRpcServiceName,
            EventStreamMethodName,
            marshaller,
            StreamingMessageMarshaller);
        return CreateClient(factory.GetFunctionRpcAddress(side), method, cancellationToken);
    }

    private static async Task TerminatePeerAsync(
        RelayClient peer,
        string termination,
        CancellationToken cancellationToken)
    {
        if (string.Equals(
            termination,
            nameof(FunctionRpcRelayTerminationReason.PeerClosed),
            StringComparison.Ordinal))
        {
            await peer.CompleteRequestAsync(cancellationToken);
            return;
        }

        if (string.Equals(
            termination,
            nameof(FunctionRpcRelayTerminationReason.Canceled),
            StringComparison.Ordinal))
        {
            await peer.DisposeAsync();
            return;
        }

        if (string.Equals(
            termination,
            nameof(FunctionRpcRelayTerminationReason.Faulted),
            StringComparison.Ordinal))
        {
            await peer.WriteAsync(CreateMessage("malformed-frame"), cancellationToken);
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(termination));
    }
}
