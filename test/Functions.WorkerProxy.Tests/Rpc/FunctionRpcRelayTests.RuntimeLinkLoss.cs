// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.Http;
using Azure.Functions.WorkerProxy.Rpc;
using Azure.Functions.WorkerProxy.State;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RuntimeEventStream_EofBeforeInitializationRequestsRemoval(bool assigned)
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        if (assigned)
        {
            await AssignWorkerAsync(management, timeout.Token);
        }

        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        await runtime.WriteAsync(CreateMessage("runtime-admitted"), timeout.Token);
        await WaitForAttachmentAsync(relay, FunctionRpcRelaySide.Runtime, timeout.Token);
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);

        await runtime.CompleteRequestAsync(timeout.Token);

        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(SplitWorkerLinkState.Unlinked, removed.Link.State);
        Assert.Null(removed.Link.Runtime);
        Assert.Equal(initial.RevisionId + 1, removed.RevisionId);
        Assert.Equal(initial.StartupTime, removed.StartupTime);
    }

    [Theory]
    [InlineData(false, "worker-init")]
    [InlineData(false, "")]
    [InlineData(true, "worker-init")]
    public async Task RuntimeEventStream_RemovalIsStickyLinkedPollableAndClosesAdmission(
        bool cancelRuntime, string initializationRequestId)
    {
        int forwardedRequests = 0;
        await using WebApplication destination = await WorkerHttpForwardingTests.StartWorkerAsync(context =>
        {
            Interlocked.Increment(ref forwardedRequests);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
            return Task.CompletedTask;
        });
        Dictionary<string, string?> configuration = new()
        {
            [$"{WorkerProxyOptions.SectionName}:{nameof(WorkerProxyOptions.WorkerHttpEndpoint)}"] =
                WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri
        };
        await using WorkerProxyWebApplicationFactory factory = new(configuration);
        WorkerPodStateManager manager = factory.Services.GetRequiredService<WorkerPodStateManager>();
        using CancellationTokenSource timeout = new(TestTimeout);
        using CancellationTokenSource runtimeCancellation = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        using HttpClient management = factory.CreateWorkerProxyClient();
        using HttpClient http = factory.CreateHttpForwardingClient();
        await AssignWorkerAsync(management, timeout.Token);
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        Metadata headers = new()
        {
            { "x-ms-functions-runtime-identity-bin", Encoding.UTF8.GetBytes(RuntimeIdentityJson) }
        };
        await using RelayClient runtime = CreateClient(
            factory.GetFunctionRpcAddress(FunctionRpcRelaySide.Runtime), runtimeCancellation.Token, headers);
        await InitializeLinkedWorkerAsync(manager, worker, runtime, initializationRequestId, timeout.Token);
        using (HttpResponseMessage before = await http.GetAsync("/before-removal", timeout.Token))
        {
            Assert.Equal(HttpStatusCode.NoContent, before.StatusCode);
        }

        SplitWorkerOriginState running = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.Running, running.Status);
        Assert.Equal(SplitWorkerLinkState.Linked, running.Link.State);
        Task<HttpResponseMessage> poll = PollSplitManagementStateAsync(management, running, timeout.Token);
        await WaitForManagementPollAsync(manager, timeout.Token);
        if (cancelRuntime)
        {
            runtimeCancellation.Cancel();
        }
        else
        {
            await runtime.CompleteRequestAsync(timeout.Token);
        }

        await Task.WhenAll(runtime.WaitForTerminationAsync(timeout.Token), worker.WaitForTerminationAsync(timeout.Token));
        using HttpResponseMessage changed = await poll;
        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(changed, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(running.RevisionId + 1, removed.RevisionId);
        Assert.Equal(running.Link, removed.Link);
        Assert.Equal(running.StartupTime, removed.StartupTime);
        Assert.Equal(running.ConfigLastModifiedTime, removed.ConfigLastModifiedTime);
        Assert.Equal(running.ContentLastModifiedTime, removed.ContentLastModifiedTime);
        Assert.True(removed.IsUnhealthy);
        Assert.Null(removed.FunctionGroup);
        WorkerPodState state = manager.State;
        Assert.False(state.IsWorkerReady);
        Assert.False(state.IsRuntimeInitialized);
        Assert.False(state.IsWorkerAttached);
        Assert.Null(state.SessionId);
        Assert.Null(state.WorkerId);
        Assert.Null(state.FunctionAppName);
        Assert.Equal(WorkerAssignmentState.Unassigned, state.AssignmentState);
        Assert.False(manager.RequestRemoval());
        Assert.False(manager.OnSessionTerminated(1));
        Assert.Same(state, manager.State);

        await AssertReadinessAsync(management, "/admin/instance/ready", HttpStatusCode.OK, timeout.Token);
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.ServiceUnavailable, timeout.Token);
        using (HttpResponseMessage assignment = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token))
        {
            await AssertManagementErrorAsync(assignment, HttpStatusCode.ServiceUnavailable, "WorkerNotReady", timeout.Token);
        }

        using (HttpResponseMessage after = await http.GetAsync("/after-removal", timeout.Token))
        {
            Assert.Equal(HttpStatusCode.ServiceUnavailable, after.StatusCode);
        }

        Assert.Equal(1, Volatile.Read(ref forwardedRequests));
        foreach (FunctionRpcRelaySide side in new[] { FunctionRpcRelaySide.Runtime, FunctionRpcRelaySide.Worker })
        {
            await using RelayClient replacement = CreateClient(factory, side, timeout.Token);
            Grpc.Core.RpcException rejected = await replacement.WriteAndReadRejectionAsync(
                side == FunctionRpcRelaySide.Worker ? CreateStartStream("replacement") : CreateMessage("replacement"),
                timeout.Token);
            Assert.Equal(StatusCode.Unavailable, rejected.StatusCode);
        }

        Assert.Equal(removed, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RuntimeEventStream_FaultAfterInitializationPreservesLink(bool writeFailure)
    {
        WorkerPodStateManager manager = CreatePodStateManager();
        Assert.Equal(WorkerAssignmentResult.Created, manager.Assign(CreateWorkerAssignment()));
        await using FunctionRpcRelay relay = CreateInProcessRelay(manager);
        using CancellationTokenSource timeout = new(TestTimeout);
        Channel<StreamingMessage> runtimeInbound = Channel.CreateUnbounded<StreamingMessage>();
        Channel<StreamingMessage> workerInbound = Channel.CreateUnbounded<StreamingMessage>();
        Channel<StreamingMessage> workerOutbound = Channel.CreateUnbounded<StreamingMessage>();
        IOException expectedException = new("Injected Runtime transport failure.");
        int failWrites = 0;
        Mock<IServerStreamWriter<StreamingMessage>> writer = new(MockBehavior.Strict);
        writer.Setup(instance => instance.WriteAsync(It.IsAny<StreamingMessage>(), It.IsAny<CancellationToken>()))
            .Returns((StreamingMessage _, CancellationToken _) => Volatile.Read(ref failWrites) == 0
                ? Task.CompletedTask : Task.FromException(expectedException));
        SplitPodIdentity identity = new("runtime-ns", "runtime-pod", new("test-app", 42, "test-stamp"));
        Task<FunctionRpcRelayTerminalState> runtimeTask = relay.AttachAsync(
            FunctionRpcRelaySide.Runtime, CreateMessageReader(runtimeInbound.Reader), writer.Object, timeout.Token, identity);
        Task<FunctionRpcRelayTerminalState> workerTask = relay.AttachAsync(
            FunctionRpcRelaySide.Worker, CreateMessageReader(workerInbound.Reader),
            CreateMessageWriter(workerOutbound.Writer), timeout.Token);
        await workerInbound.Writer.WriteAsync(CreateStartStream(), timeout.Token);
        StreamingMessage request = new() { RequestId = "init", WorkerInitRequest = new() };
        await runtimeInbound.Writer.WriteAsync(request, timeout.Token);
        Assert.Equal(request, await workerOutbound.Reader.ReadAsync(timeout.Token));
        await workerInbound.Writer.WriteAsync(CreateInitResponse(request.RequestId, httpUri: null), timeout.Token);
        await WaitForRuntimeInitializedAsync(manager, timeout.Token);
        SplitWorkerOriginState running = SplitWorkerOriginState.FromState(manager.State);

        if (writeFailure)
        {
            Volatile.Write(ref failWrites, 1);
            await workerInbound.Writer.WriteAsync(CreateMessage("trigger-write-failure"), timeout.Token);
        }
        else
        {
            runtimeInbound.Writer.Complete(expectedException);
        }

        FunctionRpcRelayTerminalState[] terminal = await Task.WhenAll(runtimeTask, workerTask).WaitAsync(timeout.Token);
        Assert.All(terminal, state =>
        {
            Assert.Equal(FunctionRpcRelaySide.Runtime, state.Side);
            Assert.Equal(FunctionRpcRelayTerminationReason.Faulted, state.Reason);
        });
        SplitWorkerOriginState removed = SplitWorkerOriginState.FromState(manager.State);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(running.RevisionId + 1, removed.RevisionId);
        Assert.Equal(running.Link, removed.Link);
        Assert.Equal(identity, removed.Link.Runtime);
        Assert.False(manager.State.IsWorkerReady);
    }

    [Fact]
    public async Task HttpForwarding_RemovalDuringReadinessWaitPreventsDispatch()
    {
        int forwardedRequests = 0;
        using BlockingLogger<WorkerEndpointReadinessProbe> logger = new(eventIdToBlock: 1);
        await using WebApplication destination = await WorkerHttpForwardingTests.StartWorkerAsync(context =>
        {
            Interlocked.Increment(ref forwardedRequests);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
            return Task.CompletedTask;
        });
        Dictionary<string, string?> configuration = new()
        {
            [$"{WorkerProxyOptions.SectionName}:{nameof(WorkerProxyOptions.WorkerHttpEndpoint)}"] =
                WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri
        };
        await using WorkerProxyWebApplicationFactory factory = new(configuration,
            services => services.AddSingleton<ILogger<WorkerEndpointReadinessProbe>>(logger));
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient client = factory.CreateHttpForwardingClient();
        Task<HttpResponseMessage> pendingRequest = client.GetAsync("/waiting-for-readiness", timeout.Token);
        try
        {
            await logger.LogEntered.WaitAsync(timeout.Token);
            Assert.False(pendingRequest.IsCompleted);
            Assert.True(factory.Services.GetRequiredService<WorkerPodStateManager>().RequestRemoval());
        }
        finally
        {
            logger.Release();
        }

        using HttpResponseMessage response = await pendingRequest;
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(0, Volatile.Read(ref forwardedRequests));
    }

    [Fact]
    public async Task ApplicationShutdown_InitializedWorkerDoesNotRequestRemoval()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await AssignWorkerAsync(management, timeout.Token);
        WorkerPodStateManager manager = factory.Services.GetRequiredService<WorkerPodStateManager>();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        Metadata headers = new()
        {
            { "x-ms-functions-runtime-identity-bin", Encoding.UTF8.GetBytes(RuntimeIdentityJson) }
        };
        await using RelayClient runtime = CreateClient(
            factory.GetFunctionRpcAddress(FunctionRpcRelaySide.Runtime), timeout.Token, headers);
        try
        {
            await InitializeLinkedWorkerAsync(manager, worker, runtime, "init", timeout.Token);
        }
        finally
        {
            await factory.DisposeAsync();
        }

        Assert.False(manager.State.RemovalRequested);
        Assert.False(manager.State.IsWorkerReady);
    }

    private static async Task InitializeLinkedWorkerAsync(
        WorkerPodStateManager manager, RelayClient worker, RelayClient runtime,
        string requestId, CancellationToken cancellationToken)
    {
        StreamingMessage start = CreateStartStream();
        await worker.WriteAsync(start, cancellationToken);
        Assert.Equal(start, await runtime.ReadAsync(cancellationToken));
        StreamingMessage request = new() { RequestId = requestId, WorkerInitRequest = new() { HostVersion = "test-host" } };
        await runtime.WriteAsync(request, cancellationToken);
        Assert.Equal(request, await worker.ReadAsync(cancellationToken));
        await worker.WriteAsync(CreateInitResponse(requestId, httpUri: null), cancellationToken);
        Assert.NotNull((await runtime.ReadAsync(cancellationToken)).WorkerInitResponse);
        await WaitForRuntimeInitializedAsync(manager, cancellationToken);
    }

    private static async Task WaitForRuntimeInitializedAsync(WorkerPodStateManager manager, CancellationToken cancellationToken)
    {
        while (true)
        {
            WorkerPodState snapshot = manager.State;
            if (snapshot.IsRuntimeInitialized)
            {
                return;
            }

            await manager.WaitForChangeAsync(snapshot.Revision, cancellationToken);
        }
    }
}
