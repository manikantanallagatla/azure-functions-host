// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.Rpc;
using Azure.Functions.WorkerProxy.State;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Azure.WebJobs.Script.Grpc.WorkerLink;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    private static readonly Method<AcceptLinkRequest, AcceptLinkResponse> AcceptLinkMethod = new(
        MethodType.Unary, "AzureFunctionsWorkerLink.WorkerLink", "Accept",
        Marshallers.Create(static value => value.ToByteArray(), static bytes => AcceptLinkRequest.Parser.ParseFrom(bytes)),
        Marshallers.Create(static value => value.ToByteArray(), static bytes => AcceptLinkResponse.Parser.ParseFrom(bytes)));

    [Fact]
    public async Task LinkAcceptance_InitializedMetadataAttachment_PublishesHistoryWithoutServing()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateLinkFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        AcceptLinkRequest request = CreateLinkRequest("metadata-init");

        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerLinkState.Unlinked, before.Link.State);
        await AcceptLinkAsync(factory, request, timeout.Token);

        SplitWorkerOriginState linked = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(before.RevisionId + 1, linked.RevisionId);
        Assert.Equal(SplitWorkerLinkState.Linked, linked.Link.State);
        Assert.Equal("runtime-pod", linked.Link.Runtime?.PodName);
        Assert.Equal(Guid.Parse(request.Runtime.PodGuid), linked.Link.Runtime?.PodGuid);
        Assert.Equal("test-app", linked.Link.Runtime?.App.FunctionAppName);
        Assert.Equal(SplitWorkerStatus.Starting, linked.Status);
        using var legacyState = await ReadManagementStateAsync(management, null, timeout.Token);
        Assert.Equal("None", legacyState.RootElement.GetProperty("workerPodState").GetProperty("podStatus").GetString());

        await worker.CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        SplitWorkerOriginState terminated = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(linked.Link, terminated.Link);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LinkAcceptance_ReplayIsIdempotentAndDifferentRuntimeOrAppConflicts(bool assignmentFirst)
    {
        await using WorkerProxyWebApplicationFactory factory = CreateLinkFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        AcceptLinkRequest request = CreateLinkRequest("replay-init");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        if (assignmentFirst)
        {
            using HttpResponseMessage assignment = await PutManagementJsonAsync(
                management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
            Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
            AcceptLinkRequest wrongApp = request.Clone();
            wrongApp.Runtime.App.FunctionAppName = "other-app";
            GrpcRpcException mismatch = await Assert.ThrowsAsync<GrpcRpcException>(
                () => AcceptLinkAsync(factory, wrongApp, timeout.Token));
            Assert.Equal(StatusCode.FailedPrecondition, mismatch.StatusCode);
        }

        await AcceptLinkAsync(factory, request, timeout.Token);
        if (!assignmentFirst)
        {
            using HttpResponseMessage wrongAssignment = await PutManagementJsonAsync(
                management, "/admin/worker/assignment",
                ManagementAssignment.Replace("test-app", "other-app", StringComparison.Ordinal), timeout.Token);
            Assert.Equal(HttpStatusCode.Conflict, wrongAssignment.StatusCode);
        }

        SplitWorkerOriginState accepted = await ReadSplitManagementStateAsync(management, timeout.Token);

        await AcceptLinkAsync(factory, request, timeout.Token);
        Assert.Equal(accepted, await ReadSplitManagementStateAsync(management, timeout.Token));
        AcceptLinkRequest conflict = request.Clone();
        conflict.Runtime.PodGuid = Guid.NewGuid().ToString();
        GrpcRpcException error = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, conflict, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, error.StatusCode);
        Assert.Equal(accepted, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Theory]
    [InlineData(false, false, false, StatusCode.Unimplemented)]
    [InlineData(true, true, false, StatusCode.Unimplemented)]
    [InlineData(true, false, true, StatusCode.InvalidArgument)]
    public async Task LinkAcceptance_DisabledWrongListenerOrIncompleteIdentity_DoesNotMutateState(
        bool enabled, bool workerListener, bool invalidIdentity, StatusCode expected)
    {
        await using WorkerProxyWebApplicationFactory factory = CreateLinkFactory(enabled);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        AcceptLinkRequest request = CreateLinkRequest("invalid-init");
        if (invalidIdentity)
        {
            request.Runtime.PodGuid = string.Empty;
        }

        GrpcRpcException error = await Assert.ThrowsAsync<GrpcRpcException>(() => AcceptLinkAsync(
            factory, request, timeout.Token, workerListener ? FunctionRpcRelaySide.Worker : FunctionRpcRelaySide.Runtime));
        Assert.Equal(expected, error.StatusCode);
        Assert.Equal(before, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Fact]
    public async Task LinkAcceptance_PendingOrFailedInitialization_DoesNotCountAsLinked()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateLinkFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        AcceptLinkRequest request = CreateLinkRequest("failed-init");
        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        await runtime.WriteAsync(new StreamingMessage
        {
            RequestId = request.InitializationId,
            WorkerInitRequest = new() { HostVersion = "test-host" }
        }, timeout.Token);
        await worker.ReadAsync(timeout.Token);

        GrpcRpcException pending = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, request, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, pending.StatusCode);
        await worker.WriteAsync(new StreamingMessage
        {
            RequestId = request.InitializationId,
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Failure } }
        }, timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        GrpcRpcException failed = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, request, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, failed.StatusCode);
        Assert.Equal(SplitWorkerLinkState.Unlinked, (await ReadSplitManagementStateAsync(management, timeout.Token)).Link.State);
    }

    [Fact]
    public async Task LinkAcceptance_StaleHandshakeCannotAcceptReplacementSession()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateLinkFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        AcceptLinkRequest stale = CreateLinkRequest("old-init");
        await using (RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token))
        await using (RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token))
        {
            await InitializeLinkAsync(worker, runtime, stale.InitializationId, timeout.Token);
            await worker.CompleteRequestAsync(timeout.Token);
            await runtime.WaitForTerminationAsync(timeout.Token);
            await worker.WaitForTerminationAsync(timeout.Token);
        }

        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        AcceptLinkRequest current = CreateLinkRequest("new-init");
        await InitializeLinkAsync(replacementWorker, replacementRuntime, current.InitializationId, timeout.Token);
        GrpcRpcException rejected = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, stale, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, rejected.StatusCode);
        await AcceptLinkAsync(factory, current, timeout.Token);
    }

    private static WorkerProxyWebApplicationFactory CreateLinkFactory(bool enabled = true) => new(
        new Dictionary<string, string?>
        {
            [$"{WorkerProxyOptions.SectionName}:{nameof(WorkerProxyOptions.EnableLinkLifecycle)}"] = enabled.ToString()
        });

    private static AcceptLinkRequest CreateLinkRequest(string initializationId) => new()
    {
        InitializationId = initializationId,
        Runtime = new()
        {
            ResourceNamespace = "test-namespace",
            PodName = "runtime-pod",
            PodGuid = "67039bac-3141-4d82-b6ab-fad68d7e39de",
            LegionStampName = "test-legion-stamp",
            PodType = "FunctionsSplitRuntimePod",
            App = new() { FunctionAppName = "test-app", FunctionAppSiteId = 42, AntaresStampName = "test-antares-stamp" }
        }
    };

    private static async Task InitializeLinkAsync(RelayClient worker, RelayClient runtime, string initializationId, CancellationToken token)
    {
        await worker.WriteAsync(CreateStartStream(), token);
        await runtime.ReadAsync(token);
        await runtime.WriteAsync(new StreamingMessage
        {
            RequestId = initializationId,
            WorkerInitRequest = new() { HostVersion = "test-host" }
        }, token);
        await worker.ReadAsync(token);
        await worker.WriteAsync(new StreamingMessage
        {
            RequestId = initializationId,
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Success } }
        }, token);
        await runtime.ReadAsync(token);
    }

    private static async Task AcceptLinkAsync(WorkerProxyWebApplicationFactory factory, AcceptLinkRequest request,
        CancellationToken token, FunctionRpcRelaySide side = FunctionRpcRelaySide.Runtime)
    {
        using GrpcChannel channel = GrpcChannel.ForAddress(factory.GetFunctionRpcAddress(side));
        using var call = channel.CreateCallInvoker().AsyncUnaryCall(AcceptLinkMethod, null, new CallOptions(cancellationToken: token), request);
        await call.ResponseAsync;
    }
}
