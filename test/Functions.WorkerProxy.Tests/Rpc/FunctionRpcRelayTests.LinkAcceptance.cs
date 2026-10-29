// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
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
    public async Task LinkAcceptance_InitializedMetadataAttachment_RecordsAcceptanceWithoutPublishingIdentity()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
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
        Assert.Equal(before.Link, linked.Link);
        Assert.Null(linked.Link.Runtime);
        Assert.Equal(SplitWorkerStatus.Starting, linked.Status);
        using var workerState = await ReadManagementStateAsync(management, null, timeout.Token);
        Assert.Equal("None", workerState.RootElement.GetProperty("workerPodState").GetProperty("podStatus").GetString());

        await worker.CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        SplitWorkerOriginState terminated = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(linked.Link, terminated.Link);
    }

    [Fact]
    public async Task LinkAcceptance_ReplayIsIdempotentAndAcceptedProxyCannotBeRetargeted()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        AcceptLinkRequest request = CreateLinkRequest("replay-init");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        await AcceptLinkAsync(factory, request, timeout.Token);
        SplitWorkerOriginState accepted = await ReadSplitManagementStateAsync(management, timeout.Token);

        await AcceptLinkAsync(factory, request, timeout.Token);
        Assert.Equal(accepted, await ReadSplitManagementStateAsync(management, timeout.Token));
        AcceptLinkRequest conflict = request.Clone();
        conflict.InitializationId = "different-init";
        GrpcRpcException error = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, conflict, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, error.StatusCode);
        Assert.Equal(accepted, await ReadSplitManagementStateAsync(management, timeout.Token));

        await worker.CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        await worker.WaitForTerminationAsync(timeout.Token);
        await using RelayClient replacement = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await replacement.WriteAsync(new StreamingMessage
        {
            RequestId = "replacement-init",
            WorkerInitRequest = new() { HostVersion = "test-host" }
        }, timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await replacement.WaitForTerminationAsync(timeout.Token));
    }

    [Theory]
    [InlineData(true, StatusCode.Unimplemented)]
    [InlineData(false, StatusCode.InvalidArgument)]
    public async Task LinkAcceptance_WrongListenerOrEmptyInitializationId_DoesNotMutateState(
        bool workerListener, StatusCode expected)
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        AcceptLinkRequest request = CreateLinkRequest(workerListener ? "invalid-init" : string.Empty);

        GrpcRpcException error = await Assert.ThrowsAsync<GrpcRpcException>(() => AcceptLinkAsync(
            factory, request, timeout.Token, workerListener ? FunctionRpcRelaySide.Worker : FunctionRpcRelaySide.Runtime));
        Assert.Equal(expected, error.StatusCode);
        Assert.Equal(before, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Fact]
    public async Task LinkAcceptance_PendingFailedOrStaleInitialization_CannotAcceptReplacement()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
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
        await worker.CompleteRequestAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);

        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        AcceptLinkRequest current = CreateLinkRequest("new-init");
        await InitializeLinkAsync(replacementWorker, replacementRuntime, current.InitializationId, timeout.Token);
        GrpcRpcException rejected = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, request, timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, rejected.StatusCode);
        await AcceptLinkAsync(factory, current, timeout.Token);
    }

    private static AcceptLinkRequest CreateLinkRequest(string initializationId) => new()
    {
        InitializationId = initializationId
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
            WorkerInitResponse = new()
            {
                Result = new() { Status = StatusResult.Types.Status.Success },
            }
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
