// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.Rpc;
using Azure.Functions.WorkerProxy.State;
using Grpc.Core;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Xunit;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    [Fact]
    public async Task LinkAcceptance_InitializedMetadataAttachment_RecordsHistoryWithoutPublishingIdentity()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);

        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        await ExchangeInitializationAsync(worker, runtime, "metadata-init", timeout.Token);

        SplitWorkerOriginState linked = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(before.RevisionId + 1, linked.RevisionId);
        Assert.Equal(before.Link, linked.Link);
        Assert.Equal(SplitWorkerLinkState.Unlinked, linked.Link.State);
        Assert.Null(linked.Link.Runtime);
        Assert.Equal(SplitWorkerStatus.Starting, linked.Status);
        using var workerState = await ReadManagementStateAsync(management, null, timeout.Token);
        Assert.Equal("None", workerState.RootElement.GetProperty("workerPodState").GetProperty("podStatus").GetString());
    }

    [Fact]
    public async Task LinkAcceptance_RepeatedInitializationIsIdempotentAndLinkedProxyCannotBeRetargeted()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, "replay-init", timeout.Token);
        SplitWorkerOriginState linked = await ReadSplitManagementStateAsync(management, timeout.Token);

        await worker.WriteAsync(new StreamingMessage
        {
            RequestId = "replay-init",
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Success } }
        }, timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        Assert.Equal(linked, await ReadSplitManagementStateAsync(management, timeout.Token));

        await worker.CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        await worker.WaitForTerminationAsync(timeout.Token);
        SplitWorkerOriginState terminated = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(linked.Link, terminated.Link);
        await using RelayClient replacement = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await replacement.WriteAsync(new StreamingMessage
        {
            WorkerInitRequest = new() { HostVersion = "test-host" }
        }, timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await replacement.WaitForTerminationAsync(timeout.Token));
    }

    [Fact]
    public async Task LinkAcceptance_PendingOrFailedInitialization_AllowsUnlinkedSessionReplacement()
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);
        await runtime.WriteAsync(new StreamingMessage
        {
            WorkerInitRequest = new() { HostVersion = "test-host" }
        }, timeout.Token);
        await worker.ReadAsync(timeout.Token);
        Assert.Equal(before, await ReadSplitManagementStateAsync(management, timeout.Token));
        await worker.WriteAsync(new StreamingMessage
        {
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Failure } }
        }, timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        Assert.Equal(before, await ReadSplitManagementStateAsync(management, timeout.Token));
        await worker.CompleteRequestAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);

        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(replacementWorker, replacementRuntime, string.Empty, timeout.Token);
    }

    [Theory]
    [InlineData(StatusResult.Types.Status.Success)]
    [InlineData(StatusResult.Types.Status.Failure)]
    public async Task LinkAcceptance_InitializationWithoutRuntime_DoesNotPreventReplacement(StatusResult.Types.Status status)
    {
        await using WorkerProxyWebApplicationFactory factory = CreateFactory();
        using CancellationTokenSource timeout = new(TestTimeout);
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await worker.WriteAsync(new StreamingMessage
        {
            WorkerInitResponse = new() { Result = new() { Status = status } }
        }, timeout.Token);
        await worker.CompleteRequestAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);

        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(replacementWorker, replacementRuntime, string.Empty, timeout.Token);
    }

    private static async Task InitializeLinkAsync(RelayClient worker, RelayClient runtime, string initializationId, CancellationToken token)
    {
        await worker.WriteAsync(CreateStartStream(), token);
        await runtime.ReadAsync(token);
        await ExchangeInitializationAsync(worker, runtime, initializationId, token);
    }

    private static async Task ExchangeInitializationAsync(RelayClient worker, RelayClient runtime, string initializationId, CancellationToken token)
    {
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
}
