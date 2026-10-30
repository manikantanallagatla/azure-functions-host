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
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    public enum LinkAcceptanceScenario
    {
        AcceptedHistory,
        FailedInitialization,
        UnpairedSuccess,
        UnpairedFailure
    }

    [Theory]
    [InlineData(LinkAcceptanceScenario.AcceptedHistory)]
    [InlineData(LinkAcceptanceScenario.FailedInitialization)]
    [InlineData(LinkAcceptanceScenario.UnpairedSuccess)]
    [InlineData(LinkAcceptanceScenario.UnpairedFailure)]
    public async Task LinkAcceptance_InitializationHistoryAndReplacementFollowPublicLifecycleContract(
        LinkAcceptanceScenario scenario)
    {
        switch (scenario)
        {
            case LinkAcceptanceScenario.AcceptedHistory:
                await VerifyAcceptedLinkHistoryAsync();
                break;
            case LinkAcceptanceScenario.FailedInitialization:
                await VerifyFailedInitializationAllowsReplacementAsync();
                break;
            case LinkAcceptanceScenario.UnpairedSuccess:
                await VerifyUnpairedInitializationAllowsReplacementAsync(StatusResult.Types.Status.Success);
                break;
            case LinkAcceptanceScenario.UnpairedFailure:
                await VerifyUnpairedInitializationAllowsReplacementAsync(StatusResult.Types.Status.Failure);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario));
        }
    }

    private static async Task VerifyAcceptedLinkHistoryAsync()
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
        Assert.Equal(SplitWorkerLinkState.Unlinked, linked.Link.State);
        Assert.Null(linked.Link.Runtime);
        Assert.Equal(SplitWorkerStatus.Starting, linked.Status);
        using (var workerState = await ReadManagementStateAsync(management, null, timeout.Token))
        {
            Assert.Equal(
                "None",
                workerState.RootElement.GetProperty("workerPodState").GetProperty("podStatus").GetString());
        }

        await worker.WriteAsync(new StreamingMessage
        {
            RequestId = "metadata-init",
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Success } }
        }, timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        Assert.Equal(linked, await ReadSplitManagementStateAsync(management, timeout.Token));

        await worker.CompleteRequestAsync(timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        await worker.WaitForTerminationAsync(timeout.Token);
        await WaitForReleaseAsync(factory.Services.GetRequiredService<FunctionRpcRelay>(), timeout.Token);

        SplitWorkerOriginState terminated = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, terminated.Status);
        Assert.Equal(linked.Link, terminated.Link);
        await using RelayClient replacement = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        Grpc.Core.RpcException rejected = await replacement.WriteAndReadRejectionAsync(
            new StreamingMessage { WorkerInitRequest = new() { HostVersion = "replacement" } },
            timeout.Token);
        Assert.Equal(StatusCode.Unavailable, rejected.StatusCode);
    }

    private static async Task VerifyFailedInitializationAllowsReplacementAsync()
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
        await WaitForReleaseAsync(factory.Services.GetRequiredService<FunctionRpcRelay>(), timeout.Token);
        Assert.Equal(SplitWorkerStatus.Starting, (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);

        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(replacementWorker, replacementRuntime, string.Empty, timeout.Token);
    }

    private static async Task VerifyUnpairedInitializationAllowsReplacementAsync(StatusResult.Types.Status status)
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
        await WaitForReleaseAsync(factory.Services.GetRequiredService<FunctionRpcRelay>(), timeout.Token);

        await using RelayClient replacementWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient replacementRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(replacementWorker, replacementRuntime, string.Empty, timeout.Token);
    }

    private static async Task InitializeLinkAsync(
        RelayClient worker,
        RelayClient runtime,
        string initializationId,
        CancellationToken token)
    {
        await worker.WriteAsync(CreateStartStream(), token);
        await runtime.ReadAsync(token);
        await ExchangeInitializationAsync(worker, runtime, initializationId, token);
    }

    private static async Task ExchangeInitializationAsync(
        RelayClient worker,
        RelayClient runtime,
        string initializationId,
        CancellationToken token)
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
