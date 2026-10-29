// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

#nullable enable

using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Azure.Functions.Rpc.Client.Tests;

public sealed partial class WorkerChannelRegistryTests
{
    [Theory]
    [InlineData(StatusCode.OK)]
    [InlineData(StatusCode.Unavailable)]
    public async Task UnlinkAsync_GrpcPeerCompletion_WaitsWithoutWithdrawingOtherWorkers(StatusCode closeStatus)
    {
        using var timeout = new CancellationTokenSource(GrpcTestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        var worker = server.AddWorker(GrpcWorkerId);
        worker.HoldCloseResponse();
        server.AddWorker("other-worker");
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateGrpcRegistry(clientFactory);
        await registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token);
        WorkerLinkResult other = await registry.LinkAsync("other-worker", server.Endpoint, timeout.Token);

        Task<bool> unlink = registry.UnlinkAsync(GrpcWorkerId, timeout.Token);
        await worker.RequestsCompleted.WaitAsync(timeout.Token);
        Assert.False(unlink.IsCompleted);
        Assert.False(registry.TryGetInitializedChannel(GrpcWorkerId, out _));
        Assert.Same(other.Channel, Assert.Single(registry.GetInitializedChannels()));
        WorkerLinkException replay = await Assert.ThrowsAsync<WorkerLinkException>(
            () => registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token));
        Assert.Equal(WorkerLinkFailureReason.WorkerTerminated, replay.Reason);

        worker.CompleteClose(new Status(closeStatus, "A FunctionRpc relay peer closed its stream."));
        Assert.True(await unlink.WaitAsync(timeout.Token));
        await worker.Disconnected.WaitAsync(timeout.Token);
        Assert.False(await registry.UnlinkAsync(GrpcWorkerId, timeout.Token));
        Assert.Same(other.Channel, Assert.Single(registry.GetInitializedChannels()));
        Assert.Equal(2, server.StreamCount);
    }

    [Fact]
    public async Task UnlinkAsync_GrpcUnresponsivePeer_ReportsTimeoutAndReleasesLocalResources()
    {
        using var timeout = new CancellationTokenSource(GrpcTestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        var worker = server.AddWorker(GrpcWorkerId);
        worker.HoldCloseResponse();
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateGrpcRegistry(clientFactory);
        await registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token);

        Task<bool> unlink = registry.UnlinkAsync(GrpcWorkerId, timeout.Token);
        await worker.RequestsCompleted.WaitAsync(timeout.Token);
        await Assert.ThrowsAsync<TimeoutException>(() => unlink.WaitAsync(timeout.Token));
        await worker.Disconnected.WaitAsync(timeout.Token);
        Assert.Empty(registry.GetInitializedChannels());
        Assert.False(await registry.UnlinkAsync(GrpcWorkerId, timeout.Token));
        WorkerLinkException replay = await Assert.ThrowsAsync<WorkerLinkException>(
            () => registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token));
        Assert.Equal(WorkerLinkFailureReason.WorkerTerminated, replay.Reason);

        server.AddWorker("replacement-worker");
        WorkerLinkResult replacement = await registry.LinkAsync("replacement-worker", server.Endpoint, timeout.Token);
        Assert.Same(replacement.Channel, Assert.Single(registry.GetInitializedChannels()));
    }

    [Fact]
    public async Task UnlinkAsync_GrpcCallerCancellation_AfterCloseBeginsStillDisposesChannel()
    {
        using var timeout = new CancellationTokenSource(GrpcTestTimeout);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        var worker = server.AddWorker(GrpcWorkerId);
        worker.HoldCloseResponse();
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateGrpcRegistry(clientFactory);
        await registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token);

        Task<bool> unlink = registry.UnlinkAsync(GrpcWorkerId, cancellation.Token);
        await worker.RequestsCompleted.WaitAsync(timeout.Token);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => unlink.WaitAsync(timeout.Token));
        await worker.Disconnected.WaitAsync(timeout.Token);
        Assert.Empty(registry.GetInitializedChannels());
        Assert.False(await registry.UnlinkAsync(GrpcWorkerId, timeout.Token));
    }

    [Fact]
    public async Task UnlinkAsync_GrpcPreCanceledCaller_DoesNotCloseExistingWorker()
    {
        using var timeout = new CancellationTokenSource(GrpcTestTimeout);
        using var cancellation = new CancellationTokenSource();
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        var worker = server.AddWorker(GrpcWorkerId);
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateGrpcRegistry(clientFactory);
        WorkerLinkResult linked = await registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => registry.UnlinkAsync(GrpcWorkerId, cancellation.Token));
        Assert.Same(linked.Channel, Assert.Single(registry.GetInitializedChannels()));
        Assert.False(worker.RequestsCompleted.IsCompleted);
        Assert.False(worker.Disconnected.IsCompleted);
        Assert.True(await registry.UnlinkAsync(GrpcWorkerId, timeout.Token));
        await worker.RequestsCompleted.WaitAsync(timeout.Token);
        await worker.Disconnected.WaitAsync(timeout.Token);
    }

    [Fact]
    public async Task UnlinkAsync_GrpcRegistryShutdown_AbortsPendingCloseAndOtherChannels()
    {
        using var timeout = new CancellationTokenSource(GrpcTestTimeout);
        await using FakeWorkerProxyGrpcServer server = await FakeWorkerProxyGrpcServer.StartAsync(timeout.Token);
        var worker = server.AddWorker(GrpcWorkerId);
        worker.HoldCloseResponse();
        var other = server.AddWorker("other-worker");
        other.HoldCloseResponse();
        await using RpcClientFactory clientFactory = new(NullLogger<RpcClientFactory>.Instance);
        await using WorkerChannelRegistry registry = CreateGrpcRegistry(clientFactory);
        await registry.LinkAsync(GrpcWorkerId, server.Endpoint, timeout.Token);
        await registry.LinkAsync("other-worker", server.Endpoint, timeout.Token);

        Task<bool> unlink = registry.UnlinkAsync(GrpcWorkerId, timeout.Token);
        await worker.RequestsCompleted.WaitAsync(timeout.Token);
        await registry.DisposeAsync().AsTask().WaitAsync(timeout.Token);
        Assert.False(await unlink.WaitAsync(timeout.Token));
        await Task.WhenAll(worker.Disconnected, other.Disconnected).WaitAsync(timeout.Token);
        Assert.Empty(registry.GetInitializedChannels());
    }
}
