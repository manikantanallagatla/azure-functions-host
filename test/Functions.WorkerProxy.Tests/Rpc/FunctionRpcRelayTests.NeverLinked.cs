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
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    [Fact]
    public async Task NeverLinkedDeadline_PublishesStickyRemovalAndClosesPodAdmission()
    {
        ControlledTimeProvider clock = new();
        int forwardedRequests = 0;
        await using WebApplication workerApp = await WorkerHttpForwardingTests.StartWorkerAsync(context =>
        {
            Interlocked.Increment(ref forwardedRequests);
            context.Response.StatusCode = StatusCodes.Status202Accepted;
            return Task.CompletedTask;
        });
        await using WorkerProxyWebApplicationFactory factory =
            CreateTimeoutFactory(clock, WorkerHttpForwardingTests.GetAddress(workerApp));
        WorkerPodStateManager manager = factory.Services.GetRequiredService<WorkerPodStateManager>();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        using HttpClient forwarding = factory.CreateHttpForwardingClient();
        await clock.WaitForTimerAsync(timeout.Token);

        SplitWorkerOriginState removed;
        await using (RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token))
        {
            await worker.WriteAsync(CreateStartStream(), timeout.Token);
            await WaitForWorkerReadyAsync(manager, timeout.Token);
            SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
            await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.OK, timeout.Token);
            using (HttpResponseMessage response = await forwarding.GetAsync("/before-removal", timeout.Token))
            {
                Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            }

            clock.Advance(TimeSpan.FromMinutes(5));
            await clock.FireTimerAsync(timeout.Token);
            await WaitForRemovalAsync(manager, timeout.Token);
            removed = await ReadSplitManagementStateAsync(management, timeout.Token);

            Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
            Assert.True(removed.IsUnhealthy);
            Assert.Equal(initial.RevisionId + 1, removed.RevisionId);
            await AssertReadinessAsync(
                management,
                "/admin/worker/ready",
                HttpStatusCode.ServiceUnavailable,
                timeout.Token);
            using (HttpResponseMessage response = await forwarding.GetAsync("/after-removal", timeout.Token))
            {
                Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            }

            Assert.Equal(1, forwardedRequests);

            using HttpResponseMessage repeatedResponse =
                await PollSplitManagementStateAsync(management, removed, timeout.Token);
            Assert.Equal(removed, await ReadSplitManagementStateAsync(repeatedResponse, timeout.Token));

            await worker.CompleteRequestAsync(timeout.Token);
            Assert.Equal(StatusCode.Unavailable, await worker.WaitForTerminationAsync(timeout.Token));
        }

        await using RelayClient lateRuntime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        GrpcRpcException runtimeRejection = await lateRuntime.WriteAndReadRejectionAsync(
            CreateMessage("late-runtime"),
            timeout.Token);
        Assert.Equal(StatusCode.Unavailable, runtimeRejection.StatusCode);

        await using RelayClient lateWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        GrpcRpcException workerRejection = await lateWorker.WriteAndReadRejectionAsync(
            CreateStartStream("late-worker"),
            timeout.Token);
        Assert.Equal(StatusCode.Unavailable, workerRejection.StatusCode);

        Assert.Equal(
            SplitWorkerStatus.RemovalRequested,
            (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
    }

    [Fact]
    public async Task RuntimeConnectionBeforeDeadline_PreventsNeverLinkedRemoval()
    {
        ControlledTimeProvider clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        FunctionRpcRelay relay = factory.Services.GetRequiredService<FunctionRpcRelay>();
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await clock.WaitForTimerAsync(timeout.Token);
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);

        await runtime.WriteAsync(CreateMessage("runtime-link"), timeout.Token);
        await WaitForAttachmentAsync(relay, FunctionRpcRelaySide.Runtime, timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(30));
        await clock.FireTimerAsync(timeout.Token);

        Assert.Equal(initial, await ReadSplitManagementStateAsync(management, timeout.Token));
        Assert.False(factory.Services.GetRequiredService<WorkerPodStateManager>().State.RemovalRequested);
    }

    private static WorkerProxyWebApplicationFactory CreateTimeoutFactory(
        ControlledTimeProvider clock,
        Uri? workerHttpEndpoint = null)
    {
        Dictionary<string, string?> configuration = new()
        {
            [$"{WorkerProxyOptions.SectionName}:{nameof(WorkerProxyOptions.NeverLinkedTimeout)}"] = "00:05:00"
        };
        if (workerHttpEndpoint is not null)
        {
            configuration[$"{WorkerProxyOptions.SectionName}:{nameof(WorkerProxyOptions.WorkerHttpEndpoint)}"] =
                workerHttpEndpoint.AbsoluteUri;
        }

        WorkerProxyStartupTime startupTime = WorkerProxyStartupTime.Capture(clock);

        return new(configuration, services =>
            services.Replace(ServiceDescriptor.Singleton(startupTime)));
    }

    private static async Task WaitForRemovalAsync(
        WorkerPodStateManager manager,
        CancellationToken cancellationToken)
    {
        while (!manager.State.RemovalRequested)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
        }
    }

    private sealed class ControlledTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _origin = new(2026, 10, 28, 0, 0, 0, TimeSpan.Zero);
        private readonly TaskCompletionSource<ManualTimer> _timerCreated =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private long _elapsedTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() =>
            _origin + TimeSpan.FromTicks(Interlocked.Read(ref _elapsedTicks));

        public override long GetTimestamp() => Interlocked.Read(ref _elapsedTicks);

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            ArgumentNullException.ThrowIfNull(callback);
            ManualTimer timer = new(callback, state);
            _timerCreated.TrySetResult(timer);
            return timer;
        }

        public void Advance(TimeSpan amount) => Interlocked.Add(ref _elapsedTicks, amount.Ticks);

        public Task WaitForTimerAsync(CancellationToken cancellationToken) =>
            _timerCreated.Task.WaitAsync(cancellationToken);

        public async Task FireTimerAsync(CancellationToken cancellationToken)
        {
            ManualTimer timer = await _timerCreated.Task.WaitAsync(cancellationToken);
            timer.Fire();
        }

        private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
        {
            private int _disposed;

            public bool Change(TimeSpan dueTime, TimeSpan period) => Volatile.Read(ref _disposed) == 0;

            public void Dispose() => Interlocked.Exchange(ref _disposed, 1);

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }

            public void Fire()
            {
                if (Volatile.Read(ref _disposed) == 0)
                {
                    callback(state);
                }
            }
        }
    }
}
