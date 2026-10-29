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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    [Fact]
    public async Task NeverLinked_DefaultDeadline_PublishesStickyRemovalAndClosesIntake()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
        await clock.WaitForTimerAsync(TimeSpan.FromMinutes(5), timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(5) - TimeSpan.FromSeconds(1));
        Assert.Equal(initial, await ReadSplitManagementStateAsync(management, timeout.Token));

        Task<HttpResponseMessage> poll = PollSplitManagementStateAsync(management, initial, timeout.Token);
        await WaitForManagementPollAsync(factory.Services.GetRequiredService<WorkerPodStateManager>(), timeout.Token);
        clock.Advance(TimeSpan.FromMilliseconds(998.5));
        clock.FireTimerEarly();
        await clock.WaitForTimerAsync(TimeSpan.FromMilliseconds(2), timeout.Token);
        clock.Advance(TimeSpan.FromMilliseconds(2));
        using HttpResponseMessage changed = await poll;
        Assert.Equal(HttpStatusCode.OK, changed.StatusCode);
        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(SplitWorkerLinkState.Unlinked, removed.Link.State);
        Assert.Equal(initial.StartupTime, removed.StartupTime);
        Assert.Equal(initial.RevisionId + 1, removed.RevisionId);
        await AssertReadinessAsync(management, "/admin/instance/ready", HttpStatusCode.OK, timeout.Token);
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.ServiceUnavailable, timeout.Token);

        using HttpResponseMessage assignment = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
        await AssertManagementErrorAsync(assignment, HttpStatusCode.Conflict, "RemovalRequested", timeout.Token);
        await using RelayClient lateWorker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        Assert.Equal(StatusCode.Unavailable,
            (await lateWorker.WriteAndReadRejectionAsync(CreateStartStream(), timeout.Token)).StatusCode);
        GrpcRpcException lateLink = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, CreateLinkRequest("late-init"), timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, lateLink.StatusCode);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(removed, await ReadSplitManagementStateAsync(management, timeout.Token));
    }

    [Fact]
    public async Task NeverLinked_AssignmentInitRetriesAndConfigReload_DoNotResetSelectedDeadline()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock, duration: "00:02:00");
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(1));
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, "initialized-but-not-accepted", timeout.Token);
        using HttpResponseMessage assignment = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
        Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
        using HttpResponseMessage replay = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
        Assert.Equal(HttpStatusCode.NoContent, replay.StatusCode);
        GrpcRpcException rejected = await Assert.ThrowsAsync<GrpcRpcException>(
            () => AcceptLinkAsync(factory, CreateLinkRequest("wrong-initialization"), timeout.Token));
        Assert.Equal(StatusCode.FailedPrecondition, rejected.StatusCode);
        IConfiguration configuration = factory.Services.GetRequiredService<IConfiguration>();
        configuration["WorkerProxy:NeverLinkedTimeout"] = "00:30:00";
        ((IConfigurationRoot)configuration).Reload();

        clock.Advance(TimeSpan.FromSeconds(59));
        Assert.Equal(SplitWorkerStatus.Starting, (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
        clock.Advance(TimeSpan.FromSeconds(1));
        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(SplitWorkerLinkState.Unlinked, removed.Link.State);
        Assert.Equal(initial.StartupTime, removed.StartupTime);
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.ServiceUnavailable, timeout.Token);
    }

    [Fact]
    public async Task NeverLinked_AcceptedMetadataAttachment_EndsEligibilityWithoutServingPromotion()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        var request = CreateLinkRequest("accepted-metadata");
        await InitializeLinkAsync(worker, runtime, request.InitializationId, timeout.Token);
        await AcceptLinkAsync(factory, request, timeout.Token);
        SplitWorkerOriginState linked = await ReadSplitManagementStateAsync(management, timeout.Token);

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(linked, await ReadSplitManagementStateAsync(management, timeout.Token));
        Assert.Equal(SplitWorkerLinkState.Linked, linked.Link.State);
        Assert.Equal(SplitWorkerStatus.Starting, linked.Status);
        using var state = await ReadManagementStateAsync(management, null, timeout.Token);
        Assert.Equal("None", state.RootElement.GetProperty("workerPodState").GetProperty("podStatus").GetString());
    }

    [Fact]
    public async Task NeverLinked_AcceptanceRacingExpiry_HasExactlyOneWinner()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        var request = CreateLinkRequest("racing-init");
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

        await Task.WhenAll(TryAcceptAsync(), Task.Run(() => clock.Advance(TimeSpan.FromMinutes(5)), timeout.Token));
        SplitWorkerOriginState after = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(before.RevisionId + 1, after.RevisionId);
        Assert.Equal(accepted ? SplitWorkerLinkState.Linked : SplitWorkerLinkState.Unlinked, after.Link.State);
        Assert.Equal(accepted ? SplitWorkerStatus.Starting : SplitWorkerStatus.RemovalRequested, after.Status);
    }

    [Fact]
    public async Task NeverLinked_DisabledLifecycle_PreservesLegacyWorkerAfterDeadline()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock, enabled: false);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, "legacy-init", timeout.Token);
        using HttpResponseMessage assignment = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
        Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
        SplitWorkerOriginState before = await ReadSplitManagementStateAsync(management, timeout.Token);

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(before, await ReadSplitManagementStateAsync(management, timeout.Token));
        await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.OK, timeout.Token);
        using HttpResponseMessage replay = await PutManagementJsonAsync(
            management, "/admin/worker/assignment", ManagementAssignment, timeout.Token);
        Assert.Equal(HttpStatusCode.NoContent, replay.StatusCode);
    }

    private static WorkerProxyWebApplicationFactory CreateTimeoutFactory(
        LifecycleClock clock, bool enabled = true, string? duration = null)
    {
        Dictionary<string, string?> configuration = new()
        {
            ["WorkerProxy:EnableLinkLifecycle"] = enabled.ToString()
        };
        if (duration is not null)
        {
            configuration["WorkerProxy:NeverLinkedTimeout"] = duration;
        }

        return new(configuration, services => services.Replace(ServiceDescriptor.Singleton(clock.Provider)));
    }

    private sealed class LifecycleClock
    {
        private readonly Lock _sync = new();
        private readonly List<ScheduledTimer> _timers = [];
        private readonly List<TimeSpan> _requestedDelays = [];
        private TimeSpan _elapsed;

        public LifecycleClock()
        {
            Mock<TimeProvider> clock = new();
            clock.Setup(value => value.GetUtcNow()).Returns(() =>
                new DateTimeOffset(2026, 10, 28, 0, 0, 0, TimeSpan.Zero) + Elapsed);
            clock.Setup(value => value.GetTimestamp()).Returns(() => Elapsed.Ticks);
            clock.SetupGet(value => value.TimestampFrequency).Returns(TimeSpan.TicksPerSecond);
            clock.Setup(value => value.CreateTimer(
                It.IsAny<TimerCallback>(), It.IsAny<object?>(), It.IsAny<TimeSpan>(), It.IsAny<TimeSpan>()))
                .Returns((TimerCallback callback, object? state, TimeSpan due, TimeSpan _) =>
                {
                    ScheduledTimer scheduled = new(callback, state);
                    Mock<ITimer> timer = new();
                    timer.Setup(value => value.Change(It.IsAny<TimeSpan>(), It.IsAny<TimeSpan>()))
                        .Returns((TimeSpan delay, TimeSpan _) =>
                        {
                            lock (_sync)
                            {
                                scheduled.Due = delay == Timeout.InfiniteTimeSpan ? null : _elapsed + delay;
                                return true;
                            }
                        });
                    timer.Setup(value => value.Dispose()).Callback(() =>
                    {
                        lock (_sync)
                        {
                            scheduled.Due = null;
                        }
                    });
                    lock (_sync)
                    {
                        scheduled.Due = due == Timeout.InfiniteTimeSpan ? null : _elapsed + due;
                        _requestedDelays.Add(due);
                        _timers.Add(scheduled);
                    }

                    return timer.Object;
                });
            Provider = clock.Object;
        }

        public TimeProvider Provider { get; }

        private TimeSpan Elapsed
        {
            get
            {
                lock (_sync)
                {
                    return _elapsed;
                }
            }
        }

        public void Advance(TimeSpan amount)
        {
            List<ScheduledTimer> due = [];
            lock (_sync)
            {
                _elapsed += amount;
                foreach (ScheduledTimer timer in _timers)
                {
                    if (timer.Due is { } deadline && deadline <= _elapsed)
                    {
                        timer.Due = null;
                        due.Add(timer);
                    }
                }
            }

            foreach (ScheduledTimer timer in due)
            {
                timer.Callback(timer.State);
            }
        }

        public async Task WaitForTimerAsync(TimeSpan delay, CancellationToken token)
        {
            while (true)
            {
                lock (_sync)
                {
                    if (_requestedDelays.Contains(delay))
                    {
                        return;
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(10), token);
            }
        }

        public void FireTimerEarly()
        {
            ScheduledTimer timer;
            lock (_sync)
            {
                timer = _timers.Find(value => value.Due is not null)
                    ?? throw new InvalidOperationException("No active deadline timer.");
                timer.Due = null;
            }

            timer.Callback(timer.State);
        }

        private sealed class ScheduledTimer(TimerCallback callback, object? state)
        {
            public TimerCallback Callback { get; } = callback;

            public object? State { get; } = state;

            public TimeSpan? Due { get; set; }
        }
    }
}
