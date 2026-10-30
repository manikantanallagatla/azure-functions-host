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
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Xunit;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Tests;

public partial class FunctionRpcRelayTests
{
    public enum NeverLinkedScenario
    {
        DeadlineAdmission,
        ProcessStartAndClock,
        RetryAndConfigurationReload,
        AcceptedLinkStopsDeadline,
        InitializationExpiryRace
    }

    [Theory]
    [InlineData(NeverLinkedScenario.DeadlineAdmission)]
    [InlineData(NeverLinkedScenario.ProcessStartAndClock)]
    [InlineData(NeverLinkedScenario.RetryAndConfigurationReload)]
    [InlineData(NeverLinkedScenario.AcceptedLinkStopsDeadline)]
    [InlineData(NeverLinkedScenario.InitializationExpiryRace)]
    public async Task NeverLinkedDeadline_UsesFixedProcessStartAndStickyAdmission(NeverLinkedScenario scenario)
    {
        switch (scenario)
        {
            case NeverLinkedScenario.DeadlineAdmission:
                await VerifyDeadlineAdmissionAsync();
                break;
            case NeverLinkedScenario.ProcessStartAndClock:
                await VerifyProcessStartAndClockAsync();
                break;
            case NeverLinkedScenario.RetryAndConfigurationReload:
                await VerifyRetriesAndConfigurationReloadAsync();
                break;
            case NeverLinkedScenario.AcceptedLinkStopsDeadline:
                await VerifyAcceptedLinkStopsDeadlineAsync();
                break;
            case NeverLinkedScenario.InitializationExpiryRace:
                await VerifyInitializationExpiryRaceAsync();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario));
        }
    }

    private static async Task VerifyDeadlineAdmissionAsync()
    {
        foreach ((bool explicitHttpDestination, bool workerStarted) in new[]
        {
            (false, false),
            (true, false),
            (false, true),
            (true, true)
        })
        {
            int forwardedRequests = 0;
            await using var destination = await WorkerHttpForwardingTests.StartWorkerAsync(_ =>
            {
                Interlocked.Increment(ref forwardedRequests);
                return Task.CompletedTask;
            });
            LifecycleClock clock = new();
            string? httpEndpoint = explicitHttpDestination
                ? WorkerHttpForwardingTests.GetAddress(destination).AbsoluteUri
                : null;
            await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(
                clock,
                httpEndpoint: httpEndpoint);
            using CancellationTokenSource timeout = new(TestTimeout);
            using HttpClient management = factory.CreateWorkerProxyClient();
            using HttpClient http = factory.CreateHttpForwardingClient();
            await using RelayClient? abandonedWorker = workerStarted
                ? CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token)
                : null;
            if (abandonedWorker is not null)
            {
                await abandonedWorker.WriteAsync(CreateStartStream(), timeout.Token);
                await WaitForWorkerReadyAsync(factory.Services.GetRequiredService<WorkerPodStateManager>(), timeout.Token);
                await AssertReadinessAsync(management, "/admin/worker/ready", HttpStatusCode.OK, timeout.Token);
            }

            using HttpResponseMessage beforeDeadline = await http.GetAsync("/before-deadline", timeout.Token);
            Assert.Equal(
                explicitHttpDestination ? HttpStatusCode.OK : HttpStatusCode.ServiceUnavailable,
                beforeDeadline.StatusCode);
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
            SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(changed, timeout.Token);
            Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
            Assert.Equal(SplitWorkerLinkState.Unlinked, removed.Link.State);
            Assert.Equal(initial.StartupTime, removed.StartupTime);
            Assert.Equal(initial.RevisionId + 1, removed.RevisionId);
            await AssertReadinessAsync(management, "/admin/instance/ready", HttpStatusCode.OK, timeout.Token);
            await AssertReadinessAsync(
                management,
                "/admin/worker/ready",
                HttpStatusCode.ServiceUnavailable,
                timeout.Token);
            using HttpResponseMessage afterDeadline = await http.GetAsync("/after-deadline", timeout.Token);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, afterDeadline.StatusCode);
            Assert.Equal(explicitHttpDestination ? 1 : 0, Volatile.Read(ref forwardedRequests));

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
            await using RelayClient lateWorker = CreateClient(
                factory,
                FunctionRpcRelaySide.Worker,
                timeout.Token);
            Assert.Equal(
                StatusCode.Unavailable,
                (await lateWorker.WriteAndReadRejectionAsync(CreateStartStream(), timeout.Token)).StatusCode);
            await using RelayClient lateRuntime = CreateClient(
                factory,
                FunctionRpcRelaySide.Runtime,
                timeout.Token);
            GrpcRpcException lateLink = await lateRuntime.WriteAndReadRejectionAsync(
                new StreamingMessage { WorkerInitRequest = new() { HostVersion = "late-runtime" } },
                timeout.Token);
            Assert.Equal(StatusCode.Unavailable, lateLink.StatusCode);
            clock.Advance(TimeSpan.FromHours(1));
            Assert.Equal(removed, await ReadSplitManagementStateAsync(management, timeout.Token));
        }
    }

    private static async Task VerifyProcessStartAndClockAsync()
    {
        foreach ((int constructionSeconds, int durationSeconds, int utcAdjustmentMinutes) in new[]
        {
            (0, 300, 0),
            (30, 300, 0),
            (300, 300, 0),
            (301, 300, 0),
            (30, 120, 0),
            (120, 120, 0),
            (30, 300, 1440),
            (30, 300, -1440)
        })
        {
            LifecycleClock clock = new();
            DateTimeOffset processStartup = clock.Provider.GetUtcNow();
            await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(
                clock,
                duration: TimeSpan.FromSeconds(durationSeconds).ToString("c"));
            clock.Advance(TimeSpan.FromSeconds(constructionSeconds));
            clock.AdjustUtc(TimeSpan.FromMinutes(utcAdjustmentMinutes));
            using CancellationTokenSource timeout = new(TestTimeout);
            using HttpClient management = factory.CreateWorkerProxyClient();
            SplitWorkerOriginState state = await ReadSplitManagementStateAsync(management, timeout.Token);
            Assert.Equal(processStartup, state.StartupTime);

            if (constructionSeconds < durationSeconds)
            {
                Assert.Equal(SplitWorkerStatus.Starting, state.Status);
                clock.Advance(
                    TimeSpan.FromSeconds(durationSeconds - constructionSeconds) -
                    TimeSpan.FromMilliseconds(1));
                Assert.Equal(state, await ReadSplitManagementStateAsync(management, timeout.Token));
                clock.Advance(TimeSpan.FromMilliseconds(1));
                state = await ReadSplitManagementStateAsync(management, timeout.Token);
            }

            Assert.Equal(SplitWorkerStatus.RemovalRequested, state.Status);
            Assert.Equal(processStartup, state.StartupTime);
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
            await AssertReadinessAsync(management, "/admin/instance/ready", HttpStatusCode.OK, timeout.Token);
            clock.Advance(TimeSpan.FromHours(1));
            Assert.Equal(state, await ReadSplitManagementStateAsync(management, timeout.Token));
        }
    }

    private static async Task VerifyRetriesAndConfigurationReloadAsync()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock, duration: "00:02:00");
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        SplitWorkerOriginState initial = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromMinutes(1));
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await worker.WriteAsync(CreateStartStream(), timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        StreamingMessage initialization = new() { WorkerInitRequest = new() { HostVersion = "test-host" } };
        await runtime.WriteAsync(initialization, timeout.Token);
        await worker.ReadAsync(timeout.Token);
        await worker.WriteAsync(new StreamingMessage
        {
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Failure } }
        }, timeout.Token);
        await runtime.ReadAsync(timeout.Token);
        using HttpResponseMessage assignment = await PutManagementJsonAsync(
            management,
            "/admin/worker/assignment",
            ManagementAssignment,
            timeout.Token);
        Assert.Equal(HttpStatusCode.Created, assignment.StatusCode);
        using HttpResponseMessage replay = await PutManagementJsonAsync(
            management,
            "/admin/worker/assignment",
            ManagementAssignment,
            timeout.Token);
        Assert.Equal(HttpStatusCode.NoContent, replay.StatusCode);
        await runtime.WriteAsync(initialization, timeout.Token);
        await worker.ReadAsync(timeout.Token);
        IConfiguration configuration = factory.Services.GetRequiredService<IConfiguration>();
        configuration["WorkerProxy:NeverLinkedTimeout"] = "00:30:00";
        ((IConfigurationRoot)configuration).Reload();

        clock.Advance(TimeSpan.FromSeconds(59));
        Assert.Equal(
            SplitWorkerStatus.Starting,
            (await ReadSplitManagementStateAsync(management, timeout.Token)).Status);
        clock.Advance(TimeSpan.FromSeconds(1));
        SplitWorkerOriginState removed = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(SplitWorkerLinkState.Unlinked, removed.Link.State);
        Assert.Equal(initial.StartupTime, removed.StartupTime);
        await AssertReadinessAsync(
            management,
            "/admin/worker/ready",
            HttpStatusCode.ServiceUnavailable,
            timeout.Token);
        await worker.WriteAsync(new StreamingMessage
        {
            WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Success } }
        }, timeout.Token);
        Assert.Equal(StatusCode.Unavailable, await runtime.WaitForTerminationAsync(timeout.Token));
        Assert.Equal(StatusCode.Unavailable, await worker.WaitForTerminationAsync(timeout.Token));
        Assert.False(factory.Services.GetRequiredService<WorkerPodStateManager>().State.HasAcceptedLink);
    }

    private static async Task VerifyAcceptedLinkStopsDeadlineAsync()
    {
        LifecycleClock clock = new();
        await using WorkerProxyWebApplicationFactory factory = CreateTimeoutFactory(clock);
        using CancellationTokenSource timeout = new(TestTimeout);
        using HttpClient management = factory.CreateWorkerProxyClient();
        await using RelayClient worker = CreateClient(factory, FunctionRpcRelaySide.Worker, timeout.Token);
        await using RelayClient runtime = CreateClient(factory, FunctionRpcRelaySide.Runtime, timeout.Token);
        await InitializeLinkAsync(worker, runtime, string.Empty, timeout.Token);
        SplitWorkerOriginState linked = await ReadSplitManagementStateAsync(management, timeout.Token);

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(linked, await ReadSplitManagementStateAsync(management, timeout.Token));
        Assert.Equal(SplitWorkerLinkState.Unlinked, linked.Link.State);
        Assert.Null(linked.Link.Runtime);
        Assert.Equal(SplitWorkerStatus.Starting, linked.Status);
        using (var state = await ReadManagementStateAsync(management, null, timeout.Token))
        {
            Assert.Equal(
                "None",
                state.RootElement.GetProperty("workerPodState").GetProperty("podStatus").GetString());
        }

        await worker.CompleteRequestAsync(timeout.Token);
        await runtime.WaitForTerminationAsync(timeout.Token);
        await worker.WaitForTerminationAsync(timeout.Token);
        SplitWorkerOriginState disconnected = await ReadSplitManagementStateAsync(management, timeout.Token);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal(disconnected, await ReadSplitManagementStateAsync(management, timeout.Token));
        Assert.Equal(SplitWorkerStatus.RemovalRequested, disconnected.Status);
    }

    private static async Task VerifyInitializationExpiryRaceAsync()
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
        bool initialized = false;

        async Task TryInitializeAsync()
        {
            try
            {
                await worker.WriteAsync(new StreamingMessage
                {
                    WorkerInitResponse = new() { Result = new() { Status = StatusResult.Types.Status.Success } }
                }, timeout.Token);
                Assert.NotNull((await runtime.ReadAsync(timeout.Token)).WorkerInitResponse);
                initialized = true;
            }
            catch (GrpcRpcException error) when (error.StatusCode == StatusCode.Unavailable)
            {
                await runtime.WaitForTerminationAsync(timeout.Token);
                await worker.WaitForTerminationAsync(timeout.Token);
            }
        }

        await Task.WhenAll(
            TryInitializeAsync(),
            Task.Run(() => clock.Advance(TimeSpan.FromMinutes(5)), timeout.Token));
        SplitWorkerOriginState after = await ReadSplitManagementStateAsync(management, timeout.Token);
        Assert.Equal(before.RevisionId + (initialized ? 1 : 2), after.RevisionId);
        Assert.Equal(SplitWorkerLinkState.Unlinked, after.Link.State);
        Assert.Null(after.Link.Runtime);
        Assert.Equal(
            initialized ? SplitWorkerStatus.Starting : SplitWorkerStatus.RemovalRequested,
            after.Status);
        Assert.Equal(
            initialized,
            factory.Services.GetRequiredService<WorkerPodStateManager>().State.HasAcceptedLink);
    }

    private static WorkerProxyWebApplicationFactory CreateTimeoutFactory(
        LifecycleClock clock,
        string? duration = null,
        string? httpEndpoint = null)
    {
        Dictionary<string, string?> configuration = [];
        if (duration is not null)
        {
            configuration["WorkerProxy:NeverLinkedTimeout"] = duration;
        }

        if (httpEndpoint is not null)
        {
            configuration["WorkerProxy:WorkerHttpEndpoint"] = httpEndpoint;
        }

        WorkerProxyStartupTime startupTime = WorkerProxyStartupTime.Capture(clock.Provider);
        return new(configuration, services =>
        {
            services.Replace(ServiceDescriptor.Singleton(clock.Provider));
            services.Replace(ServiceDescriptor.Singleton(startupTime));
        });
    }

    private sealed class LifecycleClock
    {
        private readonly Lock _sync = new();
        private readonly List<ScheduledTimer> _timers = [];
        private readonly List<TimeSpan> _requestedDelays = [];
        private TimeSpan _elapsed;
        private TimeSpan _utcAdjustment;

        public LifecycleClock()
        {
            Mock<TimeProvider> clock = new();
            clock.Setup(value => value.GetUtcNow()).Returns(() =>
            {
                lock (_sync)
                {
                    return new DateTimeOffset(2026, 10, 28, 0, 0, 0, TimeSpan.Zero) +
                        _elapsed +
                        _utcAdjustment;
                }
            });
            clock.Setup(value => value.GetTimestamp()).Returns(() => Elapsed.Ticks);
            clock.SetupGet(value => value.TimestampFrequency).Returns(TimeSpan.TicksPerSecond);
            clock.Setup(value => value.CreateTimer(
                    It.IsAny<TimerCallback>(),
                    It.IsAny<object?>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<TimeSpan>()))
                .Returns((TimerCallback callback, object? state, TimeSpan due, TimeSpan _) =>
                {
                    ScheduledTimer scheduled = new(callback, state);
                    Mock<ITimer> timer = new();
                    timer.Setup(value => value.Change(It.IsAny<TimeSpan>(), It.IsAny<TimeSpan>()))
                        .Returns((TimeSpan delay, TimeSpan _) =>
                        {
                            lock (_sync)
                            {
                                scheduled.Due = delay == Timeout.InfiniteTimeSpan
                                    ? null
                                    : _elapsed + delay;
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

        public void AdjustUtc(TimeSpan amount)
        {
            lock (_sync)
            {
                _utcAdjustment += amount;
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
