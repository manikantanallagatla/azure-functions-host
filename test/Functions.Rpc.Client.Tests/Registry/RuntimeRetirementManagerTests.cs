// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.HostState;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace Azure.Functions.Rpc.Client.Tests;

public sealed class RuntimeRetirementManagerTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task RequestRetirementAsync_RejectsStaleThenOrdersAcknowledgedRetirementOnce()
    {
        TestRetirementWorkerManager workerManager = new();
        using RetirementHostStateHandler handler = new(blockFirstDrainingResponse: true);
        AppServerHostStateClient client = CreateClient(workerManager, handler);
        RuntimeRetirementManager manager = new(
            workerManager,
            workerManager,
            client,
            NullLogger<RuntimeRetirementManager>.Instance);
        SplitScriptHostActivationFaulted staleCondition = new(
            RuntimeIncarnation.Create(),
            new InvalidOperationException("stale activation"));
        SplitScriptHostActivationFaulted condition = new(
            workerManager.Incarnation,
            new InvalidOperationException("activation failed"));

        await manager.RequestRetirementAsync(staleCondition);
        Assert.Equal(0, workerManager.CloseAdmissionCallCount);
        Assert.Empty(handler.PublishedStates);

        Task retirement = manager.RequestRetirementAsync(condition);
        await handler.DrainingRequestObserved.Task.WaitAsync(TestTimeout);

        Assert.Equal(1, workerManager.CloseAdmissionCallCount);
        Assert.Equal(0, workerManager.DrainCallCount);
        Assert.Collection(
            handler.PublishedStates,
            state =>
            {
                Assert.Null(state.Status);
                Assert.Null(state.AcquireClosed);
                Assert.Equal(workerManager.Incarnation.Value, state.RuntimeIncarnationId);
            },
            state =>
            {
                Assert.Equal(AppServerHostStateClient.DrainingStatus, state.Status);
                Assert.True(state.AcquireClosed);
                Assert.Equal(0, state.HttpCapacity);
                Assert.Equal(workerManager.Incarnation.Value, state.RuntimeIncarnationId);
            });
        Assert.True(handler.PublishedStates[1].CreatedTime > handler.PublishedStates[0].CreatedTime);

        handler.AllowDrainingResponse.TrySetResult();
        await workerManager.DrainStarted.Task.WaitAsync(TestTimeout);
        Assert.Equal(1, workerManager.DrainCallCount);

        workerManager.CompleteDrain();
        await handler.FallbackRequestObserved.Task.WaitAsync(TestTimeout);
        Assert.False(retirement.IsCompleted);
        Assert.DoesNotContain(
            handler.PublishedStates,
            state => string.Equals(
                state.Status,
                AppServerHostStateClient.RemovalRequestedStatus,
                StringComparison.Ordinal));
        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            workerManager.AcknowledgeWorkerRemoval(workerManager.FallbackAction));
        await retirement.WaitAsync(TestTimeout);
        await manager.RequestRetirementAsync(condition);

        Assert.Equal(1, workerManager.CloseAdmissionCallCount);
        Assert.Equal(1, workerManager.DrainCallCount);
        Assert.Collection(
            handler.PublishedStates,
            _ => { },
            _ => { },
            state =>
            {
                Assert.Equal(AppServerHostStateClient.DrainingStatus, state.Status);
                Assert.True(state.AcquireClosed);
                Assert.Equal(0, state.HttpCapacity);
                Assert.Equal(0, state.LinkedWorkerCount);
                Assert.Equal(workerManager.FallbackAction, Assert.Single(state.OutstandingWorkerRemovals));
            },
            state =>
            {
                Assert.Equal(AppServerHostStateClient.DrainingStatus, state.Status);
                Assert.True(state.AcquireClosed);
                Assert.Equal(0, state.HttpCapacity);
                Assert.Equal(0, state.LinkedWorkerCount);
                Assert.Empty(state.OutstandingWorkerRemovals);
            },
            removal =>
            {
                Assert.Equal(AppServerHostStateClient.RemovalRequestedStatus, removal.Status);
                Assert.True(removal.AcquireClosed);
                Assert.Equal(0, removal.HttpCapacity);
                Assert.Equal(0, removal.LinkedWorkerCount);
                Assert.Empty(removal.OutstandingWorkerRemovals);
                Assert.Equal(workerManager.Incarnation.Value, removal.RuntimeIncarnationId);
            });
    }

    private static AppServerHostStateClient CreateClient(
        TestRetirementWorkerManager workerManager,
        RetirementHostStateHandler handler)
    {
        HttpClient httpClient = new(handler);
        Mock<IHttpClientFactory> httpClientFactory = new();
        httpClientFactory
            .Setup(factory => factory.CreateClient(AppServerHostStateClient.HttpClientName))
            .Returns(httpClient);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                [EnvironmentSettingNames.ContainerName] = "test-container",
                [EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
            })
            .Build();

        return new(
            httpClientFactory.Object,
            workerManager,
            workerManager,
            TimeProvider.System,
            configuration);
    }

    private sealed class TestRetirementWorkerManager :
        IHttpWorkerScaleInManager,
        IRuntimeRetirementWorkerManager
    {
        private readonly Lock _stateLock = new();
        private TaskCompletionSource<long> _stateChanged =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private HttpWorkerScaleInState _state = new(
            DateTimeOffset.UtcNow,
            16,
            1,
            0);

        public int CloseAdmissionCallCount { get; private set; }

        public TaskCompletionSource DrainStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int DrainCallCount { get; private set; }

        public FunctionsSplitWorkerRemovalAction FallbackAction { get; } = new(
            new(
                "test-namespace",
                "worker-pod",
                Guid.NewGuid(),
                "test-legion-stamp",
                FunctionsSplitPodIdentity.WorkerPodType,
                new("test-app", 123, "test-antares-stamp")),
            "RuntimeUnlink");

        public RuntimeIncarnation Incarnation { get; } = RuntimeIncarnation.Create();

        public bool IsRetirementRequested { get; private set; }

        public HttpWorkerScaleInState Current
        {
            get
            {
                lock (_stateLock)
                {
                    return _state;
                }
            }
        }

        public long Version => Current.Version;

        private TaskCompletionSource<RuntimeRetirementDrainResult> DrainCompletion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool CancelReservation(HttpWorkerScaleInReservation reservation) => false;

        public WorkerRemovalAcknowledgementResult AcknowledgeWorkerRemoval(
            FunctionsSplitWorkerRemovalAction action)
        {
            lock (_stateLock)
            {
                if (!_state.OutstandingWorkerRemovals.Contains(action))
                {
                    return WorkerRemovalAcknowledgementResult.NotFound;
                }

                _state = new(
                    _state.CreatedTime.AddTicks(1),
                    0,
                    0,
                    _state.Version + 1);
                TaskCompletionSource<long> changed = _stateChanged;
                _stateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
                changed.TrySetResult(_state.Version);
                return WorkerRemovalAcknowledgementResult.Accepted;
            }
        }

        public void CompleteDrain()
        {
            lock (_stateLock)
            {
                _state = new(
                    _state.CreatedTime.AddTicks(1),
                    0,
                    0,
                    _state.Version + 1)
                {
                    OutstandingWorkerRemovals = [FallbackAction],
                };
                TaskCompletionSource<long> changed = _stateChanged;
                _stateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
                changed.TrySetResult(_state.Version);
            }

            DrainCompletion.TrySetResult(new(true, null));
        }

        public Task<RuntimeRetirementDrainResult> DrainAllWorkersAsync(
            RuntimeIncarnation incarnation,
            CancellationToken cancellationToken)
        {
            Assert.Equal(Incarnation, incarnation);
            DrainCallCount++;
            DrainStarted.TrySetResult();
            return DrainCompletion.Task.WaitAsync(cancellationToken);
        }

        public bool TryCloseAdmission(SplitScriptHostActivationFaulted condition)
        {
            CloseAdmissionCallCount++;
            if (condition.Incarnation != Incarnation)
            {
                return false;
            }

            lock (_stateLock)
            {
                IsRetirementRequested = true;
                _state = new(
                    _state.CreatedTime.AddTicks(1),
                    0,
                    _state.LinkedWorkerCount,
                    _state.Version + 1);
                TaskCompletionSource<long> changed = _stateChanged;
                _stateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
                changed.TrySetResult(_state.Version);
            }

            return true;
        }

        public bool TryCommitReservation(
            HttpWorkerScaleInReservation reservation,
            out Task<bool> drainCompletion)
        {
            drainCompletion = null;
            return false;
        }

        public HttpWorkerScaleInReservation TryReserveIdleWorker(TimeSpan minimumIdleDuration) => null;

        public Task<long> WaitForChangeAsync(long version, CancellationToken cancellationToken = default)
        {
            lock (_stateLock)
            {
                return version != _state.Version
                    ? Task.FromResult(_state.Version)
                    : _stateChanged.Task.WaitAsync(cancellationToken);
            }
        }
    }

    private sealed class RetirementHostStateHandler(bool blockFirstDrainingResponse = false) : HttpMessageHandler
    {
        private bool _drainingResponseBlocked;

        public TaskCompletionSource AllowDrainingResponse { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource DrainingRequestObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource FallbackRequestObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<AppServerHostStateClient.HostStateRequest> PublishedStates { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            string json = await request.Content!.ReadAsStringAsync(cancellationToken);
            AppServerHostStateClient.HostStateRequest state =
                JsonSerializer.Deserialize(json, AppServerHostStateJsonContext.Default.HostStateRequest);
            Assert.NotNull(state);
            PublishedStates.Add(state);
            Assert.Equal("test-container", Assert.Single(request.Headers.GetValues(ScriptConstants.ContainerInstanceHeader)));
            Assert.Equal(
                state.Status is null
                    ? AppServerHostStateClient.CapacityFenceRequired
                    : AppServerHostStateClient.LifecycleCapacityFenceRequired,
                Assert.Single(request.Headers.GetValues(AppServerHostStateClient.CapacityFenceHeader)));

            if (string.Equals(state.Status, AppServerHostStateClient.DrainingStatus, StringComparison.Ordinal) &&
                blockFirstDrainingResponse &&
                !_drainingResponseBlocked)
            {
                _drainingResponseBlocked = true;
                DrainingRequestObserved.TrySetResult();
                await AllowDrainingResponse.Task.WaitAsync(cancellationToken);
            }

            if (string.Equals(state.Status, AppServerHostStateClient.DrainingStatus, StringComparison.Ordinal) &&
                state.OutstandingWorkerRemovals.Count != 0)
            {
                FallbackRequestObserved.TrySetResult();
            }

            HttpResponseMessage response = new(HttpStatusCode.OK);
            response.Headers.Add(
                AppServerHostStateClient.CapacityFenceHeader,
                AppServerHostStateClient.CapacityFenceApplied);
            return response;
        }
    }
}
