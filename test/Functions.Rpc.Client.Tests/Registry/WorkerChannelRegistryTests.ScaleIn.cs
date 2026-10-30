// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.HostState;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Azure.WebJobs.Script.Http;
using Microsoft.Azure.WebJobs.Script.ManagedDependencies;
using Microsoft.Azure.WebJobs.Script.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace Azure.Functions.Rpc.Client.Tests;

public sealed partial class WorkerChannelRegistryTests
{
    private static readonly Uri HttpProxyEndpoint = new("http://worker-proxy:8080");

    [Theory]
    [InlineData(HttpStatusCode.OK)]
    [InlineData(HttpStatusCode.BadRequest)]
    public async Task RunScaleInPassAsync_AppServerDoesNotApplyProjectedCapacity_RestoresInvocationAdmission(
        HttpStatusCode projectedStatus)
    {
        ManualTimeProvider timeProvider = new();
        (RegistryHarness harness, ChannelControl worker, FunctionMetadata function) =
            await CreateIdleHttpWorkerAsync(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        SequenceHostStateHandler handler = new(projectedStatus);
        using HttpClient httpClient = new(handler);
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
        IHttpWorkerScaleInManager scaleInManager = registry;
        IRuntimeRetirementWorkerManager retirementWorkerManager = registry;
        AppServerHostStateClient client = new(
            httpClientFactory.Object,
            scaleInManager,
            retirementWorkerManager,
            timeProvider,
            configuration);
        HttpOnDemandWorkerScaleInService service = new(
            scaleInManager,
            retirementWorkerManager,
            client,
            timeProvider,
            NullLogger<HttpOnDemandWorkerScaleInService>.Instance);

        await service.RunScaleInPassAsync(CancellationToken.None);

        Assert.Equal([16L, 0L, 16L], handler.PublishedStates.ConvertAll(state => state.HttpCapacity));
        Assert.Equal(16, scaleInManager.Current.HttpCapacity);
        using RpcClientFunctionInvocationDispatcher dispatcher = new(
            registry,
            NullLogger<RpcClientFunctionInvocationDispatcher>.Instance);
        await dispatcher.InitializeAsync([function]);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);
        await dispatcher.InvokeAsync(invocation);
        StreamingMessage request = await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);
        await SendInvocationResponseAsync(worker, request.InvocationRequest.InvocationId);
        Assert.NotNull(await invocation.ResultSource.Task.WaitAsync(TestTimeout));
    }

    [Fact]
    public async Task TryReserveIdleWorker_InvocationCommittedFirst_CompletesWithoutInterruption()
    {
        ManualTimeProvider timeProvider = new();
        (RegistryHarness harness, ChannelControl worker, FunctionMetadata function) =
            await CreateIdleHttpWorkerAsync(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        IHttpWorkerScaleInManager scaleInManager = registry;
        using RpcClientFunctionInvocationDispatcher dispatcher = new(
            registry,
            NullLogger<RpcClientFunctionInvocationDispatcher>.Instance);
        await dispatcher.InitializeAsync([function]);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);

        await dispatcher.InvokeAsync(invocation);
        StreamingMessage request = await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);
        Assert.Null(scaleInManager.TryReserveIdleWorker(TimeSpan.Zero));

        await SendInvocationResponseAsync(worker, request.InvocationRequest.InvocationId);
        Assert.NotNull(await invocation.ResultSource.Task.WaitAsync(TestTimeout));
        HttpWorkerScaleInReservation reservation = await WaitForReservationAsync(scaleInManager);
        Assert.True(scaleInManager.CancelReservation(reservation));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task InvokeAsync_HttpDrainWaitsForResultAndForwarding(bool resultCompletesFirst)
    {
        ManualTimeProvider timeProvider = new();
        TaskCompletionSource forwarding = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Mock<IHttpProxyService> proxy = new(MockBehavior.Strict);
        proxy.Setup(service => service.StartForwarding(It.IsAny<ScriptInvocationContext>(), HttpProxyEndpoint))
            .Callback<ScriptInvocationContext, Uri>((context, _) =>
                context.Properties[ScriptConstants.HttpProxyTask] = forwarding.Task);
        proxy.Setup(service => service.EnsureSuccessfulForwardingAsync(It.IsAny<ScriptInvocationContext>()))
            .Returns(forwarding.Task);
        await using ClientWorkerChannelTestHarness worker = await ClientWorkerChannelTestHarness.CreateAsync(
            "worker",
            advertisedHttpUri: HttpProxyEndpoint.AbsoluteUri,
            httpProxyService: proxy.Object,
            functionGroupName: FunctionGroups.Http,
            isAlwaysReady: false,
            timeProvider: timeProvider);
        FunctionMetadata function = CreateScaleInHttpFunction();
        await PrepareScaleInReadyChannelAsync(worker, function);
        Mock<IWorkerChannelRegistry> registry = new();
        registry.Setup(value => value.GetReadyChannelsAsync())
            .Returns(new ValueTask<IReadOnlyList<WorkerChannel>>([worker.Channel]));
        using RpcClientFunctionInvocationDispatcher dispatcher = new(
            registry.Object,
            NullLogger<RpcClientFunctionInvocationDispatcher>.Instance);
        await dispatcher.InitializeAsync([function]);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);
        await dispatcher.InvokeAsync(invocation);
        StreamingMessage request = await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);

        if (resultCompletesFirst)
        {
            await worker.SendInvocationResponseAsync(request.InvocationRequest.InvocationId);
        }
        else
        {
            forwarding.SetResult();
        }

        Assert.False(worker.Channel.TryReserveIdleScaleIn(TimeSpan.Zero, out _));

        if (resultCompletesFirst)
        {
            forwarding.SetResult();
        }
        else
        {
            await worker.SendInvocationResponseAsync(request.InvocationRequest.InvocationId);
        }

        Assert.NotNull(await invocation.ResultSource.Task.WaitAsync(TestTimeout));
        Guid reservationId = await WaitForReservationAsync(worker.Channel);
        Assert.True(worker.Channel.CancelScaleInReservation(reservationId));
        proxy.Verify(service => service.StartForwarding(invocation, HttpProxyEndpoint), Times.Once);
        proxy.Verify(service => service.EnsureSuccessfulForwardingAsync(invocation), Times.Once);
        proxy.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task TryCommitReservation_RevalidatesStateAndUnlinksIdleWorkerExactlyOnce()
    {
        ManualTimeProvider timeProvider = new();
        (RegistryHarness harness, ChannelControl worker, FunctionMetadata function) =
            await CreateIdleHttpWorkerAsync(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        IHttpWorkerScaleInManager scaleInManager = registry;
        ChannelControl otherWorker = new(
            "other-worker",
            functionGroupName: FunctionGroups.Http,
            isAlwaysReady: false);
        harness.Enqueue(otherWorker);
        await LinkAsync(registry, otherWorker.Id);
        await otherWorker.ReadRequestAsync(StreamingMessage.ContentOneofCase.FunctionLoadRequest);
        await SendFunctionLoadResponseAsync(otherWorker, function.GetFunctionId());
        timeProvider.Advance(HttpOnDemandWorkerScaleInService.IdleDuration);

        HttpWorkerScaleInReservation staleReservation =
            scaleInManager.TryReserveIdleWorker(HttpOnDemandWorkerScaleInService.IdleDuration);
        Assert.NotNull(staleReservation);
        HttpWorkerScaleInReservation transientReservation =
            scaleInManager.TryReserveIdleWorker(HttpOnDemandWorkerScaleInService.IdleDuration);
        Assert.NotNull(transientReservation);
        Assert.True(scaleInManager.CancelReservation(transientReservation));
        Assert.Equal(staleReservation.ProjectedState.HttpCapacity, scaleInManager.Current.HttpCapacity);
        Assert.Equal(staleReservation.ProjectedState.LinkedWorkerCount, scaleInManager.Current.LinkedWorkerCount);
        Assert.NotEqual(staleReservation.ProjectedState.Version, scaleInManager.Current.Version);

        Assert.False(scaleInManager.TryCommitReservation(staleReservation, out _));
        Assert.True(scaleInManager.CancelReservation(staleReservation));
        HttpWorkerScaleInReservation currentReservation =
            scaleInManager.TryReserveIdleWorker(HttpOnDemandWorkerScaleInService.IdleDuration);
        Assert.NotNull(currentReservation);
        ChannelControl drainedWorker = string.Equals(
            currentReservation.WorkerId,
            worker.Id,
            StringComparison.Ordinal)
                ? worker
                : otherWorker;
        ChannelControl remainingWorker = ReferenceEquals(drainedWorker, worker)
            ? otherWorker
            : worker;

        Assert.True(scaleInManager.TryCommitReservation(currentReservation, out Task<bool> drainCompletion));
        Assert.True(await drainCompletion.WaitAsync(TestTimeout));

        Assert.False(scaleInManager.TryCommitReservation(currentReservation, out _));
        Assert.Same(
            remainingWorker.Channel,
            Assert.Single(registry.GetInitializedChannels()));
        Assert.Equal(1, scaleInManager.Current.LinkedWorkerCount);
        Assert.Equal(1, drainedWorker.DisposeCount);
        Assert.Equal(0, remainingWorker.DisposeCount);
        harness.ChannelFactory.Verify(factory => factory.Create(worker.Id, worker.Transport), Times.Once);
        harness.ChannelFactory.Verify(factory => factory.Create(otherWorker.Id, otherWorker.Transport), Times.Once);
    }

    [Fact]
    public async Task RuntimeRetirement_CloseThenDrain_PreservesCommittedWorkAndSelectsOnce()
    {
        ManualTimeProvider timeProvider = new();
        (RegistryHarness harness, ChannelControl worker, FunctionMetadata function) =
            await CreateIdleHttpWorkerAsync(timeProvider);
        await using WorkerChannelRegistry registry = harness.Registry;
        IRuntimeRetirementWorkerManager retirementWorkerManager = registry;
        IHttpWorkerScaleInManager scaleInManager = registry;
        using RpcClientFunctionInvocationDispatcher dispatcher = new(
            registry,
            NullLogger<RpcClientFunctionInvocationDispatcher>.Instance);
        await dispatcher.InitializeAsync([function]);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);
        await dispatcher.InvokeAsync(invocation);
        StreamingMessage request = await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);
        SplitScriptHostActivationFaulted condition = new(
            retirementWorkerManager.Incarnation,
            new InvalidOperationException("activation failed"));

        Assert.True(retirementWorkerManager.TryCloseAdmission(condition));
        Assert.Equal(0, scaleInManager.Current.HttpCapacity);
        WorkerLinkException linkFailure = await Assert.ThrowsAsync<WorkerLinkException>(
            () => registry.LinkAsync(
                "blocked-worker",
                new Uri("http://127.0.0.1:5001"),
                CancellationToken.None));
        Assert.Equal(WorkerLinkFailureReason.RuntimeStopping, linkFailure.Reason);

        Task<RuntimeRetirementDrainResult> drain = retirementWorkerManager.DrainAllWorkersAsync(
            retirementWorkerManager.Incarnation,
            CancellationToken.None);
        Assert.False(drain.IsCompleted);
        Assert.Equal(0, worker.DisposeCount);

        await SendInvocationResponseAsync(worker, request.InvocationRequest.InvocationId);
        Assert.NotNull(await invocation.ResultSource.Task.WaitAsync(TestTimeout));
        Assert.True((await drain.WaitAsync(TestTimeout)).IsComplete);
        Assert.Empty(registry.GetInitializedChannels());
        Assert.Equal(1, worker.DisposeCount);

        Assert.True((await retirementWorkerManager.DrainAllWorkersAsync(
            retirementWorkerManager.Incarnation,
            CancellationToken.None)).IsComplete);
        Assert.Equal(1, worker.DisposeCount);

        ManualTimeProvider overlapTimeProvider = new();
        FunctionsSplitWorkerRemovalAction fallbackAction = new(
            new(
                "test-namespace",
                "overlap-worker"),
            "RuntimeUnlink");
        (RegistryHarness overlapHarness, ChannelControl _, FunctionMetadata _) = await CreateIdleHttpWorkerAsync(
            overlapTimeProvider,
            completeResponsesOnRequestCompletion: false,
            unlinkTimeout: TimeSpan.FromSeconds(1),
            workerIdentity: fallbackAction.Worker);
        await using WorkerChannelRegistry overlapRegistry = overlapHarness.Registry;
        IHttpWorkerScaleInManager overlapScaleInManager = overlapRegistry;
        IRuntimeRetirementWorkerManager overlapRetirementManager = overlapRegistry;
        HttpWorkerScaleInReservation reservation = overlapScaleInManager.TryReserveIdleWorker(
            HttpOnDemandWorkerScaleInService.IdleDuration);
        Assert.NotNull(reservation);
        Assert.True(overlapScaleInManager.TryCommitReservation(reservation, out Task<bool> existingDrain));
        while (overlapScaleInManager.Current.LinkedWorkerCount != 0)
        {
            long version = overlapScaleInManager.Version;
            await overlapScaleInManager.WaitForChangeAsync(version).WaitAsync(TestTimeout);
        }

        SplitScriptHostActivationFaulted overlapCondition = new(
            overlapRetirementManager.Incarnation,
            new InvalidOperationException("activation failed during scale-in"));
        Assert.True(overlapRetirementManager.TryCloseAdmission(overlapCondition));
        Task<RuntimeRetirementDrainResult> overlappingRetirement = overlapRetirementManager.DrainAllWorkersAsync(
            overlapRetirementManager.Incarnation,
            CancellationToken.None);

        Assert.False(overlappingRetirement.IsCompleted);
        Assert.True(await existingDrain.WaitAsync(TestTimeout));
        Assert.True((await overlappingRetirement.WaitAsync(TestTimeout)).IsComplete);
        Assert.Equal(fallbackAction, Assert.Single(overlapScaleInManager.Current.OutstandingWorkerRemovals));
        Assert.Equal(
            WorkerRemovalAcknowledgementResult.Accepted,
            overlapScaleInManager.AcknowledgeWorkerRemoval(fallbackAction));
    }

    private static async Task<(RegistryHarness Harness, ChannelControl Worker, FunctionMetadata Function)>
        CreateIdleHttpWorkerAsync(
            ManualTimeProvider timeProvider,
            bool completeResponsesOnRequestCompletion = true,
            TimeSpan? unlinkTimeout = null,
            FunctionsSplitPodIdentity workerIdentity = null)
    {
        RegistryHarness harness = unlinkTimeout.HasValue
            ? new(timeProvider, unlinkTimeout.Value)
            : new(timeProvider);
        ChannelControl worker = new(
            workerIdentity?.PodName ?? "worker",
            functionGroupName: FunctionGroups.Http,
            isAlwaysReady: false,
            completeResponsesOnRequestCompletion: completeResponsesOnRequestCompletion);
        harness.Enqueue(worker);
        FunctionMetadata function = CreateScaleInHttpFunction();
        if (workerIdentity is null)
        {
            await LinkAsync(harness.Registry, worker.Id);
        }
        else
        {
            await harness.Registry.LinkAsync(
                worker.Id,
                CreateEndpoint(worker.Id),
                workerIdentity,
                CancellationToken.None);
        }

        await harness.Registry.ActivateAsync([function], new ManagedDependencyOptions(), functionTimeout: null);
        await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.FunctionLoadRequest);
        await SendFunctionLoadResponseAsync(worker, function.GetFunctionId());
        timeProvider.Advance(HttpOnDemandWorkerScaleInService.IdleDuration);

        return (harness, worker, function);
    }

    private static FunctionMetadata CreateScaleInHttpFunction()
    {
        FunctionMetadata function = CreateFunction("HttpFunction");
        function.Bindings.Add(new()
        {
            Name = "req",
            Type = "httpTrigger",
            Direction = BindingDirection.In,
        });

        return function;
    }

    private static ScriptInvocationContext CreateScaleInInvocation(FunctionMetadata function)
    {
        HttpRequest request = new DefaultHttpContext().Request;
        request.Method = "POST";
        request.Host = new("functions.test");
        request.Path = "/api/test";
        request.ContentType = "application/json";
        request.Body = new MemoryStream(Encoding.UTF8.GetBytes("{}"));
        request.ContentLength = request.Body.Length;

        return new()
        {
            FunctionMetadata = function,
            ExecutionContext = new()
            {
                FunctionName = function.Name,
                InvocationId = Guid.NewGuid(),
            },
            BindingData = [],
            Inputs = [("req", DataType.String, request)],
            ResultSource = new(TaskCreationOptions.RunContinuationsAsynchronously),
            AsyncExecutionContext = System.Threading.ExecutionContext.Capture(),
            Logger = NullLogger.Instance,
        };
    }

    private static async Task PrepareScaleInReadyChannelAsync(
        ClientWorkerChannelTestHarness worker,
        FunctionMetadata function)
    {
        worker.Channel.SetupFunctionInvocationBuffers([function]);
        worker.Channel.SendFunctionLoadRequests(new ManagedDependencyOptions(), functionTimeout: null);
        await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.FunctionLoadRequest);
        await worker.SendFunctionLoadResponseAsync(function.GetFunctionId());
        worker.Channel.SetScaleInReady(true);
    }

    private static ValueTask SendInvocationResponseAsync(ChannelControl worker, string invocationId)
        => worker.Transport.SendResponseAsync(new()
        {
            InvocationResponse = new()
            {
                InvocationId = invocationId,
                Result = new() { Status = StatusResult.Types.Status.Success },
            },
        });

    private static ValueTask SendFunctionLoadResponseAsync(ChannelControl worker, string functionId)
        => worker.Transport.SendResponseAsync(new()
        {
            FunctionLoadResponse = new()
            {
                FunctionId = functionId,
                Result = new() { Status = StatusResult.Types.Status.Success },
            },
        });

    private static async Task<HttpWorkerScaleInReservation> WaitForReservationAsync(
        IHttpWorkerScaleInManager scaleInManager)
    {
        using CancellationTokenSource timeout = new(TestTimeout);
        while (true)
        {
            HttpWorkerScaleInReservation reservation = scaleInManager.TryReserveIdleWorker(TimeSpan.Zero);
            if (reservation is not null)
            {
                return reservation;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
        }
    }

    private static async Task<Guid> WaitForReservationAsync(RpcClientWorkerChannel channel)
    {
        using CancellationTokenSource timeout = new(TestTimeout);
        while (true)
        {
            if (channel.TryReserveIdleScaleIn(TimeSpan.Zero, out Guid reservationId))
            {
                return reservationId;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => _timestamp;

        public void Advance(TimeSpan duration)
        {
            if (duration < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(duration));
            }

            _utcNow += duration;
            _timestamp += duration.Ticks;
        }
    }

    private sealed class SequenceHostStateHandler(HttpStatusCode projectedStatus) : HttpMessageHandler
    {
        private int _requestCount;

        public List<AppServerHostStateClient.HostStateRequest> PublishedStates { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            string json = await request.Content.ReadAsStringAsync(cancellationToken);
            AppServerHostStateClient.HostStateRequest state =
                JsonSerializer.Deserialize(json, AppServerHostStateJsonContext.Default.HostStateRequest);
            PublishedStates.Add(state);

            int requestIndex = _requestCount++;
            if (requestIndex == 1)
            {
                return new(projectedStatus);
            }

            HttpResponseMessage response = new(HttpStatusCode.OK);
            response.Headers.TryAddWithoutValidation(
                AppServerHostStateClient.CapacityFenceHeader,
                AppServerHostStateClient.CapacityFenceApplied);

            return response;
        }
    }
}
