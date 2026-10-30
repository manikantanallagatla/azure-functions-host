// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.Controllers;
using Azure.Functions.Host.HostState;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Azure.WebJobs.Script.ManagedDependencies;
using Microsoft.Azure.WebJobs.Script.WebHost;
using Microsoft.Azure.WebJobs.Script.WebHost.Security.Authorization.Policies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Azure.Functions.Rpc.Client.Tests;

public sealed partial class WorkerChannelRegistryTests
{
    [Fact]
    public async Task PostScaleIn_GroupClassAndReadinessIsolation_DrainsOnlyMatchingReadyWorkers()
    {
        RegistryHarness harness = new();
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("TestFunction");
        ChannelControl matching = new("matching", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false);
        ChannelControl wrongGroup = new("wrong-group", functionGroupName: FunctionGroups.Http, isAlwaysReady: false);
        ChannelControl wrongClass = new("wrong-class", functionGroupName: FunctionGroups.Durable, isAlwaysReady: true);
        ChannelControl readinessPending =
            new("readiness-pending", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false);
        foreach (ChannelControl worker in new[] { matching, wrongGroup, wrongClass, readinessPending })
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, matching, wrongGroup, wrongClass, readinessPending);
        readinessPending.Channel.SetScaleInReady(false);

        ChannelControl starting = new(
            "starting",
            blockStart: true,
            functionGroupName: FunctionGroups.Durable,
            isAlwaysReady: false);
        harness.Enqueue(starting);
        Task<WorkerLinkResult> startingLink = registry.LinkAsync(starting.Id, CreateEndpoint(starting.Id));
        await starting.StartEntered.WaitAsync(TestTimeout);

        using CountScaleInHostStateHandler handler = new();
        await using CountScaleInApiHost host = await CountScaleInApiHost.StartAsync(registry, handler);
        using HttpResponseMessage response = await host.PostAsync(
            """{"functionGroup":"durable","isAlwaysReady":false,"desiredWorkerCount":0}""");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, await ReadNewlyDrainingWorkerCountAsync(response));
        await WaitUntilAsync(() => matching.DisposeCount == 1);
        Assert.Equal(0, wrongGroup.DisposeCount);
        Assert.Equal(0, wrongClass.DisposeCount);
        Assert.Equal(0, readinessPending.DisposeCount);

        starting.AllowStart();
        await startingLink.WaitAsync(TestTimeout);
        await starting.ReadRequestAsync(StreamingMessage.ContentOneofCase.FunctionLoadRequest);
        await SendFunctionLoadResponseAsync(starting, function.GetFunctionId());
        await WaitUntilAsync(() => starting.Channel.IsReadyForInvocations);
        Assert.Contains(starting.Channel, registry.GetInitializedChannels());
    }

    [Fact]
    public async Task PostScaleIn_DesiredCountAndRepeat_CountOnlyNewDrainsAndPublishOrdinaryHttpState()
    {
        RegistryHarness harness = new();
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("HttpFunction");
        const string mixedCaseHttp = "HTTP";
        ChannelControl first = new("first", functionGroupName: mixedCaseHttp, isAlwaysReady: false);
        ChannelControl second = new("second", functionGroupName: mixedCaseHttp, isAlwaysReady: false);
        foreach (ChannelControl worker in new[] { first, second })
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, first, second);

        using CountScaleInHostStateHandler handler = new();
        await using CountScaleInApiHost host = await CountScaleInApiHost.StartAsync(registry, handler);
        using HttpResponseMessage retained = await host.PostAsync(
            """{"functionGroup":"HTTP","isAlwaysReady":false,"desiredWorkerCount":2}""");
        using HttpResponseMessage reduced = await host.PostAsync(
            """{"functionGroup":"HTTP","isAlwaysReady":false,"desiredWorkerCount":1}""");
        using HttpResponseMessage repeated = await host.PostAsync(
            """{"functionGroup":"HTTP","isAlwaysReady":false,"desiredWorkerCount":1}""");

        Assert.Equal(0, await ReadNewlyDrainingWorkerCountAsync(retained));
        Assert.Equal(1, await ReadNewlyDrainingWorkerCountAsync(reduced));
        Assert.Equal(0, await ReadNewlyDrainingWorkerCountAsync(repeated));
        await WaitUntilAsync(() => first.DisposeCount == 1);
        Assert.Equal(0, second.DisposeCount);
        CountScaleInPublication publication = Assert.Single(handler.Publications);
        Assert.Equal(16L, publication.HttpCapacity);
        Assert.All(handler.Publications, publication => Assert.False(publication.HasCapacityFence));
        IHttpWorkerScaleInManager scaleInManager = registry;
        HttpWorkerScaleInReservation reservation = scaleInManager.TryReserveIdleWorker(TimeSpan.Zero);
        Assert.NotNull(reservation);
        Assert.True(scaleInManager.CancelReservation(reservation));
    }

    [Fact]
    public async Task PostScaleIn_ZeroTarget_DrainsAllMatchingWorkers()
    {
        RegistryHarness harness = new();
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("DurableFunction");
        ChannelControl first = new("first", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false);
        ChannelControl second = new("second", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false);
        foreach (ChannelControl worker in new[] { first, second })
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, first, second);

        using CountScaleInHostStateHandler handler = new();
        await using CountScaleInApiHost host = await CountScaleInApiHost.StartAsync(registry, handler);
        using HttpResponseMessage response = await host.PostAsync(
            """{"functionGroup":"durable","isAlwaysReady":false,"desiredWorkerCount":0}""");

        Assert.Equal(2, await ReadNewlyDrainingWorkerCountAsync(response));
        await WaitUntilAsync(() => first.DisposeCount == 1 && second.DisposeCount == 1);
        Assert.Empty(registry.GetInitializedChannels());
    }

    [Fact]
    public async Task PostScaleIn_PositiveDurableTarget_PrefersIdleThenWorkerId()
    {
        RegistryHarness harness = new();
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("DurableFunction");
        const string mixedCaseDurable = "Durable";
        ChannelControl busy = new("a-busy", functionGroupName: mixedCaseDurable, isAlwaysReady: false);
        ChannelControl firstIdle = new("b-idle", functionGroupName: mixedCaseDurable, isAlwaysReady: false);
        ChannelControl secondIdle = new("c-idle", functionGroupName: mixedCaseDurable, isAlwaysReady: false);
        ChannelControl[] workers =
        [
            busy,
            firstIdle,
            secondIdle,
        ];
        foreach (ChannelControl worker in workers)
        {
            harness.Enqueue(worker);
            await registry.LinkAsync(
                worker.Id,
                CreateEndpoint(worker.Id),
                CreateWorkerIdentity(worker.Id, Guid.NewGuid()));
        }

        await MakeWorkersReadyAsync(registry, function, workers);
        ScriptInvocationContext invocation = CreateScaleInInvocation(function);
        Assert.True(busy.Channel.TryDispatchInvocation(invocation));
        StreamingMessage request =
            await busy.ReadRequestAsync(StreamingMessage.ContentOneofCase.InvocationRequest);

        using CountScaleInHostStateHandler handler = new();
        await using CountScaleInApiHost host = await CountScaleInApiHost.StartAsync(registry, handler);
        using HttpResponseMessage response = await host.PostAsync(
            """{"functionGroup":"Durable","isAlwaysReady":false,"desiredWorkerCount":2}""");

        Assert.Equal(1, await ReadNewlyDrainingWorkerCountAsync(response));
        await WaitUntilAsync(() => firstIdle.DisposeCount == 1);
        Assert.Equal(0, busy.DisposeCount);
        Assert.Equal(0, secondIdle.DisposeCount);
        Assert.Contains(busy.Channel, registry.GetInitializedChannels());
        Assert.Contains(secondIdle.Channel, registry.GetInitializedChannels());

        await SendInvocationResponseAsync(busy, request.InvocationRequest.InvocationId);
        Assert.NotNull(await invocation.ResultSource.Task.WaitAsync(TestTimeout));
    }

    [Fact]
    public async Task PostScaleIn_InvalidRequestAndPositiveDurableTargetWithoutLinkMetadata_UseNormalOrdering()
    {
        RegistryHarness harness = new();
        await using WorkerChannelRegistry registry = harness.Registry;
        FunctionMetadata function = CreateFunction("DurableFunction");
        ChannelControl first = new("first", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false);
        ChannelControl second = new("second", functionGroupName: FunctionGroups.Durable, isAlwaysReady: false);
        foreach (ChannelControl worker in new[] { first, second })
        {
            harness.Enqueue(worker);
            await LinkAsync(registry, worker.Id);
        }

        await MakeWorkersReadyAsync(registry, function, first, second);

        using CountScaleInHostStateHandler handler = new();
        await using CountScaleInApiHost host = await CountScaleInApiHost.StartAsync(registry, handler);
        using HttpResponseMessage invalid = await host.PostAsync(
            """{"functionGroup":"durable","isAlwaysReady":false,"desiredWorkerCount":-1}""");
        using HttpResponseMessage reduced = await host.PostAsync(
            """{"functionGroup":"durable","isAlwaysReady":false,"desiredWorkerCount":1}""");

        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
        Assert.Contains(
            "\"code\":\"InvalidValue\",\"target\":\"desiredWorkerCount\"",
            await invalid.Content.ReadAsStringAsync(),
            StringComparison.Ordinal);
        Assert.Equal(1, await ReadNewlyDrainingWorkerCountAsync(reduced));
        await WaitUntilAsync(() => first.DisposeCount == 1);
        Assert.Equal(0, second.DisposeCount);
        Assert.Single(registry.GetInitializedChannels());
    }

    private static async Task MakeWorkersReadyAsync(
        WorkerChannelRegistry registry,
        FunctionMetadata function,
        params ChannelControl[] workers)
    {
        await registry.ActivateAsync([function], new ManagedDependencyOptions(), functionTimeout: null);
        foreach (ChannelControl worker in workers)
        {
            await worker.ReadRequestAsync(StreamingMessage.ContentOneofCase.FunctionLoadRequest);
            await SendFunctionLoadResponseAsync(worker, function.GetFunctionId());
        }

        await WaitUntilAsync(() => workers.All(worker => worker.Channel.IsReadyForInvocations));
    }

    private static async Task<int> ReadNewlyDrainingWorkerCountAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        using JsonDocument body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return body.RootElement.GetProperty("newlyDrainingWorkerCount").GetInt32();
    }

    private sealed class CountScaleInApiHost : IAsyncDisposable
    {
        private readonly HttpClient _appServerClient;
        private readonly IHost _host;

        private CountScaleInApiHost(IHost host, HttpClient appServerClient)
        {
            _host = host;
            _appServerClient = appServerClient;
            Client = host.GetTestClient();
        }

        internal HttpClient Client { get; }

        internal static async Task<CountScaleInApiHost> StartAsync(
            WorkerChannelRegistry registry,
            CountScaleInHostStateHandler handler)
        {
            HttpClient appServerClient = new(handler, disposeHandler: false);
            Mock<IHttpClientFactory> factory = new();
            factory.Setup(value => value.CreateClient(AppServerHostStateClient.HttpClientName))
                .Returns(appServerClient);
            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    [Microsoft.Azure.WebJobs.Script.EnvironmentSettingNames.ContainerName] = "test-container",
                    [Microsoft.Azure.WebJobs.Script.EnvironmentSettingNames.MeshInitURI] = "http://appserver.test/",
                })
                .Build();
            IHost host = new HostBuilder()
                .ConfigureWebHost(webBuilder =>
                {
                    webBuilder
                        .UseSetting(WebHostDefaults.ApplicationKey, typeof(Startup).Assembly.GetName().Name)
                        .UseTestServer()
                        .ConfigureLogging(logging => logging.ClearProviders())
                        .ConfigureServices(services =>
                        {
                            IMvcBuilder mvcBuilder = services.AddMvc(options =>
                            {
                                options.EnableEndpointRouting = false;
                            }).AddNewtonsoftJson();
                            mvcBuilder.ConfigureApplicationPartManager(parts =>
                            {
                                parts.ApplicationParts.Clear();
                                parts.ApplicationParts.Add(new AssemblyPart(typeof(WorkerScaleInController).Assembly));
                            });

                            services.AddSingleton<IWorkerCountScaleInManager>(registry);
                            services.AddSingleton<IHttpWorkerScaleInManager>(registry);
                            services.AddSingleton<IRuntimeRetirementWorkerManager>(registry);
                            services.AddSingleton(factory.Object);
                            services.AddSingleton(configuration);
                            services.AddSingleton(TimeProvider.System);
                            services.AddSingleton<AppServerHostStateClient>();
                            services.AddSingleton<IWorkerScaleInStatePublisher>(
                                provider => provider.GetRequiredService<AppServerHostStateClient>());
                            services.AddAuthentication();
                            services.AddAuthorization(options =>
                            {
                                options.AddScriptPolicies();
                                options.AddPolicy(
                                    PolicyNames.AdminAuthLevel,
                                    policy => policy.RequireAssertion(_ => true));
                            });
                        })
                        .Configure(app => app.UseMvc());
                })
                .Build();

            try
            {
                await host.StartAsync();
                return new CountScaleInApiHost(host, appServerClient);
            }
            catch
            {
                appServerClient.Dispose();
                await DisposeHostAsync(host);
                throw;
            }
        }

        internal async Task<HttpResponseMessage> PostAsync(string json)
        {
            using HttpRequestMessage request = new(HttpMethod.Post, "/admin/workers/scale-in");
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
            return await Client.SendAsync(request);
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            using CancellationTokenSource timeout = new(TestTimeout);
            try
            {
                await _host.StopAsync(timeout.Token);
            }
            finally
            {
                _appServerClient.Dispose();
                await DisposeHostAsync(_host);
            }
        }

        private static async Task DisposeHostAsync(IHost host)
        {
            if (host is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync().AsTask().WaitAsync(TestTimeout);
            }
            else
            {
                host.Dispose();
            }
        }
    }

    private sealed class CountScaleInHostStateHandler : HttpMessageHandler
    {
        internal List<CountScaleInPublication> Publications { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            using JsonDocument body = JsonDocument.Parse(
                await request.Content!.ReadAsStringAsync(cancellationToken));
            Publications.Add(new(
                body.RootElement.GetProperty("httpCapacity").GetInt64(),
                request.Headers.Contains(AppServerHostStateClient.CapacityFenceHeader)));
            return new(HttpStatusCode.OK);
        }
    }

    private sealed record CountScaleInPublication(long HttpCapacity, bool HasCapacityFence);
}
