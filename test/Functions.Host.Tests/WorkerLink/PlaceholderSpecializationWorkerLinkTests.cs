// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Rpc.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Azure.WebJobs.Script.WebHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Moq;
using Xunit;

namespace Azure.Functions.Host.Tests.WorkerLink;

// Specialization completes a process-wide standby change token once, so this flow is covered by a single test.
[Collection(nameof(EnvironmentVariableCollection))]
public sealed class PlaceholderSpecializationWorkerLinkTests
{
    private const string WorkerId = "worker-pod-1";
    private const string GrpcEndpoint = "http://100.64.1.12:50053";
    private const string RuntimePodName = "runtime-pod-1";
    private const string SiteName = "test-site";
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task LinkWorker_DuringPlaceholderSpecialization_LinksWithSpecializedConfiguration()
    {
        using EnvironmentVariableScope placeholderMode = new(EnvironmentSettingNames.AzureWebsitePlaceholderMode, "1");
        using EnvironmentVariableScope containerReady = new(EnvironmentSettingNames.AzureWebsiteContainerReady, null);
        using EnvironmentVariableScope siteName = new(EnvironmentSettingNames.AzureWebsiteName, SiteName);
        using EnvironmentVariableScope sku = new(EnvironmentSettingNames.AzureWebsiteSku, "FlexConsumption");
        using EnvironmentVariableScope legion = new(EnvironmentSettingNames.LegionServiceHost, "1");
        using EnvironmentVariableScope podName = new(EnvironmentSettingNames.WebsitePodName, RuntimePodName);
        using EnvironmentVariableScope encryptionKey = new(
            EnvironmentSettingNames.ContainerEncryptionKey,
            Convert.ToBase64String(new byte[32]));
        using EnvironmentVariableScope siteEncryptionKey = new(
            EnvironmentSettingNames.WebSiteAuthEncryptionKey,
            Convert.ToBase64String(new byte[32]));
        IOptionsMonitor<ScriptApplicationHostOptions>? hostOptions = null;
        bool? linkedWithStandbyConfiguration = null;
        Mock<IWorkerChannelRegistry> registry = new();
        registry.Setup(value => value.LinkAsync(
                WorkerId,
                new Uri(GrpcEndpoint),
                It.Is<FunctionsSplitPodIdentity>(worker =>
                    string.Equals(worker.ResourceNamespace, "resource-namespace", StringComparison.Ordinal) &&
                    string.Equals(worker.PodName, WorkerId, StringComparison.Ordinal)),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                linkedWithStandbyConfiguration = hostOptions!.CurrentValue.IsStandbyConfiguration;
                return Task.FromResult(new WorkerLinkResult(null!, IsNewLink: true));
            });

        // No channel initializes, so the script host never starts. In compute separation mode, specialization must not
        // wait for the script host, because the script host waits for this link.
        registry.Setup(value => value.WaitForFirstInitializedAsync(It.IsAny<CancellationToken>()))
            .Returns((CancellationToken token) => new TaskCompletionSource<WorkerChannel>().Task.WaitAsync(token));
        registry.Setup(value => value.GetInitializedChannels()).Returns(Array.Empty<WorkerChannel>());

        using IHost host = CreateHost(registry.Object);
        await host.StartAsync();
        try
        {
            hostOptions = host.Services.GetRequiredService<IOptionsMonitor<ScriptApplicationHostOptions>>();
            Assert.True(hostOptions.CurrentValue.IsStandbyConfiguration);
            Environment.SetEnvironmentVariable(EnvironmentSettingNames.AzureWebsitePlaceholderMode, "0");
            Environment.SetEnvironmentVariable(EnvironmentSettingNames.AzureWebsiteContainerReady, "1");

            using CancellationTokenSource timeout = new(TestTimeout);
            using HttpResponseMessage response = await PutLinkAsync(host.GetTestClient(), timeout.Token);

            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.False(linkedWithStandbyConfiguration);
        }
        finally
        {
            using CancellationTokenSource stopTimeout = new(TimeSpan.FromSeconds(5));
            await host.StopAsync(stopTimeout.Token);
        }
    }

    private static IHost CreateHost(IWorkerChannelRegistry registry)
        => new HostBuilder()
            .ConfigureAppConfiguration(config => config.AddEnvironmentVariables())
            .ConfigureWebHost(webBuilder => webBuilder
                .UseTestServer()
                .ConfigureLogging(logging => logging.ClearProviders())
                .ConfigureServices((context, services) =>
                {
                    services.AddWebJobsScriptHostAuthentication();
                    services.AddWebJobsScriptHostAuthorization();
                    services.AddWebJobsScriptHost(context.Configuration, ClientWorkerComposition.Instance);
                    services.Replace(ServiceDescriptor.Singleton(registry));
                })
                .Configure(app => app.UseWebJobsScriptHost()))
            .Build();

    private static async Task<HttpResponseMessage> PutLinkAsync(HttpClient client, CancellationToken cancellationToken)
    {
        using HttpRequestMessage request = new(HttpMethod.Put, $"/admin/workers/{WorkerId}")
        {
            Content = new StringContent(
                $$"""
                {
                  "workerGrpcEndpoint": "{{GrpcEndpoint}}",
                  "workerNamespace": "resource-namespace",
                  "workerPodName": "{{WorkerId}}"
                }
                """,
                Encoding.UTF8,
                "application/json"),
        };
        request.Headers.Add(
            ScriptConstants.SiteTokenHeaderName,
            CreateSiteToken());

        return await client.SendAsync(request, cancellationToken);
    }

    private static string CreateSiteToken()
    {
        JwtSecurityTokenHandler handler = new();
        SecurityToken token = handler.CreateToken(new()
        {
            Claims = new Dictionary<string, object>
            {
                [JwtRegisteredClaimNames.Aud] = new[]
                {
                    RuntimePodName,
                    string.Format(ScriptConstants.SiteAzureFunctionsUriFormat, SiteName),
                    string.Format(ScriptConstants.SiteUriFormat, SiteName),
                },
            },
            Issuer = ScriptConstants.AppServiceCoreUri,
            Expires = DateTime.UtcNow.AddMinutes(5),
            SigningCredentials = new(
                new SymmetricSecurityKey(new byte[32]),
                SecurityAlgorithms.HmacSha256Signature),
        });

        return handler.WriteToken(token);
    }
}
