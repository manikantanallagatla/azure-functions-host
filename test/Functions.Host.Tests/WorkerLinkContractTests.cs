// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Reflection;
using Azure.Functions.Host.Controllers;
using Azure.Functions.Host.Models;
using Azure.Functions.Host.WorkerLink;
using Azure.Functions.Host.WorkerScaleIn;
using Azure.Functions.Rpc.Client;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Azure.WebJobs.Script.WebHost;
using Microsoft.Azure.WebJobs.Script.WebHost.Security.Authorization.Policies;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Azure.Functions.Host.Tests;

/// <summary>
/// Verifies that the worker-link route and DTOs remain isolated to the compute host.
/// </summary>
public class WorkerLinkContractTests
{
    [Fact]
    public void WorkerLinkTypes_LiveInComputeProductAssembly()
    {
        Assembly compute = typeof(ClientWorkerComposition).Assembly;
        Assert.Equal("Azure.Functions.Host", compute.GetName().Name);

        Assert.Same(compute, typeof(WorkerLinkController).Assembly);
        Assert.Same(compute, typeof(WorkerLinkRequest).Assembly);
        Assert.Same(compute, typeof(WorkerLinkError).Assembly);
        Assert.Same(compute, typeof(WorkerLinkErrorResponse).Assembly);
        Assert.Same(compute, typeof(WorkerScaleInController).Assembly);
        Assert.Same(compute, typeof(WorkerScaleInRequest).Assembly);
        Assert.Same(compute, typeof(WorkerScaleInResponse).Assembly);
        Assert.Same(compute, typeof(RequestValidationError).Assembly);
        Assert.Same(compute, typeof(RequestValidationResponse).Assembly);
    }

    [Fact]
    public void StandardWebHost_DoesNotDefineWorkerLinkTypes()
    {
        // FunctionsHost is the shared/standard WebHost entry point; use it to reach that assembly.
        Assembly standardWebHost = typeof(FunctionsHost).Assembly;
        Assert.Equal("Microsoft.Azure.WebJobs.Script.WebHost", standardWebHost.GetName().Name);

        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.Controllers.WorkerLinkController"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.WorkerLink.WorkerLinkRequest"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.WorkerLink.WorkerLinkError"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.WorkerLink.WorkerLinkErrorResponse"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.Controllers.WorkerScaleInController"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.WorkerScaleIn.WorkerScaleInRequest"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.WorkerScaleIn.WorkerScaleInResponse"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.Models.RequestValidationError"));
        Assert.Null(standardWebHost.GetType("Azure.Functions.Host.Models.RequestValidationResponse"));
    }

    [Fact]
    public void ComputeManagementControllers_MapExpectedRoutesAndAuthorization()
    {
        MethodInfo link = typeof(WorkerLinkController).GetMethod(nameof(WorkerLinkController.LinkWorker))!;

        var httpPut = link.GetCustomAttribute<Microsoft.AspNetCore.Mvc.HttpPutAttribute>();
        var route = link.GetCustomAttribute<Microsoft.AspNetCore.Mvc.RouteAttribute>();

        Assert.NotNull(httpPut);
        Assert.NotNull(route);
        Assert.Equal("admin/workers/{workerId}", route!.Template);
        Assert.Equal(PolicyNames.AdminAuthLevel, link.GetCustomAttribute<AuthorizeAttribute>()!.Policy);
        ParameterInfo workerId = Assert.Single(link.GetParameters().Where(parameter =>
            string.Equals(parameter.Name, "workerId", StringComparison.Ordinal)));
        Assert.NotNull(workerId.GetCustomAttribute<Microsoft.AspNetCore.Mvc.FromRouteAttribute>());

        MethodInfo scaleIn = typeof(WorkerScaleInController).GetMethod(
            nameof(WorkerScaleInController.ScaleInWorkers))!;
        Assert.NotNull(scaleIn.GetCustomAttribute<Microsoft.AspNetCore.Mvc.HttpPostAttribute>());
        Assert.Equal("admin/workers/scale-in",
            scaleIn.GetCustomAttribute<Microsoft.AspNetCore.Mvc.RouteAttribute>()!.Template);
        Assert.Equal(PolicyNames.AdminAuthLevel,
            scaleIn.GetCustomAttribute<AuthorizeAttribute>()!.Policy);

        MethodInfo acknowledge = typeof(WorkerRemovalAcknowledgementController).GetMethod(
            nameof(WorkerRemovalAcknowledgementController.AcknowledgeWorkerRemoval))!;
        Assert.Equal("admin/functions/split/acknowledgeWorkerRemoval",
            acknowledge.GetCustomAttribute<Microsoft.AspNetCore.Mvc.RouteAttribute>()!.Template);
        Assert.Equal(PolicyNames.AdminAuthLevel,
            acknowledge.GetCustomAttribute<AuthorizeAttribute>()!.Policy);
        FunctionsSplitWorkerRemovalAcknowledgement acknowledgement =
            Assert.IsType<FunctionsSplitWorkerRemovalAcknowledgement>(
                JsonConvert.DeserializeObject<FunctionsSplitWorkerRemovalAcknowledgement>(
                    """
                    {
                      "action": {
                        "worker": {
                          "resourceNamespace": "resource-namespace",
                          "podName": "worker-pod-abc123",
                          "podGuid": "47bf3a08-900a-48b3-a0f1-f6baa87191e7",
                          "legionStampName": "legion-stamp",
                          "podType": "FunctionsSplitWorkerPod",
                          "app": {
                            "functionAppName": "app-name",
                            "functionAppSiteId": 123,
                            "antaresStampName": "antares-stamp"
                          }
                        },
                        "reason": "RuntimeUnlink"
                      }
                    }
                    """));
        Assert.True(acknowledgement.Action.IsValid());
    }

    [Fact]
    public void WorkerLinkErrorResponse_WithoutDetail_SerializesOnlyCode()
    {
        WorkerLinkErrorResponse response = new(new WorkerLinkError("LinkConflict"));

        string json = JsonConvert.SerializeObject(response);

        Assert.Equal("""{"error":{"code":"LinkConflict"}}""", json);
    }

    [Theory]
    [InlineData("workerId")]
    [InlineData("workerPodName")]
    public void WorkerLinkRequest_Serialization_UsesExactWorkerObjectNotLegacyBodyIdentity(string propertyName)
    {
        WorkerLinkRequest request = Assert.IsType<WorkerLinkRequest>(JsonConvert.DeserializeObject<WorkerLinkRequest>(
            $$"""
            {
              "workerGrpcEndpoint": "http://worker-pod-abc123:5001",
              "workerProxyStartupTime": "2026-10-30T01:02:03.4567890+00:00",
              "{{propertyName}}": null,
              "worker": {
                "resourceNamespace": "resource-namespace",
                "podName": "worker-pod-abc123",
                "podGuid": "47bf3a08-900a-48b3-a0f1-f6baa87191e7",
                "legionStampName": "legion-stamp",
                "podType": "FunctionsSplitWorkerPod",
                "app": {
                  "functionAppName": "app-name",
                  "functionAppSiteId": 123,
                  "antaresStampName": "antares-stamp"
                }
              }
            }
            """));

        string json = JsonConvert.SerializeObject(request);
        JObject body = JObject.Parse(json);
        WorkerLinkRequest roundTrip = Assert.IsType<WorkerLinkRequest>(
            JsonConvert.DeserializeObject<WorkerLinkRequest>(json));

        Assert.Equal("http://worker-pod-abc123:5001", request.WorkerGrpcEndpoint);
        Assert.Equal(
            new DateTimeOffset(2026, 10, 30, 1, 2, 3, 456, TimeSpan.Zero).AddTicks(7890),
            request.WorkerProxyStartupTime);
        Assert.True(request.Worker!.IsValid());
        Assert.Equal(request.Worker, roundTrip.Worker);
        Assert.Equal(request.WorkerProxyStartupTime, roundTrip.WorkerProxyStartupTime);
        Assert.Null(body.Property(propertyName));
    }

    [Fact]
    public void WorkerLinkRequest_Serialization_DoesNotIncludeRemovedHttpEndpoint()
    {
        WorkerLinkRequest request = Assert.IsType<WorkerLinkRequest>(JsonConvert.DeserializeObject<WorkerLinkRequest>(
            """{"workerGrpcEndpoint":"http://worker-pod-abc123:5001","workerHttpEndpoint":"http://unused:28080"}"""));

        JObject body = JObject.Parse(JsonConvert.SerializeObject(request));

        Assert.Equal(new[] { "worker", "workerContainerEncryptionKey", "workerGrpcEndpoint", "workerProxyStartupTime" },
            body.Properties().Select(property => property.Name).Order(StringComparer.Ordinal));
    }
}
