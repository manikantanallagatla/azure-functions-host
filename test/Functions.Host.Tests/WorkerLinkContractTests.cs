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
                          "podName": "worker-pod-abc123"
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

    [Fact]
    public void WorkerLinkRequest_Serialization_UsesRequiredReducedIdentity()
    {
        WorkerLinkRequest request = Assert.IsType<WorkerLinkRequest>(JsonConvert.DeserializeObject<WorkerLinkRequest>(
            """
            {
              "workerGrpcEndpoint": "http://worker-pod-abc123:5001",
              "workerNamespace": "resource-namespace",
              "workerPodName": "worker-pod-abc123",
              "app": {
                "functionAppName": "legacy-app",
                "functionAppSiteId": 123,
                "antaresStampName": "legacy-stamp"
              },
              "workerId": "legacy-worker",
              "worker": {
                "resourceNamespace": "legacy-namespace"
              }
            }
            """));

        string json = JsonConvert.SerializeObject(request);
        JObject body = JObject.Parse(json);
        WorkerLinkRequest roundTrip = Assert.IsType<WorkerLinkRequest>(
            JsonConvert.DeserializeObject<WorkerLinkRequest>(json));

        Assert.Equal("http://worker-pod-abc123:5001", request.WorkerGrpcEndpoint);
        Assert.Equal("resource-namespace", request.WorkerNamespace);
        Assert.Equal("worker-pod-abc123", request.WorkerPodName);
        Assert.Equal(request.WorkerNamespace, roundTrip.WorkerNamespace);
        Assert.Equal(request.WorkerPodName, roundTrip.WorkerPodName);
        Assert.Null(body.Property("app"));
        Assert.Null(body.Property("worker"));
        Assert.Null(body.Property("workerId"));
    }

    [Fact]
    public void WorkerLinkRequest_Serialization_DoesNotIncludeRemovedHttpEndpoint()
    {
        WorkerLinkRequest request = Assert.IsType<WorkerLinkRequest>(JsonConvert.DeserializeObject<WorkerLinkRequest>(
            """
            {
              "workerGrpcEndpoint": "http://worker-pod-abc123:5001",
              "workerHttpEndpoint": "http://unused:28080",
              "workerNamespace": "resource-namespace",
              "workerPodName": "worker-pod-abc123"
            }
            """));

        JObject body = JObject.Parse(JsonConvert.SerializeObject(request));

        Assert.Equal(new[] { "workerContainerEncryptionKey", "workerGrpcEndpoint", "workerNamespace", "workerPodName" },
            body.Properties().Select(property => property.Name).Order(StringComparer.Ordinal));
    }
}
