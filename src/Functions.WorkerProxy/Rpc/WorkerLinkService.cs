// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.State;
using Grpc.Core;
using Microsoft.Azure.WebJobs.Script.Grpc.WorkerLink;
using Microsoft.Extensions.Options;
using GrpcRpcException = Grpc.Core.RpcException;

namespace Azure.Functions.WorkerProxy.Rpc;

/// <summary>
/// Receives platform Link acknowledgements on the runtime-facing listener only.
/// Authentication is supplied by the runtime-to-WorkerProxy transport dependency.
/// </summary>
internal sealed class WorkerLinkService(
    FunctionRpcRelay relay,
    WorkerProxyEndpointConfiguration endpoints,
    IOptions<WorkerProxyOptions> options) : WorkerLink.WorkerLinkBase
{
    public override Task<AcceptLinkResponse> Accept(AcceptLinkRequest request, ServerCallContext context)
    {
        if (!options.Value.EnableLinkLifecycle ||
            !endpoints.TryGetRelaySide(context.GetHttpContext().Connection.LocalPort, out FunctionRpcRelaySide side) ||
            side != FunctionRpcRelaySide.Runtime)
        {
            throw new GrpcRpcException(new Status(StatusCode.Unimplemented, "Link acceptance is unavailable on this listener."));
        }

        RuntimeIdentity? runtime = request.Runtime;
        if (string.IsNullOrWhiteSpace(request.InitializationId) ||
            runtime is null || string.IsNullOrWhiteSpace(runtime.ResourceNamespace) ||
            string.IsNullOrWhiteSpace(runtime.PodName) ||
            !Guid.TryParse(runtime.PodGuid, out Guid podGuid) || podGuid == Guid.Empty ||
            string.IsNullOrWhiteSpace(runtime.LegionStampName) ||
            !string.Equals(runtime.PodType, "FunctionsSplitRuntimePod", StringComparison.Ordinal) ||
            runtime.App is not { } app || string.IsNullOrWhiteSpace(app.FunctionAppName) ||
            app.FunctionAppSiteId <= 0 || string.IsNullOrWhiteSpace(app.AntaresStampName))
        {
            throw new GrpcRpcException(new Status(StatusCode.InvalidArgument, "A complete runtime identity and initialization ID are required."));
        }

        context.CancellationToken.ThrowIfCancellationRequested();
        SplitPodIdentity identity = new(runtime.ResourceNamespace, runtime.PodName, podGuid,
            runtime.LegionStampName, runtime.PodType, new(app.FunctionAppName, app.FunctionAppSiteId, app.AntaresStampName));
        if (!relay.AcceptLink(request.InitializationId, identity))
        {
            throw new GrpcRpcException(new Status(StatusCode.FailedPrecondition, "The initialized attachment is no longer available or has a different runtime."));
        }

        return Task.FromResult(new AcceptLinkResponse());
    }
}
