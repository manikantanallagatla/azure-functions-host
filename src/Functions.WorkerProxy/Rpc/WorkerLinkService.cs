// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
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

        if (string.IsNullOrWhiteSpace(request.InitializationId))
        {
            throw new GrpcRpcException(new Status(StatusCode.InvalidArgument, "An initialization ID is required."));
        }

        context.CancellationToken.ThrowIfCancellationRequested();
        if (!relay.AcceptLink(request.InitializationId))
        {
            throw new GrpcRpcException(new Status(StatusCode.FailedPrecondition, "The initialized attachment is no longer available."));
        }

        return Task.FromResult(new AcceptLinkResponse());
    }
}
