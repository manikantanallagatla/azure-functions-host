// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Script.Grpc.WorkerLink;

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Freezes the platform runtime identity and acknowledges completed worker initialization.
/// </summary>
internal sealed class WorkerLinkAcceptance(IRpcClientFactory clients, string runtimeIdentity)
{
    private readonly RuntimeIdentity _runtime = string.IsNullOrWhiteSpace(runtimeIdentity)
        ? null
        : RuntimeIdentity.Parser.ParseJson(runtimeIdentity);

    public Task AcceptAsync(Uri endpoint, string initializationId, CancellationToken cancellationToken)
    {
        return _runtime is null
            ? Task.CompletedTask
            : clients.AcceptLinkAsync(endpoint, new AcceptLinkRequest
            {
                InitializationId = initializationId,
                Runtime = _runtime.Clone()
            }, cancellationToken);
    }
}
