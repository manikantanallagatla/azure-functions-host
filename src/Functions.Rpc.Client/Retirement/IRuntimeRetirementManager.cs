// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.Functions.Rpc.Client;

internal interface IRuntimeRetirementManager
{
    RuntimeIncarnation Incarnation { get; }

    Task RequestRetirementAsync(SplitScriptHostActivationFaulted condition);
}

internal interface IRuntimeRetirementWorkerManager
{
    bool IsRetirementRequested { get; }

    RuntimeIncarnation Incarnation { get; }

    bool TryCloseAdmission(SplitScriptHostActivationFaulted condition);

    Task<RuntimeRetirementDrainResult> DrainAllWorkersAsync(
        RuntimeIncarnation incarnation,
        CancellationToken cancellationToken);
}

internal readonly record struct RuntimeIncarnation(Guid Value)
{
    public static RuntimeIncarnation Create()
        => new(Guid.NewGuid());
}

internal sealed record RuntimeRetirementDrainResult(bool IsComplete, string Failure);

internal sealed record SplitScriptHostActivationFaulted(
    RuntimeIncarnation Incarnation,
    Exception Exception);
