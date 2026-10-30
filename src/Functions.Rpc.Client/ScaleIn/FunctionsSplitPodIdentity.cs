// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Identifies one Functions split worker for lifecycle removal.
/// </summary>
/// <param name="ResourceNamespace">The Legion resource namespace.</param>
/// <param name="PodName">The pod name.</param>
public sealed record FunctionsSplitPodIdentity(
    string ResourceNamespace,
    string PodName)
{
    /// <summary>
    /// Determines whether every required identity component is present and bounded.
    /// </summary>
    /// <returns><see langword="true"/> when this is a complete worker identity.</returns>
    public bool IsValid()
        => IsValidString(ResourceNamespace) &&
            IsValidString(PodName);

    private static bool IsValidString(string value)
        => !string.IsNullOrWhiteSpace(value) && value.Length <= 256;
}

/// <summary>
/// Represents one immutable Runtime fallback request to remove a worker by exact coordinates.
/// </summary>
/// <param name="Worker">The exact worker identity.</param>
/// <param name="Reason">The frozen bounded removal reason.</param>
public sealed record FunctionsSplitWorkerRemovalAction(
    FunctionsSplitPodIdentity Worker,
    string Reason)
{
    /// <summary>
    /// Determines whether the worker identity and reason form a valid removal action.
    /// </summary>
    /// <returns><see langword="true"/> when the action is complete and bounded.</returns>
    public bool IsValid()
        => Worker is not null &&
            Worker.IsValid() &&
            !string.IsNullOrWhiteSpace(Reason) &&
            Reason.Length <= 128;
}

/// <summary>
/// Acknowledges Legion's accepted deletion handoff for one exact removal action.
/// </summary>
/// <param name="Action">The exact action previously published by Runtime.</param>
public sealed record FunctionsSplitWorkerRemovalAcknowledgement(
    FunctionsSplitWorkerRemovalAction Action);
