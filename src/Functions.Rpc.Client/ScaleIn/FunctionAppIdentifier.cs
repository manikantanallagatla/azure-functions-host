// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Identifies one Function App incarnation in the Functions split-pod contract.
/// </summary>
/// <param name="FunctionAppName">The Function App name.</param>
/// <param name="FunctionAppSiteId">The Function App site identifier.</param>
/// <param name="AntaresStampName">The Antares stamp name.</param>
public sealed record FunctionAppIdentifier(
    string FunctionAppName,
    int FunctionAppSiteId,
    string AntaresStampName);

/// <summary>
/// Identifies one exact Functions split pod incarnation.
/// </summary>
/// <param name="ResourceNamespace">The Legion resource namespace.</param>
/// <param name="PodName">The pod name.</param>
/// <param name="PodGuid">The pod incarnation identifier.</param>
/// <param name="LegionStampName">The Legion stamp name.</param>
/// <param name="PodType">The Functions pod type.</param>
/// <param name="App">The owning Function App identity.</param>
public sealed record FunctionsSplitPodIdentity(
    string ResourceNamespace,
    string PodName,
    Guid PodGuid,
    string LegionStampName,
    string PodType,
    FunctionAppIdentifier App)
{
    /// <summary>
    /// Gets the wire value for a Functions split worker pod.
    /// </summary>
    public const string WorkerPodType = "FunctionsSplitWorkerPod";

    /// <summary>
    /// Determines whether every required identity component is present and bounded.
    /// </summary>
    /// <returns><see langword="true"/> when this is a complete worker identity.</returns>
    public bool IsValid()
        => IsValidString(ResourceNamespace) &&
            IsValidString(PodName) &&
            PodGuid != Guid.Empty &&
            IsValidString(LegionStampName) &&
            string.Equals(PodType, WorkerPodType, StringComparison.Ordinal) &&
            App is not null &&
            IsValidString(App.FunctionAppName) &&
            App.FunctionAppSiteId > 0 &&
            IsValidString(App.AntaresStampName);

    private static bool IsValidString(string value)
        => !string.IsNullOrWhiteSpace(value) && value.Length <= 256;
}

/// <summary>
/// Represents one immutable Runtime fallback request to remove an exact worker incarnation.
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
