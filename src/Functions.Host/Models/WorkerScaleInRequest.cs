// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Newtonsoft.Json;

namespace Azure.Functions.Host.WorkerScaleIn;

/// <summary>
/// Requests one authoritative count-only Worker scale-in decision.
/// </summary>
public sealed class WorkerScaleInRequest
{
    /// <summary>
    /// Gets or sets the exact function group to scale in.
    /// </summary>
    [JsonProperty("functionGroup")]
    public string? FunctionGroup { get; set; }

    /// <summary>
    /// Gets or sets the exact always-ready capacity class to scale in.
    /// </summary>
    [JsonProperty("isAlwaysReady")]
    public bool? IsAlwaysReady { get; set; }

    /// <summary>
    /// Gets or sets the absolute number of matching usable Workers to retain for this decision.
    /// </summary>
    [JsonProperty("desiredWorkerCount")]
    public int? DesiredWorkerCount { get; set; }
}

/// <summary>
/// Reports how many Workers this request newly committed to drain.
/// </summary>
/// <param name="NewlyDrainingWorkerCount">The newly committed Worker drain count.</param>
public sealed record WorkerScaleInResponse(
    [property: JsonProperty("newlyDrainingWorkerCount")]
    int NewlyDrainingWorkerCount);
