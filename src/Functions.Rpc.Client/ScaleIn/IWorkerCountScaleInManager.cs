// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Provides authoritative count-only Worker scale-in for one exact function group and capacity class.
/// </summary>
public interface IWorkerCountScaleInManager
{
    /// <summary>
    /// Atomically commits the excess invocation-ready Workers matching one exact function group and capacity class.
    /// </summary>
    /// <param name="functionGroup">The exact function group assigned by WorkerProxy.</param>
    /// <param name="isAlwaysReady">The exact always-ready capacity class assigned by WorkerProxy.</param>
    /// <param name="desiredWorkerCount">The absolute number of matching usable Workers to retain for this decision.</param>
    /// <returns>The newly committed drain count and resulting Host capacity snapshot.</returns>
    WorkerCountScaleInResult ScaleInWorkers(
        string functionGroup,
        bool isAlwaysReady,
        int desiredWorkerCount);
}

/// <summary>
/// Represents one authoritative count-only Worker scale-in decision.
/// </summary>
/// <param name="NewlyDrainingWorkerCount">The number of Workers newly committed to drain by this decision.</param>
/// <param name="ResultingState">The Host capacity snapshot after those commitments close new invocation admission.</param>
public sealed record WorkerCountScaleInResult(
    int NewlyDrainingWorkerCount,
    HttpWorkerScaleInState ResultingState);
