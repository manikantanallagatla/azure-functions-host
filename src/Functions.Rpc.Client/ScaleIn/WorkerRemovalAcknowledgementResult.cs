// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Describes the result of matching a worker-removal acknowledgement.
/// </summary>
public enum WorkerRemovalAcknowledgementResult
{
    /// <summary>
    /// The outstanding action was atomically removed.
    /// </summary>
    Accepted,

    /// <summary>
    /// The exact action was already accepted and remains in bounded replay evidence.
    /// </summary>
    Duplicate,

    /// <summary>
    /// No outstanding or recently completed exact action matched.
    /// </summary>
    NotFound,
}
