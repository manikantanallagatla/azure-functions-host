// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Azure.Functions.Host.RuntimeDrain;

namespace Azure.Functions.Host.HostState;

/// <summary>
/// Coordinates one in-memory app-version Runtime drain and one optional Phased-to-Immediate escalation.
/// </summary>
public interface IAppVersionRuntimeDrainManager
{
    /// <summary>
    /// Accepts an app-version Runtime drain after committing local admission closure and the AppServer fence.
    /// </summary>
    /// <param name="operationId">The caller-owned idempotency identifier.</param>
    /// <param name="mode">The requested Host-local scheduling mode.</param>
    /// <param name="reason">The external operation reason retained for idempotency and diagnostics.</param>
    /// <returns>The immutable acceptance receipt.</returns>
    Task<RuntimeDrainReceipt> AcceptAsync(
        Guid operationId,
        RuntimeDrainMode mode,
        RuntimeDrainReason reason);
}
