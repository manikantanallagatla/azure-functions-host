// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Threading.Tasks;
using Azure.Functions.Host.RuntimeDrain;

namespace Azure.Functions.Host.HostState;

/// <summary>
/// Coordinates one in-memory Runtime drain and one optional Phased-to-Immediate escalation.
/// </summary>
public interface IAppVersionRuntimeDrainManager
{
    /// <summary>
    /// Accepts a Runtime drain after committing local admission closure and the AppServer fence.
    /// </summary>
    /// <param name="mode">The requested Host-local scheduling mode.</param>
    /// <param name="reason">The external operation reason retained for diagnostics.</param>
    /// <returns>The immutable acceptance receipt.</returns>
    Task<RuntimeDrainReceipt> AcceptAsync(
        RuntimeDrainMode mode,
        RuntimeDrainReason reason);
}
