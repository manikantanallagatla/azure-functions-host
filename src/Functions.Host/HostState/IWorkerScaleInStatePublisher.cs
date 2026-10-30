// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Rpc.Client;

namespace Azure.Functions.Host.HostState;

/// <summary>
/// Publishes the ordinary Host capacity snapshot after authoritative Worker scale-in.
/// </summary>
public interface IWorkerScaleInStatePublisher
{
    /// <summary>
    /// Publishes one ordinary Host capacity snapshot without the autonomous R2 capacity fence.
    /// </summary>
    /// <param name="state">The resulting Host capacity snapshot.</param>
    /// <param name="cancellationToken">The publication cancellation token.</param>
    /// <returns><see langword="true"/> when AppServer accepted the ordinary snapshot.</returns>
    Task<bool> PublishAuthoritativeScaleInAsync(
        HttpWorkerScaleInState state,
        CancellationToken cancellationToken);
}
