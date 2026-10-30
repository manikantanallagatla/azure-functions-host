// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace Azure.Functions.Rpc.Client;

internal interface IWorkerRemovalHandoffChannel
{
    bool IsWorkerRemovalHandoffConfirmed { get; }
}
