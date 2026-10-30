// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.Functions.Rpc.Client;

internal interface IAppVersionRuntimeDrainWorkerManager
{
    RuntimeIncarnation Incarnation { get; }

    bool TryStartAppVersionDrain(out AppVersionRuntimeDrainStart start);

    AppVersionRuntimeDrainSelection SelectWorkers(
        RuntimeIncarnation incarnation,
        IReadOnlyList<string> workerIds,
        DateTimeOffset invocationGraceDeadline);

    AppVersionRuntimeDrainSelection SelectAllRemainingWorkers(
        RuntimeIncarnation incarnation,
        DateTimeOffset invocationGraceDeadline);
}

internal sealed record AppVersionRuntimeDrainStart(
    RuntimeIncarnation Incarnation,
    IReadOnlyList<string> NonHttpWorkerIds,
    IReadOnlyList<string> HttpWorkerIds,
    IReadOnlyList<string> NonReadyWorkerIds,
    IReadOnlyList<Task<bool>> ExistingDrains);

internal sealed record AppVersionRuntimeDrainSelection(
    IReadOnlyList<Task<bool>> DrainTasks);
