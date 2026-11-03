// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Azure.Functions.WorkerProxy.State;
using Xunit;

namespace Azure.Functions.WorkerProxy.Tests.State;

public partial class WorkerPodStateManagerTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Removal_RetainsOnlyEstablishedLinkAndRejectsLateLifecycleUpdates(bool initialized)
    {
        WorkerPodStateManager manager = new(CreateOptions(), TimeProvider.System);
        SplitPodIdentity runtime = new("runtime-ns", "runtime-pod", new("app", 42, "stamp"));
        Assert.Equal(WorkerAssignmentResult.Created, manager.Assign(CreateAssignment()));
        Assert.True(manager.OnRuntimeAttached(1, runtime));
        Assert.True(manager.OnWorkerAttached(1));
        Assert.True(manager.OnWorkerStartStream(1, "worker"));
        if (initialized)
        {
            Assert.True(manager.OnRuntimeInitialized(1));
        }

        WorkerPodState before = manager.State;
        Assert.True(manager.RequestRemoval());
        WorkerPodState removed = manager.State;
        SplitWorkerOriginState checkpoint = SplitWorkerOriginState.FromState(removed);

        Assert.Equal(before.Revision + 1, removed.Revision);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, checkpoint.Status);
        Assert.Equal(initialized ? SplitWorkerLinkState.Linked : SplitWorkerLinkState.Unlinked, checkpoint.Link.State);
        Assert.Equal(initialized ? runtime : null, checkpoint.Link.Runtime);
        Assert.False(removed.IsWorkerReady);
        Assert.False(removed.IsRuntimeInitialized);
        Assert.Equal(WorkerPodStatus.None, removed.PodStatus);
        Assert.Null(removed.SessionId);
        Assert.Null(removed.WorkerId);
        Assert.Null(removed.StartupMode);
        Assert.Null(removed.FunctionAppName);
        Assert.Null(removed.FunctionGroupName);
        Assert.Null(removed.IsAlwaysReady);
        Assert.False(manager.RequestRemoval());
        Assert.False(manager.OnSessionTerminated(1));
        Assert.False(manager.OnRuntimeAttached(2, runtime));
        Assert.False(manager.OnWorkerAttached(2));
        Assert.False(manager.OnWorkerStartStream(1, "late-worker"));
        Assert.False(manager.OnRuntimeInitialized(1));
        Assert.Equal(WorkerAssignmentResult.WorkerNotReady, manager.Assign(CreateAssignment()));
        Assert.Same(removed, manager.State);
    }

    [Fact]
    public void Removal_DoesNotCreditInitializationBeforeAssignment()
    {
        WorkerPodStateManager manager = new(CreateOptions(), TimeProvider.System);
        Assert.True(manager.OnRuntimeAttached(1, new("ns", "runtime", new("app", 42, "stamp"))));
        Assert.True(manager.OnWorkerAttached(1));
        Assert.True(manager.OnWorkerStartStream(1, "worker"));
        Assert.False(manager.OnRuntimeInitialized(1));
        Assert.Equal(WorkerAssignmentResult.Created, manager.Assign(CreateAssignment()));
        Assert.False(manager.State.IsRuntimeInitialized);

        Assert.True(manager.RequestRemoval());

        SplitWorkerOriginState removed = SplitWorkerOriginState.FromState(manager.State);
        Assert.Equal(SplitWorkerStatus.RemovalRequested, removed.Status);
        Assert.Equal(SplitWorkerLinkState.Unlinked, removed.Link.State);
        Assert.Null(removed.Link.Runtime);
    }

    [Fact]
    public async Task Removal_RacingAssignmentCannotRestoreAdmission()
    {
        WorkerPodStateManager manager = new(CreateOptions(), TimeProvider.System);
        Task<WorkerAssignmentResult> assignment = Task.Run(() => manager.Assign(CreateAssignment()));
        Task<bool> removal = Task.Run(manager.RequestRemoval);

        await Task.WhenAll(assignment, removal);

        Assert.True(removal.Result);
        Assert.Contains(assignment.Result, new[] { WorkerAssignmentResult.Created, WorkerAssignmentResult.WorkerNotReady });
        WorkerPodState removed = manager.State;
        Assert.True(removed.RemovalRequested);
        Assert.False(removed.IsWorkerReady);
        Assert.Equal(WorkerAssignmentState.Unassigned, removed.AssignmentState);
        Assert.Equal(WorkerAssignmentResult.WorkerNotReady, manager.Assign(CreateAssignment()));
        Assert.Same(removed, manager.State);
    }
}
