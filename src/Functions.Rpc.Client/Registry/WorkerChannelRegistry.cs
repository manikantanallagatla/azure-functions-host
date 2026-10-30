// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Azure.WebJobs.Script.ManagedDependencies;
using Microsoft.Extensions.Logging;

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Owns initialized client-backed worker channels, ScriptHost setup, and terminal cleanup.
/// </summary>
/// <remarks>
/// Admission and replay decisions are atomic. Different workers can link concurrently.
/// The registry publishes a channel after its WorkerInit handshake succeeds, configures it for the active ScriptHost
/// context, and retains terminal identities. Initialized channels remain available for metadata bootstrap before a
/// ScriptHost function set is known; only ready channels are available for invocation dispatch.
/// </remarks>
internal sealed partial class WorkerChannelRegistry : IWorkerChannelRegistry, IHttpWorkerScaleInManager,
    IWorkerCountScaleInManager, IRuntimeRetirementWorkerManager, IAppVersionRuntimeDrainWorkerManager
{
    private const int CompletedWorkerRemovalCapacity = 128;
    private const int DefaultHttpWorkerCapacity = 16;
    private const string UnlinkRemovalReason = "RuntimeUnlink";

    private static readonly TimeSpan DefaultLinkTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan DefaultUnlinkTimeout = TimeSpan.FromSeconds(10);

    private readonly IRpcClientWorkerChannelFactory _channelFactory;
    private readonly HashSet<FunctionsSplitWorkerRemovalAction> _completedWorkerRemovals = [];
    private readonly Queue<FunctionsSplitWorkerRemovalAction> _completedWorkerRemovalOrder = [];
    private readonly Lock _disposeLock = new();
    private readonly IDuplexChannelFactory<StreamingMessage> _duplexChannelFactory;
    private readonly ILogger<WorkerChannelRegistry> _logger;
    private readonly Dictionary<string, WorkerSlot> _linkAttempts = new(StringComparer.Ordinal);
    private readonly TimeSpan _linkTimeout;
    private readonly HashSet<Task> _monitorTasks = [];
    private readonly List<FunctionsSplitWorkerRemovalAction> _outstandingWorkerRemovals = [];
    private readonly HashSet<FunctionsSplitWorkerRemovalAction> _outstandingWorkerRemovalSet = [];
    private readonly CancellationTokenSource _shutdownSource = new();
    private readonly Dictionary<string, WorkerSlot> _slots = new(StringComparer.Ordinal);
    private readonly Lock _stateLock = new();
    private readonly RuntimeIncarnation _runtimeIncarnation = RuntimeIncarnation.Create();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _unlinkTimeout;
    private ChannelSetupContext _activeSetupContext;
    private Task _disposeTask;
    private bool _disposed;
    private TaskCompletionSource _initializedChannelAvailable = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _scaleInHttpCapacity;
    private int _scaleInLinkedWorkerCount;
    private DateTimeOffset _scaleInStateCreatedTime;
    private TaskCompletionSource<long> _scaleInStateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _scaleInStateVersion;
    private Task<RuntimeRetirementDrainResult> _runtimeRetirementDrainTask;
    private RuntimeRetirementOwner _runtimeRetirementOwner;
    private bool _runtimeRetirementRequested;

    // Setup writers hold _stateLock; ready-channel readers observe this published signal without taking that lock.
    private TaskCompletionSource<IReadOnlyList<WorkerChannel>> _readyChannelsAvailable = CreateReadyChannelsSignal();

    public WorkerChannelRegistry(
        IDuplexChannelFactory<StreamingMessage> duplexChannelFactory,
        IRpcClientWorkerChannelFactory channelFactory,
        ILogger<WorkerChannelRegistry> logger)
        : this(duplexChannelFactory, channelFactory, logger, DefaultLinkTimeout, TimeProvider.System)
    {
    }

    public WorkerChannelRegistry(
        IDuplexChannelFactory<StreamingMessage> duplexChannelFactory,
        IRpcClientWorkerChannelFactory channelFactory,
        ILogger<WorkerChannelRegistry> logger,
        TimeProvider timeProvider)
        : this(duplexChannelFactory, channelFactory, logger, DefaultLinkTimeout, timeProvider)
    {
    }

    internal WorkerChannelRegistry(
        IDuplexChannelFactory<StreamingMessage> duplexChannelFactory,
        IRpcClientWorkerChannelFactory channelFactory,
        ILogger<WorkerChannelRegistry> logger,
        TimeSpan linkTimeout,
        TimeProvider timeProvider = null,
        TimeSpan? unlinkTimeout = null)
    {
        _duplexChannelFactory = duplexChannelFactory ?? throw new ArgumentNullException(nameof(duplexChannelFactory));
        _channelFactory = channelFactory ?? throw new ArgumentNullException(nameof(channelFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _timeProvider = timeProvider ?? TimeProvider.System;
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(linkTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(linkTimeout.TotalMilliseconds, uint.MaxValue - 1);
        _linkTimeout = linkTimeout;
        _unlinkTimeout = unlinkTimeout ?? DefaultUnlinkTimeout;
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(_unlinkTimeout, TimeSpan.Zero);
        _scaleInStateCreatedTime = _timeProvider.GetUtcNow();
    }

    private enum RuntimeRetirementOwner
    {
        None,
        ActivationFault,
        AppVersion,
    }

    HttpWorkerScaleInState IHttpWorkerScaleInManager.Current
    {
        get
        {
            lock (_stateLock)
            {
                RefreshScaleInStateLocked();
                return GetScaleInStateLocked();
            }
        }
    }

    long IHttpWorkerScaleInManager.Version
    {
        get
        {
            lock (_stateLock)
            {
                RefreshScaleInStateLocked();
                return _scaleInStateVersion;
            }
        }
    }

    RuntimeIncarnation IRuntimeRetirementWorkerManager.Incarnation => _runtimeIncarnation;

    RuntimeIncarnation IAppVersionRuntimeDrainWorkerManager.Incarnation => _runtimeIncarnation;

    bool IRuntimeRetirementWorkerManager.IsRetirementRequested
    {
        get
        {
            lock (_stateLock)
            {
                return _runtimeRetirementRequested;
            }
        }
    }

    WorkerRemovalAcknowledgementResult IHttpWorkerScaleInManager.AcknowledgeWorkerRemoval(
        FunctionsSplitWorkerRemovalAction action)
    {
        ArgumentNullException.ThrowIfNull(action);
        if (!action.IsValid())
        {
            throw new ArgumentException("The worker-removal action is incomplete or invalid.", nameof(action));
        }

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_outstandingWorkerRemovalSet.Remove(action))
            {
                _outstandingWorkerRemovals.Remove(action);
                AddCompletedWorkerRemovalLocked(action);
                SignalScaleInStateChangedLocked();

                return WorkerRemovalAcknowledgementResult.Accepted;
            }

            return _completedWorkerRemovals.Contains(action)
                ? WorkerRemovalAcknowledgementResult.Duplicate
                : WorkerRemovalAcknowledgementResult.NotFound;
        }
    }

    public Task<WorkerLinkResult> LinkAsync(
        string workerId,
        Uri grpcEndpoint,
        CancellationToken cancellationToken = default)
        => LinkAsyncCore(workerId, grpcEndpoint, worker: null, workerProxyStartupTime: null, cancellationToken);

    public Task<WorkerLinkResult> LinkAsync(
        string workerId,
        Uri grpcEndpoint,
        FunctionsSplitPodIdentity worker,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(worker);
        if (!worker.IsValid())
        {
            throw new ArgumentException("The worker identity is incomplete or invalid.", nameof(worker));
        }

        if (!string.Equals(workerId, worker.PodName, StringComparison.Ordinal))
        {
            throw new ArgumentException("The route worker ID must exactly match the worker pod name.", nameof(worker));
        }

        return LinkAsyncCore(workerId, grpcEndpoint, worker, workerProxyStartupTime: null, cancellationToken);
    }

    public Task<WorkerLinkResult> LinkAsync(
        string workerId,
        Uri grpcEndpoint,
        FunctionsSplitPodIdentity worker,
        DateTimeOffset workerProxyStartupTime,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(worker);
        if (!worker.IsValid())
        {
            throw new ArgumentException("The worker identity is incomplete or invalid.", nameof(worker));
        }

        if (!string.Equals(workerId, worker.PodName, StringComparison.Ordinal))
        {
            throw new ArgumentException("The route worker ID must exactly match the worker pod name.", nameof(worker));
        }

        return LinkAsyncCore(workerId, grpcEndpoint, worker, workerProxyStartupTime, cancellationToken);
    }

    private Task<WorkerLinkResult> LinkAsyncCore(
        string workerId,
        Uri grpcEndpoint,
        FunctionsSplitPodIdentity worker,
        DateTimeOffset? workerProxyStartupTime,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);
        RpcClientFactory.ValidateEndpoint(grpcEndpoint);
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<WorkerChannel> linkCompletion;
        WorkerSlot slot;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_runtimeRetirementRequested)
            {
                throw new WorkerLinkException(
                    WorkerLinkFailureReason.RuntimeStopping,
                    "The Runtime is retiring and no longer accepts Worker Links.");
            }

            if (_linkAttempts.TryGetValue(workerId, out WorkerSlot existing))
            {
                if (!string.Equals(existing.Endpoint.AbsoluteUri, grpcEndpoint.AbsoluteUri, StringComparison.Ordinal))
                {
                    throw new WorkerLinkException(WorkerLinkFailureReason.Conflict,
                        "The worker is already associated with a different gRPC endpoint.");
                }

                if (worker is not null)
                {
                    if (existing.Worker is null || existing.Worker != worker)
                    {
                        throw new WorkerLinkException(WorkerLinkFailureReason.Conflict,
                            "The worker is already associated with a different physical identity.");
                    }
                }

                if (workerProxyStartupTime is not null)
                {
                    if (existing.WorkerProxyStartupTime is not null &&
                        existing.WorkerProxyStartupTime != workerProxyStartupTime)
                    {
                        throw new WorkerLinkException(WorkerLinkFailureReason.Conflict,
                            "The worker is already associated with a different original startup time.");
                    }

                    existing.WorkerProxyStartupTime ??= workerProxyStartupTime;
                }

                if (existing.Terminal || existing.Channel is { IsTerminated: true })
                {
                    throw new WorkerLinkException(WorkerLinkFailureReason.WorkerTerminated,
                        "The worker's initialized channel has terminated.");
                }

                return CompleteLinkAsync(existing.LinkTask.WaitAsync(_linkTimeout, cancellationToken), isNewLink: false);
            }

            linkCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            slot = new()
            {
                Endpoint = grpcEndpoint,
                LinkTask = linkCompletion.Task,
                Worker = worker,
                WorkerProxyStartupTime = workerProxyStartupTime,
            };
            _slots.Add(workerId, slot);
            _linkAttempts.Add(workerId, slot);
        }

        // The shared completion is published before I/O begins, and connection work runs outside the state lock.
        _ = LinkAndCompleteAsync(workerId, grpcEndpoint, slot, linkCompletion, cancellationToken);

        return CompleteLinkAsync(linkCompletion.Task, isNewLink: true);
    }

    private static async Task<WorkerLinkResult> CompleteLinkAsync(Task<WorkerChannel> link, bool isNewLink)
        => new(await link.ConfigureAwait(false), isNewLink);

    public Task<long> WaitForChangeAsync(long version, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            RefreshScaleInStateLocked();
            if (_scaleInStateVersion != version)
            {
                return Task.FromResult(_scaleInStateVersion);
            }

            return _scaleInStateChanged.Task.WaitAsync(cancellationToken);
        }
    }

    public HttpWorkerScaleInReservation TryReserveIdleWorker(TimeSpan minimumIdleDuration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumIdleDuration, TimeSpan.Zero);

        lock (_stateLock)
        {
            if (_disposed || _runtimeRetirementRequested)
            {
                return null;
            }

            foreach ((string workerId, WorkerSlot slot) in _slots)
            {
                if (slot.Channel is not { } channel ||
                    slot.DrainTask is not null ||
                    !channel.TryReserveIdleScaleIn(minimumIdleDuration, out Guid reservationId))
                {
                    continue;
                }

                SignalReadyChannelsLocked();
                SignalScaleInStateChangedLocked();
                return new(workerId, reservationId, GetScaleInStateLocked());
            }

            return null;
        }
    }

    public bool TryCommitReservation(
        HttpWorkerScaleInReservation reservation,
        out Task<bool> drainCompletion)
    {
        ArgumentNullException.ThrowIfNull(reservation);

        Task invocationDrainTask;
        TaskCompletionSource<bool> completion;
        WorkerSlot slot;
        lock (_stateLock)
        {
            RefreshScaleInStateLocked();
            if (_disposed ||
                _runtimeRetirementRequested ||
                _scaleInStateVersion != reservation.ProjectedState.Version ||
                _scaleInHttpCapacity != reservation.ProjectedState.HttpCapacity ||
                _scaleInLinkedWorkerCount != reservation.ProjectedState.LinkedWorkerCount ||
                !_slots.TryGetValue(reservation.WorkerId, out slot) ||
                slot.Channel is not { } channel ||
                slot.DrainTask is not null ||
                channel.CommitScaleInReservation(reservation.ReservationId) is not { } committedInvocations)
            {
                drainCompletion = null;
                return false;
            }

            invocationDrainTask = committedInvocations;
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            drainCompletion = completion.Task;
            slot.DrainTask = drainCompletion;
            SignalReadyChannelsLocked();
        }

        _ = CompleteDrainAsync(reservation.WorkerId, slot, invocationDrainTask, completion);
        return true;
    }

    public bool CancelReservation(HttpWorkerScaleInReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);

        lock (_stateLock)
        {
            if (_disposed ||
                !_slots.TryGetValue(reservation.WorkerId, out WorkerSlot slot) ||
                slot.Channel is not { } channel ||
                !channel.CancelScaleInReservation(reservation.ReservationId))
            {
                return false;
            }

            SignalReadyChannelsLocked();
            SignalScaleInStateChangedLocked();
            return true;
        }
    }

    public WorkerCountScaleInResult ScaleInWorkers(
        string functionGroup,
        bool isAlwaysReady,
        int desiredWorkerCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(functionGroup);
        ArgumentOutOfRangeException.ThrowIfNegative(desiredWorkerCount);

        List<CountScaleInSelection> selections = [];
        HttpWorkerScaleInState resultingState;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            RefreshScaleInStateLocked();
            if (_runtimeRetirementRequested)
            {
                Log.CountScaleInSkippedForRuntimeRetirement(_logger, functionGroup, isAlwaysReady);
                return new(0, GetScaleInStateLocked());
            }

            List<CountScaleInCandidate> candidates = [];
            foreach ((string workerId, WorkerSlot slot) in _slots)
            {
                if (slot.Channel is not { } channel ||
                    slot.DrainTask is not null ||
                    !channel.TryGetCountScaleInCandidate(functionGroup, isAlwaysReady, out bool isIdle))
                {
                    continue;
                }

                candidates.Add(new(workerId, slot, isIdle, slot.WorkerProxyStartupTime));
            }

            int excess = Math.Max(0, candidates.Count - desiredWorkerCount);
            if (excess == 0)
            {
                return new(0, GetScaleInStateLocked());
            }

            string retainedWorkerId = null;
            if (desiredWorkerCount > 0 &&
                string.Equals(functionGroup, FunctionGroups.Durable, StringComparison.OrdinalIgnoreCase))
            {
                if (candidates.Any(static candidate => candidate.WorkerProxyStartupTime is null))
                {
                    Log.CountScaleInOrderingUnavailable(
                        _logger,
                        functionGroup,
                        isAlwaysReady,
                        candidates.Count);
                    return new(0, GetScaleInStateLocked());
                }

                long newestStartupTicks = candidates.Max(
                    static candidate => candidate.WorkerProxyStartupTime.Value.UtcTicks);
                CountScaleInCandidate[] newestCandidates = [.. candidates.Where(
                    candidate => candidate.WorkerProxyStartupTime.Value.UtcTicks == newestStartupTicks)];
                if (newestCandidates.Length != 1)
                {
                    Log.CountScaleInOrderingAmbiguous(
                        _logger,
                        functionGroup,
                        isAlwaysReady,
                        newestCandidates.Length);
                    return new(0, GetScaleInStateLocked());
                }

                retainedWorkerId = newestCandidates[0].WorkerId;
            }

            CountScaleInCandidate[] orderedCandidates = [.. candidates
                .Where(candidate => !string.Equals(candidate.WorkerId, retainedWorkerId, StringComparison.Ordinal))
                .OrderByDescending(static candidate => candidate.IsIdle)
                .ThenBy(static candidate => candidate.WorkerProxyStartupTime?.UtcTicks ?? long.MinValue)
                .ThenBy(static candidate => candidate.WorkerId, StringComparer.Ordinal)];
            HashSet<string> selectedWorkerIds = new(StringComparer.Ordinal);

            foreach (CountScaleInCandidate candidate in orderedCandidates.Where(static candidate => candidate.IsIdle))
            {
                if (selections.Count == excess)
                {
                    break;
                }

                TryCommitCountScaleInCandidateLocked(
                    candidate,
                    functionGroup,
                    isAlwaysReady,
                    requireIdle: true,
                    selections,
                    selectedWorkerIds);
            }

            foreach (CountScaleInCandidate candidate in orderedCandidates)
            {
                if (selections.Count == excess)
                {
                    break;
                }

                if (selectedWorkerIds.Contains(candidate.WorkerId))
                {
                    continue;
                }

                TryCommitCountScaleInCandidateLocked(
                    candidate,
                    functionGroup,
                    isAlwaysReady,
                    requireIdle: false,
                    selections,
                    selectedWorkerIds);
            }

            if (selections.Count > 0)
            {
                SignalReadyChannelsLocked();
                SignalScaleInStateChangedLocked();
            }

            resultingState = GetScaleInStateLocked();
        }

        foreach (CountScaleInSelection selection in selections)
        {
            _ = CompleteDrainAsync(
                selection.WorkerId,
                selection.Slot,
                selection.InvocationDrainTask,
                selection.Completion);
            _ = ObserveCountScaleInDrainAsync(selection.WorkerId, selection.Completion.Task);
            Log.CountScaleInWorkerSelected(_logger, selection.WorkerId, functionGroup, isAlwaysReady);
        }

        return new(selections.Count, resultingState);
    }

    private static void TryCommitCountScaleInCandidateLocked(
        CountScaleInCandidate candidate,
        string functionGroup,
        bool isAlwaysReady,
        bool requireIdle,
        List<CountScaleInSelection> selections,
        HashSet<string> selectedWorkerIds)
    {
        if (candidate.Slot.DrainTask is not null ||
            candidate.Slot.Channel is not { } channel ||
            channel.TryBeginCountScaleIn(functionGroup, isAlwaysReady, requireIdle) is not { } invocationDrainTask)
        {
            return;
        }

        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        candidate.Slot.DrainTask = completion.Task;
        selections.Add(new(candidate.WorkerId, candidate.Slot, invocationDrainTask, completion));
        selectedWorkerIds.Add(candidate.WorkerId);
    }

    bool IRuntimeRetirementWorkerManager.TryCloseAdmission(SplitScriptHostActivationFaulted condition)
    {
        ArgumentNullException.ThrowIfNull(condition);

        lock (_stateLock)
        {
            if (_disposed || condition.Incarnation != _runtimeIncarnation)
            {
                return false;
            }

            if (_runtimeRetirementRequested)
            {
                return _runtimeRetirementOwner is RuntimeRetirementOwner.ActivationFault;
            }

            _runtimeRetirementRequested = true;
            _runtimeRetirementOwner = RuntimeRetirementOwner.ActivationFault;
            foreach (WorkerSlot slot in _slots.Values)
            {
                slot.Channel?.CloseRuntimeAdmission();
            }

            SignalReadyChannelsLocked();
            SignalScaleInStateChangedLocked();
            return true;
        }
    }

    Task<RuntimeRetirementDrainResult> IRuntimeRetirementWorkerManager.DrainAllWorkersAsync(
        RuntimeIncarnation incarnation,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        List<Task<bool>> existingDrains = null;
        List<(string WorkerId, WorkerSlot Slot, Task InvocationDrainTask, TaskCompletionSource<bool> Completion)> selections = null;
        TaskCompletionSource<RuntimeRetirementDrainResult> retirementCompletion = null;
        Task<RuntimeRetirementDrainResult> drainTask;
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_runtimeRetirementRequested ||
                _runtimeRetirementOwner is not RuntimeRetirementOwner.ActivationFault ||
                incarnation != _runtimeIncarnation)
            {
                throw new InvalidOperationException("Runtime retirement drain does not match the active retiring incarnation.");
            }

            if (_runtimeRetirementDrainTask is null)
            {
                existingDrains = [];
                selections = [];
                foreach ((string workerId, WorkerSlot slot) in _slots)
                {
                    if (slot.DrainTask is not null)
                    {
                        existingDrains.Add(slot.DrainTask);
                        continue;
                    }

                    if (slot.Channel is not { } channel)
                    {
                        continue;
                    }

                    Task invocationDrainTask = channel.BeginRuntimeRetirementDrain();
                    TaskCompletionSource<bool> completion =
                        new(TaskCreationOptions.RunContinuationsAsynchronously);
                    slot.DrainTask = completion.Task;
                    selections.Add((workerId, slot, invocationDrainTask, completion));
                }

                SignalReadyChannelsLocked();
                retirementCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _runtimeRetirementDrainTask = retirementCompletion.Task;
            }

            drainTask = _runtimeRetirementDrainTask;
        }

        if (retirementCompletion is not null)
        {
            _ = CompleteRuntimeRetirementDrainAsync(existingDrains, selections, retirementCompletion);
        }

        return drainTask.WaitAsync(cancellationToken);
    }

    bool IAppVersionRuntimeDrainWorkerManager.TryStartAppVersionDrain(
        out AppVersionRuntimeDrainStart start)
    {
        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_runtimeRetirementRequested)
            {
                start = null;
                return false;
            }

            _runtimeRetirementRequested = true;
            _runtimeRetirementOwner = RuntimeRetirementOwner.AppVersion;
            List<string> nonHttpWorkerIds = [];
            List<string> httpWorkerIds = [];
            List<string> nonReadyWorkerIds = [];
            List<Task<bool>> existingDrains = [];
            foreach ((string workerId, WorkerSlot slot) in _slots)
            {
                if (slot.DrainTask is not null)
                {
                    existingDrains.Add(slot.DrainTask);
                }

                if (slot.Channel is not { } channel ||
                    !channel.CloseRuntimeAdmissionForAppVersionDrain(
                        out bool isInvocationReady,
                        out bool isHttp))
                {
                    continue;
                }

                if (isInvocationReady)
                {
                    (isHttp ? httpWorkerIds : nonHttpWorkerIds).Add(workerId);
                }
                else
                {
                    nonReadyWorkerIds.Add(workerId);
                }
            }

            nonHttpWorkerIds.Sort(StringComparer.Ordinal);
            httpWorkerIds.Sort(StringComparer.Ordinal);
            nonReadyWorkerIds.Sort(StringComparer.Ordinal);
            SignalReadyChannelsLocked();
            SignalScaleInStateChangedLocked();
            start = new(
                _runtimeIncarnation,
                nonHttpWorkerIds,
                httpWorkerIds,
                nonReadyWorkerIds,
                existingDrains);
            return true;
        }
    }

    AppVersionRuntimeDrainSelection IAppVersionRuntimeDrainWorkerManager.SelectWorkers(
        RuntimeIncarnation incarnation,
        IReadOnlyList<string> workerIds,
        DateTimeOffset invocationGraceDeadline)
    {
        ArgumentNullException.ThrowIfNull(workerIds);
        return SelectAppVersionDrainWorkers(
            incarnation,
            workerIds,
            invocationGraceDeadline);
    }

    AppVersionRuntimeDrainSelection IAppVersionRuntimeDrainWorkerManager.SelectAllRemainingWorkers(
        RuntimeIncarnation incarnation,
        DateTimeOffset invocationGraceDeadline)
    {
        string[] workerIds;
        lock (_stateLock)
        {
            ValidateAppVersionDrainLocked(incarnation);
            workerIds = [.. _slots.Keys.Order(StringComparer.Ordinal)];
        }

        return SelectAppVersionDrainWorkers(
            incarnation,
            workerIds,
            invocationGraceDeadline);
    }

    private AppVersionRuntimeDrainSelection SelectAppVersionDrainWorkers(
        RuntimeIncarnation incarnation,
        IReadOnlyList<string> workerIds,
        DateTimeOffset invocationGraceDeadline)
    {
        List<(string WorkerId, WorkerSlot Slot, Task InvocationDrainTask, TaskCompletionSource<bool> Completion)> selections = [];
        lock (_stateLock)
        {
            ValidateAppVersionDrainLocked(incarnation);
            foreach (string workerId in workerIds)
            {
                if (!_slots.TryGetValue(workerId, out WorkerSlot slot) ||
                    slot.DrainTask is not null ||
                    slot.Channel is not { } channel ||
                    channel.BeginAppVersionRuntimeDrain() is not { } invocationDrainTask)
                {
                    continue;
                }

                TaskCompletionSource<bool> completion =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);
                slot.DrainTask = completion.Task;
                selections.Add((workerId, slot, invocationDrainTask, completion));
            }

            if (selections.Count > 0)
            {
                SignalReadyChannelsLocked();
                SignalScaleInStateChangedLocked();
            }
        }

        foreach (var selection in selections)
        {
            _ = CompleteDrainAsync(
                selection.WorkerId,
                selection.Slot,
                selection.InvocationDrainTask,
                selection.Completion,
                invocationGraceDeadline);
            Log.AppVersionDrainWorkerSelected(
                _logger,
                selection.WorkerId,
                invocationGraceDeadline);
        }

        return new([.. selections.Select(static selection => selection.Completion.Task)]);
    }

    private void ValidateAppVersionDrainLocked(RuntimeIncarnation incarnation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_runtimeRetirementRequested ||
            _runtimeRetirementOwner is not RuntimeRetirementOwner.AppVersion ||
            incarnation != _runtimeIncarnation)
        {
            throw new InvalidOperationException(
                "The app-version Runtime drain does not match the active retiring incarnation.");
        }
    }

    private async Task LinkAndCompleteAsync(string workerId, Uri grpcEndpoint, WorkerSlot slot, TaskCompletionSource<WorkerChannel> linkCompletion,
        CancellationToken cancellationToken)
    {
        try
        {
            WorkerChannel channel = await LinkCoreAsync(workerId, grpcEndpoint, slot, cancellationToken);
            linkCompletion.SetResult(channel);
        }
        catch (OperationCanceledException exception)
        {
            linkCompletion.SetCanceled(exception.CancellationToken);
        }
        catch (Exception exception)
        {
            linkCompletion.SetException(exception);
        }
    }

    private async Task<WorkerChannel> LinkCoreAsync(
        string workerId,
        Uri grpcEndpoint,
        WorkerSlot slot,
        CancellationToken cancellationToken)
    {
        using SemaphoreLock gate = new(slot.Gate);
        bool initialized = false;
        try
        {
            using CancellationTokenSource deadline = new(_linkTimeout);
            using CancellationTokenSource operationSource =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownSource.Token, deadline.Token);

            DuplexChannel<StreamingMessage> ownedChannel = null;
            RpcClientWorkerChannel candidate = null;

            try
            {
                operationSource.Token.ThrowIfCancellationRequested();
                ownedChannel = await _duplexChannelFactory.ConnectAsync(grpcEndpoint, operationSource.Token);
                operationSource.Token.ThrowIfCancellationRequested();

                candidate = _channelFactory.Create(workerId, ownedChannel)
                    ?? throw new InvalidOperationException("The client worker channel factory returned no channel.");
                ownedChannel = null;

                if (!string.Equals(candidate.Id, workerId, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"The client worker channel factory returned worker '{candidate.Id}' for requested worker '{workerId}'.");
                }

                try
                {
                    await candidate.StartAsync(operationSource.Token);
                }
                catch (InvalidOperationException exception) when (candidate.Completion.IsCompleted)
                {
                    throw new IOException("The worker channel closed before initialization completed.", exception);
                }

                operationSource.Token.ThrowIfCancellationRequested();
                RegisterInitializedChannel(workerId, slot, candidate, operationSource.Token);

                WorkerChannel linkedChannel = candidate;
                candidate = null;
                initialized = true;

                return linkedChannel;
            }
            catch (Exception exception)
            {
                Exception failure = exception switch
                {
                    OperationCanceledException when !cancellationToken.IsCancellationRequested && _shutdownSource.IsCancellationRequested
                        => new ObjectDisposedException(GetType().FullName),
                    OperationCanceledException when !cancellationToken.IsCancellationRequested && deadline.IsCancellationRequested
                        => new TimeoutException("The worker link initialization deadline expired.", exception),
                    _ => exception,
                };
                failure = await candidate.DisposeAndCaptureExceptionAsync(failure);
                failure = await ownedChannel.DisposeAndCaptureExceptionAsync(failure);
                ExceptionDispatchInfo.Capture(failure).Throw();
                throw;
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && _shutdownSource.IsCancellationRequested)
        {
            throw new ObjectDisposedException(GetType().FullName);
        }
        finally
        {
            // Accepted links are cleaned up by monitor/unlink; completing Link must not wait for deferred setup.
            if (!initialized)
            {
                RemoveEmptySlot(workerId, slot);
                lock (_stateLock)
                {
                    _linkAttempts.Remove(workerId);
                }
            }
        }
    }

    public async Task<bool> UnlinkAsync(string workerId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);
        cancellationToken.ThrowIfCancellationRequested();

        WorkerSlot slot;
        lock (_stateLock)
        {
            if (_disposed || !_slots.TryGetValue(workerId, out slot))
            {
                return false;
            }
        }

        bool channelDetached = false;
        bool primaryHandoffConfirmed = false;
        try
        {
            using CancellationTokenSource operationSource =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownSource.Token);

            SemaphoreLock gate = await slot.Gate.LockAsync(operationSource.Token);
            try
            {
                using (gate)
                {
                    operationSource.Token.ThrowIfCancellationRequested();

                    RpcClientWorkerChannel channel;
                    lock (_stateLock)
                    {
                        if (_disposed)
                        {
                            return false;
                        }

                        channel = DetachChannelLocked(slot, expectedChannel: null);
                    }

                    if (channel is null)
                    {
                        return false;
                    }

                    channelDetached = true;
                    Exception failure = null;
                    try
                    {
                        channel.FailCommittedInvocations(exception: null);
                        channel.CompleteRequests();
                        // Wait for peer termination without treating its final gRPC status as an unlink failure.
                        await Task.WhenAny(channel.Completion).WaitAsync(_unlinkTimeout, operationSource.Token);
                        primaryHandoffConfirmed = true;
                        if (channel.Completion.IsFaulted)
                        {
                            Log.ChannelFailed(_logger, channel.Completion.Exception, workerId);
                        }
                    }
                    catch (Exception exception)
                    {
                        failure = exception;
                    }

                    failure = await channel.DisposeAndCaptureExceptionAsync(failure);
                    if (failure is not null)
                    {
                        if (!primaryHandoffConfirmed && !_shutdownSource.IsCancellationRequested)
                        {
                            AddOutstandingWorkerRemoval(slot);
                        }

                        ExceptionDispatchInfo.Capture(failure).Throw();
                    }

                    return true;
                }
            }
            finally
            {
                RemoveEmptySlot(workerId, slot);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && _shutdownSource.IsCancellationRequested)
        {
            return channelDetached;
        }
    }

    public bool TryGetInitializedChannel(string workerId, out WorkerChannel channel)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);

        lock (_stateLock)
        {
            if (!_disposed && _slots.TryGetValue(workerId, out WorkerSlot slot) && slot.Channel is not null)
            {
                channel = slot.Channel;
                return true;
            }
        }

        channel = null;
        return false;
    }

    public IReadOnlyList<WorkerChannel> GetInitializedChannels()
    {
        lock (_stateLock)
        {
            if (_disposed)
            {
                return [];
            }

            return [.. _slots.Values.Where(slot => slot.Channel is not null).Select(slot => (WorkerChannel)slot.Channel)];
        }
    }

    public Task ActivateAsync(IReadOnlyList<FunctionMetadata> functions, ManagedDependencyOptions managedDependencyOptions, TimeSpan? functionTimeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(functions);
        ArgumentNullException.ThrowIfNull(managedDependencyOptions);
        cancellationToken.ThrowIfCancellationRequested();

        ChannelSetupContext setupContext = new(functions, managedDependencyOptions, functionTimeout);
        Exception firstSetupException = null;
        int attemptedChannels = 0;
        int readyChannels = 0;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();

            // Invalidate the prior ready snapshot before replacing the context, including an empty function set.
            // An incomplete signal is retained so callers already waiting are not orphaned by a ScriptHost restart.
            ResetReadyChannelsSignal();
            _activeSetupContext = setupContext;

            foreach (WorkerSlot slot in _slots.Values)
            {
                if (slot.Channel is null)
                {
                    continue;
                }

                slot.Channel.SetScaleInReady(false);
                attemptedChannels++;
                ConfigureSlotLocked(slot);
                if (slot.SetupException is null)
                {
                    readyChannels++;
                }
                else
                {
                    firstSetupException ??= slot.SetupException;
                }
            }
        }

        if (attemptedChannels > 0 && readyChannels == 0)
        {
            // Preserve the original failure; a single healthy channel otherwise lets this ScriptHost start.
            ExceptionDispatchInfo.Capture(firstSetupException).Throw();
        }

        return Task.CompletedTask;
    }

    public ValueTask<IReadOnlyList<WorkerChannel>> GetReadyChannelsAsync()
    {
        // Retry only when another thread changes the signal before our CompareExchange succeeds; never poll for readiness.
        // Rereading preserves the newer publication. Waiting callers receive a task instead of spinning.
        while (true)
        {
            // Never wait for the setup lock. The published task is either a snapshot or the shared readiness wait.
            TaskCompletionSource<IReadOnlyList<WorkerChannel>> signal = GetReadyChannelsSignal();
            if (!signal.Task.IsCompletedSuccessfully)
            {
                return new(signal.Task);
            }

            IReadOnlyList<WorkerChannel> readyChannels = signal.Task.Result;
            if (readyChannels.Any(channel => channel is not RpcClientWorkerChannel { IsReadyForInvocations: true }))
            {
                // Transport completion can precede registry cleanup, so exclude channels that stopped since publication.
                readyChannels = [.. readyChannels.Where(channel => channel is RpcClientWorkerChannel { IsReadyForInvocations: true })];
            }

            if (readyChannels.Count > 0)
            {
                return new(readyChannels);
            }

            // Replace a stale completed signal only if a setup/cleanup writer has not published a newer snapshot.
            TaskCompletionSource<IReadOnlyList<WorkerChannel>> replacement = CreateReadyChannelsSignal();
            TaskCompletionSource<IReadOnlyList<WorkerChannel>> observedSignal =
                Interlocked.CompareExchange(ref _readyChannelsAvailable, replacement, signal);

            if (!ReferenceEquals(observedSignal, signal))
            {
                // Another thread changed the signal; retry against the newer publication.
                continue;
            }

            return new(replacement.Task);
        }
    }

    public async Task<WorkerChannel> WaitForFirstInitializedAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (true)
        {
            Task initializedChannelAvailable;
            lock (_stateLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                RpcClientWorkerChannel channel = _slots.Values.FirstOrDefault(slot => slot.Channel is not null)?.Channel;
                if (channel is not null)
                {
                    return channel;
                }

                initializedChannelAvailable = _initializedChannelAvailable.Task;
            }

            await initializedChannelAvailable.WaitAsync(cancellationToken);
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposeLock)
        {
            _disposeTask ??= DisposeAsyncCore();
            return new(_disposeTask);
        }
    }

    private async Task DisposeAsyncCore()
    {
        KeyValuePair<string, WorkerSlot>[] slots;
        Task[] monitorTasks;

        lock (_stateLock)
        {
            _disposed = true;
            slots = [.. _slots];
            monitorTasks = [.. _monitorTasks];
            _initializedChannelAvailable.TrySetResult();
            _scaleInStateChanged.TrySetException(new ObjectDisposedException(GetType().FullName));

            // Publish disposal for new readers and release callers awaiting the previous shared signal.
            ObjectDisposedException exception = new(GetType().FullName);
            TaskCompletionSource<IReadOnlyList<WorkerChannel>> disposedSignal = CreateReadyChannelsSignal();
            disposedSignal.SetException(exception);
            Interlocked.Exchange(ref _readyChannelsAvailable, disposedSignal).TrySetException(exception);
        }

        Exception disposalException = null;
        try
        {
            await _shutdownSource.CancelAsync();
        }
        catch (Exception exception)
        {
            disposalException = exception;
        }

        Task<Exception>[] slotDisposals = [.. slots.Select(slot => ShutdownSlotAsync(slot.Key, slot.Value))];
        Exception[] slotExceptions = await Task.WhenAll(slotDisposals);
        foreach (Exception exception in slotExceptions)
        {
            if (exception is not null)
            {
                disposalException = AggregateException.Combine(disposalException, exception);
            }
        }

        await Task.WhenAll(monitorTasks);

        if (disposalException is not null)
        {
            ExceptionDispatchInfo.Capture(disposalException).Throw();
        }
    }

    private RpcClientWorkerChannel DetachChannelLocked(WorkerSlot slot, RpcClientWorkerChannel expectedChannel)
    {
        RpcClientWorkerChannel channel = slot.Channel;
        if (channel is null || (expectedChannel is not null && !ReferenceEquals(channel, expectedChannel)))
        {
            return null;
        }

        slot.Channel = null;
        slot.AttemptedSetupContext = null;
        slot.SetupException = null;
        if (slot.LinkTask is not null)
        {
            slot.Terminal = true;
            slot.LinkTask = null;
        }

        if (!_disposed)
        {
            // Remove the detached channel from the published snapshot before channel disposal begins.
            SignalReadyChannelsLocked();
            SignalScaleInStateChangedLocked();
            if (!_slots.Values.Any(slot => slot.Channel is not null))
            {
                _initializedChannelAvailable = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        return channel;
    }

    private void RemoveEmptySlot(string workerId, WorkerSlot slot)
    {
        lock (_stateLock)
        {
            if (slot.Channel is null &&
                _slots.TryGetValue(workerId, out WorkerSlot currentSlot) && ReferenceEquals(currentSlot, slot))
            {
                _slots.Remove(workerId);
            }
        }
    }

    private async Task MonitorChannelCompletionAsync(string workerId, WorkerSlot slot, RpcClientWorkerChannel channel)
    {
        Exception terminalException = null;
        try
        {
            // Client workers are external; transport completion and fatal protocol failures end their channel lifetime.
            await Task.WhenAny(channel.Completion, channel.FunctionLoadFailure).WaitAsync(_shutdownSource.Token);
            if (channel.FunctionLoadFailure.IsCompletedSuccessfully)
            {
                terminalException = channel.FunctionLoadFailure.Result;
            }
            else
            {
                await channel.Completion;
            }
        }
        catch (OperationCanceledException) when (_shutdownSource.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            terminalException = exception;
        }

        SemaphoreLock gate = await slot.Gate.LockAsync();
        try
        {
            using (gate)
            {
                // LinkAsync holds this gate until the monitor is tracked and publication completes. It also serializes this
                // cleanup with unlink and shutdown.
                RpcClientWorkerChannel removedChannel;
                lock (_stateLock)
                {
                    // Only detach the channel associated with this completion signal.
                    removedChannel = DetachChannelLocked(slot, channel);
                }

                if (removedChannel is null)
                {
                    return;
                }

                if (terminalException is null)
                {
                    Log.ChannelCompleted(_logger, workerId);
                }
                else
                {
                    Log.ChannelFailed(_logger, terminalException, workerId);
                }

                try
                {
                    removedChannel.FailCommittedInvocations(terminalException);
                    removedChannel.Shutdown(terminalException);
                }
                catch (Exception exception)
                {
                    Log.ChannelShutdownFailed(_logger, exception, workerId);
                }

                try
                {
                    await removedChannel.DisposeAsync();
                }
                catch (Exception exception)
                {
                    Log.ChannelDisposalFailed(_logger, exception, workerId);
                }
            }
        }
        catch (Exception exception)
        {
            Log.ChannelRemovalFailed(_logger, exception, workerId);
        }
        finally
        {
            RemoveEmptySlot(workerId, slot);
        }
    }

    private void OnMonitorCompleted(Task monitorTask)
    {
        lock (_stateLock)
        {
            _monitorTasks.Remove(monitorTask);
        }
    }

    private async Task CompleteDrainAsync(
        string workerId,
        WorkerSlot slot,
        Task invocationDrainTask,
        TaskCompletionSource<bool> drainCompletion,
        DateTimeOffset? invocationGraceDeadline = null)
    {
        try
        {
            if (invocationGraceDeadline is null)
            {
                await invocationDrainTask.WaitAsync(_shutdownSource.Token);
            }
            else
            {
                TimeSpan remaining = invocationGraceDeadline.Value - _timeProvider.GetUtcNow();
                if (remaining > TimeSpan.Zero)
                {
                    Task deadline = Task.Delay(remaining, _timeProvider, _shutdownSource.Token);
                    Task completed = await Task.WhenAny(invocationDrainTask, deadline);
                    if (ReferenceEquals(completed, invocationDrainTask))
                    {
                        await invocationDrainTask.WaitAsync(_shutdownSource.Token);
                    }
                    else
                    {
                        await deadline;
                        Log.AppVersionDrainGraceExpired(
                            _logger,
                            workerId,
                            invocationGraceDeadline.Value);
                    }
                }
                else
                {
                    Log.AppVersionDrainGraceExpired(
                        _logger,
                        workerId,
                        invocationGraceDeadline.Value);
                }
            }

            bool unlinked = await UnlinkAsync(workerId, _shutdownSource.Token);
            if (!unlinked)
            {
                lock (_stateLock)
                {
                    unlinked = slot.Terminal;
                }
            }

            drainCompletion.TrySetResult(unlinked);
        }
        catch (OperationCanceledException) when (_shutdownSource.IsCancellationRequested)
        {
            drainCompletion.TrySetResult(false);
        }
        catch (Exception exception)
        {
            if (IsRuntimeRetirementFallbackOutstanding(slot))
            {
                drainCompletion.TrySetResult(true);
            }
            else
            {
                drainCompletion.TrySetException(exception);
            }
        }
    }

    private async Task ObserveCountScaleInDrainAsync(string workerId, Task<bool> drainCompletion)
    {
        try
        {
            if (!await drainCompletion)
            {
                Log.CountScaleInDrainIncomplete(_logger, workerId);
            }
        }
        catch (Exception exception)
        {
            Log.CountScaleInDrainFailed(_logger, exception, workerId);
        }
    }

    private async Task CompleteRuntimeRetirementDrainAsync(
        IReadOnlyList<Task<bool>> existingDrains,
        IReadOnlyList<(string WorkerId, WorkerSlot Slot, Task InvocationDrainTask, TaskCompletionSource<bool> Completion)> selections,
        TaskCompletionSource<RuntimeRetirementDrainResult> retirementCompletion)
    {
        try
        {
            foreach (var selection in selections)
            {
                _ = CompleteDrainAsync(
                    selection.WorkerId,
                    selection.Slot,
                    selection.InvocationDrainTask,
                    selection.Completion);
            }

            IEnumerable<Task<bool>> workerDrains = existingDrains.Concat(
                selections.Select(static selection => selection.Completion.Task));
            bool[] results = await Task.WhenAll(workerDrains);
            RuntimeRetirementDrainResult result = results.All(static workerDrained => workerDrained)
                ? new(true, null)
                : new(false, "At least one Worker could not complete its drain and unlink.");
            retirementCompletion.TrySetResult(result);
        }
        catch (Exception exception)
        {
            retirementCompletion.TrySetException(exception);
        }
    }

    private void RegisterInitializedChannel(string workerId, WorkerSlot slot, RpcClientWorkerChannel channel, CancellationToken cancellationToken)
    {
        Task monitorTask;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_runtimeRetirementRequested)
            {
                throw new WorkerLinkException(
                    WorkerLinkFailureReason.RuntimeStopping,
                    "The Runtime started retiring before the Worker Link was registered.");
            }

            // Setup can hold this lock past the link deadline or transport lifetime.
            cancellationToken.ThrowIfCancellationRequested();
            if (channel.IsTerminated)
            {
                Exception failure = channel.FunctionLoadFailure.IsCompletedSuccessfully
                    ? channel.FunctionLoadFailure.Result
                    : channel.Completion.Exception?.GetBaseException();
                throw new IOException("The worker channel terminated before initialization was registered.", failure);
            }

            slot.Channel = channel;
            SignalScaleInStateChangedLocked();
            monitorTask = MonitorChannelCompletionAsync(workerId, slot, channel);
            _monitorTasks.Add(monitorTask);

            // Signal initialized-channel availability without starting ScriptHost here.
            _initializedChannelAvailable.TrySetResult();
        }

        _ = ConfigureSlotInBackgroundAsync(slot);
        _ = monitorTask.ContinueWith(static (task, state) => ((WorkerChannelRegistry)state).OnMonitorCompleted(task), this,
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task ConfigureSlotInBackgroundAsync(WorkerSlot slot)
    {
        // Keep function setup out of the WorkerInit/link call path.
        await Task.Yield();

        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            ConfigureSlotLocked(slot);
        }
    }

    private void ConfigureSlotLocked(WorkerSlot slot)
    {
        // Read the context when deferred setup runs, not when the channel linked, to handle ScriptHost restarts.
        ChannelSetupContext setupContext = _activeSetupContext;
        RpcClientWorkerChannel channel = slot.Channel;
        if (setupContext is null ||
            channel is null ||
            ReferenceEquals(slot.AttemptedSetupContext, setupContext))
        {
            return;
        }

        // Each slot gets one attempt per context. Failed setup stays excluded until a new activation retries it.
        slot.AttemptedSetupContext = setupContext;
        slot.SetupException = null;
        try
        {
            channel.SetupFunctionInvocationBuffers(setupContext.Functions);
            channel.SendFunctionLoadRequests(setupContext.ManagedDependencyOptions, setupContext.FunctionTimeout);
            channel.SetScaleInReady(true);
            SignalReadyChannelsLocked();
            SignalScaleInStateChangedLocked();
        }
        catch (Exception exception)
        {
            channel.SetScaleInReady(false);
            slot.SetupException = exception;
            Log.ChannelSetupFailed(_logger, exception, channel.Id);
            SignalScaleInStateChangedLocked();
        }
    }

    private IReadOnlyList<WorkerChannel> GetReadyChannelsLocked()
    {
        ChannelSetupContext setupContext = _activeSetupContext;
        if (setupContext is null)
        {
            return [];
        }

        // Invocation buffers alone are insufficient: sending load requests must also succeed for this context.
        return [.. _slots.Values
            .Where(slot =>
                ReferenceEquals(slot.AttemptedSetupContext, setupContext) &&
                slot.SetupException is null &&
                slot.Channel is { IsReadyForInvocations: true })
            .Select(slot => (WorkerChannel)slot.Channel)];
    }

    private void SignalReadyChannelsLocked()
    {
        IReadOnlyList<WorkerChannel> readyChannels = GetReadyChannelsLocked();
        if (readyChannels.Count == 0)
        {
            ResetReadyChannelsSignal();
            return;
        }

        // Publish the current snapshot for new readers, then complete the shared wait with the same snapshot.
        TaskCompletionSource<IReadOnlyList<WorkerChannel>> readySignal = CreateReadyChannelsSignal();
        readySignal.SetResult(readyChannels);
        Interlocked.Exchange(ref _readyChannelsAvailable, readySignal).TrySetResult(readyChannels);
    }

    private void ResetReadyChannelsSignal()
    {
        while (true)
        {
            TaskCompletionSource<IReadOnlyList<WorkerChannel>> signal = GetReadyChannelsSignal();
            if (!signal.Task.IsCompleted)
            {
                return;
            }

            // Readers can concurrently replace stale signals; preserve whichever incomplete wait wins.
            TaskCompletionSource<IReadOnlyList<WorkerChannel>> replacement = CreateReadyChannelsSignal();
            if (ReferenceEquals(Interlocked.CompareExchange(ref _readyChannelsAvailable, replacement, signal), signal))
            {
                return;
            }
        }
    }

    private TaskCompletionSource<IReadOnlyList<WorkerChannel>> GetReadyChannelsSignal()
        => Interlocked.CompareExchange(ref _readyChannelsAvailable, null, null);

    private static TaskCompletionSource<IReadOnlyList<WorkerChannel>> CreateReadyChannelsSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private HttpWorkerScaleInState GetScaleInStateLocked()
    {
        return new(
            _scaleInStateCreatedTime,
            _scaleInHttpCapacity,
            _scaleInLinkedWorkerCount,
            _scaleInStateVersion)
        {
            OutstandingWorkerRemovals = [.. _outstandingWorkerRemovals],
        };
    }

    private void AddOutstandingWorkerRemoval(WorkerSlot slot)
    {
        if (slot.Worker is null)
        {
            Log.WorkerRemovalFallbackUnavailable(_logger, slot.Endpoint);
            return;
        }

        FunctionsSplitWorkerRemovalAction action = new(slot.Worker, UnlinkRemovalReason);
        lock (_stateLock)
        {
            if (_completedWorkerRemovals.Contains(action) || _outstandingWorkerRemovalSet.Contains(action))
            {
                slot.RemovalFallbackRecorded = true;
                return;
            }

            _outstandingWorkerRemovalSet.Add(action);
            _outstandingWorkerRemovals.Add(action);
            slot.RemovalFallbackRecorded = true;
            SignalScaleInStateChangedLocked();
        }

        Log.WorkerRemovalFallbackAdded(_logger, slot.Worker.PodName, slot.Worker.PodGuid);
    }

    private bool IsRuntimeRetirementFallbackOutstanding(WorkerSlot slot)
    {
        lock (_stateLock)
        {
            return _runtimeRetirementRequested &&
                slot.RemovalFallbackRecorded;
        }
    }

    private void AddCompletedWorkerRemovalLocked(FunctionsSplitWorkerRemovalAction action)
    {
        if (!_completedWorkerRemovals.Add(action))
        {
            return;
        }

        _completedWorkerRemovalOrder.Enqueue(action);
        while (_completedWorkerRemovalOrder.Count > CompletedWorkerRemovalCapacity)
        {
            _completedWorkerRemovals.Remove(_completedWorkerRemovalOrder.Dequeue());
        }
    }

    private (long HttpCapacity, int LinkedWorkerCount) ComputeScaleInStateLocked()
    {
        long httpCapacity = 0;
        int linkedWorkerCount = 0;
        foreach (WorkerSlot slot in _slots.Values)
        {
            if (slot.Channel is not { } channel)
            {
                continue;
            }

            if (channel.IsTerminated)
            {
                continue;
            }

            linkedWorkerCount++;
            if (channel.ContributesHttpCapacity)
            {
                httpCapacity += DefaultHttpWorkerCapacity;
            }
        }

        return (httpCapacity, linkedWorkerCount);
    }

    private void RefreshScaleInStateLocked()
    {
        (long httpCapacity, int linkedWorkerCount) = ComputeScaleInStateLocked();
        if (_scaleInHttpCapacity == httpCapacity &&
            _scaleInLinkedWorkerCount == linkedWorkerCount)
        {
            return;
        }

        _scaleInHttpCapacity = httpCapacity;
        _scaleInLinkedWorkerCount = linkedWorkerCount;
        AdvanceScaleInStateVersionLocked();
    }

    private void SignalScaleInStateChangedLocked()
    {
        (_scaleInHttpCapacity, _scaleInLinkedWorkerCount) = ComputeScaleInStateLocked();
        AdvanceScaleInStateVersionLocked();
    }

    private void AdvanceScaleInStateVersionLocked()
    {
        DateTimeOffset createdTime = _timeProvider.GetUtcNow();
        _scaleInStateCreatedTime = createdTime > _scaleInStateCreatedTime
            ? createdTime
            : _scaleInStateCreatedTime.AddTicks(1);
        long version = ++_scaleInStateVersion;
        TaskCompletionSource<long> changed = _scaleInStateChanged;
        _scaleInStateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
        changed.TrySetResult(version);
    }

    private async Task<Exception> ShutdownSlotAsync(string workerId, WorkerSlot slot)
    {
        Exception disposalException = null;

        SemaphoreLock gate = await slot.Gate.LockAsync();
        try
        {
            using (gate)
            {
                RpcClientWorkerChannel channel;
                lock (_stateLock)
                {
                    channel = DetachChannelLocked(slot, expectedChannel: null);
                }

                if (channel is not null)
                {
                    channel.FailCommittedInvocations(new ObjectDisposedException(channel.GetType().FullName));
                }

                disposalException = await channel.DisposeAndCaptureExceptionAsync(disposalException);
            }
        }
        catch (Exception exception)
        {
            disposalException = AggregateException.Combine(disposalException, exception);
        }
        finally
        {
            RemoveEmptySlot(workerId, slot);
        }

        return disposalException;
    }

    private sealed class WorkerSlot
    {
        // The exact active setup context attempted for this channel.
        public ChannelSetupContext AttemptedSetupContext { get; set; }

        public RpcClientWorkerChannel Channel { get; set; }

        // The initial link owns this gate before its slot is published.
        public SemaphoreSlim Gate { get; } = new(0, 1);

        // The committed scale-in drain for this worker.
        public Task<bool> DrainTask { get; set; }

        // The recorded gRPC endpoint distinguishes exact retries from conflicting requests.
        public Uri Endpoint { get; init; }

        // The link task shared by exact retries; cleared when the accepted channel is detached.
        public Task<WorkerChannel> LinkTask { get; set; }

        // The original failure from the attempted setup context, or null after successful setup.
        public Exception SetupException { get; set; }

        // Indicates that an uncertain unlink produced an exact fallback handoff.
        public bool RemovalFallbackRecorded { get; set; }

        // Indicates that a previously accepted link has terminated and cannot be replayed.
        public bool Terminal { get; set; }

        // The authoritative physical identity is set at most once and retained after channel withdrawal.
        public FunctionsSplitPodIdentity Worker { get; set; }

        // The authoritative original WorkerProxy startup time is retained beside exact identity.
        public DateTimeOffset? WorkerProxyStartupTime { get; set; }
    }

    private sealed record CountScaleInCandidate(
        string WorkerId,
        WorkerSlot Slot,
        bool IsIdle,
        DateTimeOffset? WorkerProxyStartupTime);

    private sealed record CountScaleInSelection(
        string WorkerId,
        WorkerSlot Slot,
        Task InvocationDrainTask,
        TaskCompletionSource<bool> Completion);

    private sealed record ChannelSetupContext(
        IReadOnlyList<FunctionMetadata> Functions,
        ManagedDependencyOptions ManagedDependencyOptions,
        TimeSpan? FunctionTimeout);

    private static partial class Log
    {
        [LoggerMessage(0, LogLevel.Debug, "FunctionRpc channel for worker {WorkerId} completed and was removed.")]
        public static partial void ChannelCompleted(ILogger logger, string workerId);

        [LoggerMessage(1, LogLevel.Warning, "FunctionRpc channel for worker {WorkerId} failed and was removed.")]
        public static partial void ChannelFailed(ILogger logger, Exception exception, string workerId);

        [LoggerMessage(2, LogLevel.Error, "Failed to dispose the completed FunctionRpc channel for worker {WorkerId}.")]
        public static partial void ChannelDisposalFailed(ILogger logger, Exception exception, string workerId);

        [LoggerMessage(3, LogLevel.Error, "Failed to remove the completed FunctionRpc channel for worker {WorkerId}.")]
        public static partial void ChannelRemovalFailed(ILogger logger, Exception exception, string workerId);

        [LoggerMessage(4, LogLevel.Error, "Failed to shut down the completed FunctionRpc channel for worker {WorkerId}.")]
        public static partial void ChannelShutdownFailed(ILogger logger, Exception exception, string workerId);

        [LoggerMessage(5, LogLevel.Warning, "Failed to configure client-backed worker {WorkerId} for the active ScriptHost.")]
        public static partial void ChannelSetupFailed(ILogger logger, Exception exception, string workerId);

        [LoggerMessage(6, LogLevel.Warning,
            "Published fallback removal for worker {WorkerId} incarnation {WorkerPodGuid} after an uncertain graceful unlink.")]
        public static partial void WorkerRemovalFallbackAdded(ILogger logger, string workerId, Guid workerPodGuid);

        [LoggerMessage(7, LogLevel.Warning,
            "Cannot publish fallback removal after an uncertain graceful unlink for endpoint {WorkerEndpoint} because the link omitted exact worker identity.")]
        public static partial void WorkerRemovalFallbackUnavailable(ILogger logger, Uri workerEndpoint);

        [LoggerMessage(8, LogLevel.Information,
            "Selected worker {WorkerId} for authoritative count scale-in of function group {FunctionGroup} and always-ready class {IsAlwaysReady}.")]
        public static partial void CountScaleInWorkerSelected(
            ILogger logger,
            string workerId,
            string functionGroup,
            bool isAlwaysReady);

        [LoggerMessage(9, LogLevel.Warning,
            "The committed authoritative count scale-in drain for worker {WorkerId} did not complete locally.")]
        public static partial void CountScaleInDrainIncomplete(ILogger logger, string workerId);

        [LoggerMessage(10, LogLevel.Error,
            "The committed authoritative count scale-in drain for worker {WorkerId} failed.")]
        public static partial void CountScaleInDrainFailed(ILogger logger, Exception exception, string workerId);

        [LoggerMessage(11, LogLevel.Debug,
            "Skipped authoritative count scale-in for function group {FunctionGroup} and always-ready class {IsAlwaysReady} because Runtime retirement is active.")]
        public static partial void CountScaleInSkippedForRuntimeRetirement(
            ILogger logger,
            string functionGroup,
            bool isAlwaysReady);

        [LoggerMessage(12, LogLevel.Warning,
            "Skipped positive Durable count scale-in for function group {FunctionGroup} and always-ready class {IsAlwaysReady} because authoritative startup time is unavailable for one or more of {CandidateCount} eligible Workers.")]
        public static partial void CountScaleInOrderingUnavailable(
            ILogger logger,
            string functionGroup,
            bool isAlwaysReady,
            int candidateCount);

        [LoggerMessage(13, LogLevel.Warning,
            "Skipped positive Durable count scale-in for function group {FunctionGroup} and always-ready class {IsAlwaysReady} because {NewestCandidateCount} eligible Workers share the newest authoritative startup time.")]
        public static partial void CountScaleInOrderingAmbiguous(
            ILogger logger,
            string functionGroup,
            bool isAlwaysReady,
            int newestCandidateCount);

        [LoggerMessage(14, LogLevel.Information,
            "Selected worker {WorkerId} for app-version Runtime drain with committed-invocation grace deadline {InvocationGraceDeadline}.")]
        public static partial void AppVersionDrainWorkerSelected(
            ILogger logger,
            string workerId,
            DateTimeOffset invocationGraceDeadline);

        [LoggerMessage(15, LogLevel.Warning,
            "Committed-invocation grace expired for app-version draining worker {WorkerId} at {InvocationGraceDeadline}; closing its channel.")]
        public static partial void AppVersionDrainGraceExpired(
            ILogger logger,
            string workerId,
            DateTimeOffset invocationGraceDeadline);
    }
}
