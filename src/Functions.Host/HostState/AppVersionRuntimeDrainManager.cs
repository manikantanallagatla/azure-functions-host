// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.RuntimeDrain;
using Azure.Functions.Rpc.Client;
using Microsoft.Extensions.Logging;

namespace Azure.Functions.Host.HostState;

internal sealed partial class AppVersionRuntimeDrainManager(
    IAppVersionRuntimeDrainWorkerManager workerManager,
    IHttpWorkerScaleInManager scaleInManager,
    AppServerHostStateClient appServerClient,
    TimeProvider timeProvider,
    ILogger<AppVersionRuntimeDrainManager> logger) : IAppVersionRuntimeDrainManager
{
    internal static readonly TimeSpan InvocationGracePeriod = TimeSpan.FromMinutes(60);

    private static readonly TimeSpan[] NonHttpBatchOffsets =
    [
        TimeSpan.Zero,
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(20),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromSeconds(40),
    ];

    private static readonly TimeSpan HttpSelectionOffset = TimeSpan.FromSeconds(35);

    private readonly AppServerHostStateClient _appServerClient =
        appServerClient ?? throw new ArgumentNullException(nameof(appServerClient));

    private readonly ILogger<AppVersionRuntimeDrainManager> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly Lock _operationLock = new();
    private readonly IHttpWorkerScaleInManager _scaleInManager =
        scaleInManager ?? throw new ArgumentNullException(nameof(scaleInManager));

    private readonly TimeProvider _timeProvider =
        timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

    private readonly List<Task<bool>> _trackedDrains = [];
    private readonly IAppVersionRuntimeDrainWorkerManager _workerManager =
        workerManager ?? throw new ArgumentNullException(nameof(workerManager));

    private RuntimeDrainMode _activeMode;
    private int _drainGeneration;
    private OperationRecord? _escalationOperation;
    private bool _finalizationStarted;
    private OperationRecord? _initialOperation;
    private AppVersionRuntimeDrainStart? _initialStart;
    private TaskCompletionSource _operationChanged =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private CancellationTokenSource? _phasedScheduleSource;
    private Exception? _lifecycleFailure;
    private Exception? _selectionFailure;
    private bool _selectionFinalized;

    public Task<RuntimeDrainReceipt> AcceptAsync(
        Guid operationId,
        RuntimeDrainMode mode,
        RuntimeDrainReason reason)
    {
        lock (_operationLock)
        {
            if (TryGetReplayLocked(operationId, mode, reason, out Task<RuntimeDrainReceipt>? replay))
            {
                return replay!;
            }

            if (_initialOperation is null)
            {
                if (!_workerManager.TryStartAppVersionDrain(out AppVersionRuntimeDrainStart? start))
                {
                    throw new RuntimeDrainConflictException(
                        "The Runtime is already executing a different retirement operation.");
                }

                _initialStart = start;
                _activeMode = mode;
                OperationRecord operation = new(operationId, mode, reason);
                _initialOperation = operation;
                operation.AdmissionTask = AdmitInitialAsync(operation, start);
                return operation.AdmissionTask;
            }

            if (_activeMode is RuntimeDrainMode.Phased &&
                mode is RuntimeDrainMode.Immediate &&
                _escalationOperation is null &&
                !_finalizationStarted)
            {
                ThrowIfLifecycleFailedLocked();
                OperationRecord operation = new(operationId, mode, reason);
                _escalationOperation = operation;
                operation.AdmissionTask = EscalateAsync(operation, _initialOperation.AdmissionTask);
                return operation.AdmissionTask;
            }

            throw new RuntimeDrainConflictException(
                "The Runtime is already executing a different app-version drain operation.");
        }
    }

    private async Task<RuntimeDrainReceipt> AdmitInitialAsync(
        OperationRecord operation,
        AppVersionRuntimeDrainStart start)
    {
        HttpWorkerScaleInState fencedState;
        try
        {
            AppServerHostStatePublishResult bindResult =
                await _appServerClient.BindRuntimeIncarnationAsync(CancellationToken.None);
            EnsureApplied(bindResult, "bind the Runtime incarnation");

            fencedState = _scaleInManager.Current;
            AppServerHostStatePublishResult fenceResult = await _appServerClient.PublishLifecycleAsync(
                RuntimeLifecycleStatus.Draining,
                fencedState,
                CancellationToken.None);
            EnsureApplied(fenceResult, "apply the app-version Runtime drain fence");
        }
        catch (HttpRequestException exception)
        {
            throw new RuntimeDrainAdmissionException(
                "AppServer was unavailable while committing the Runtime drain fence.",
                exception);
        }
        catch (OperationCanceledException exception)
        {
            throw new RuntimeDrainAdmissionException(
                "AppServer timed out while committing the Runtime drain fence.",
                exception);
        }

        DateTimeOffset acceptedAtUtc = _timeProvider.GetUtcNow();
        RuntimeDrainReceipt receipt = new(
            operation.OperationId,
            operation.Mode.ToString(),
            acceptedAtUtc);

        CancellationTokenSource? phasedScheduleSource = null;
        int phasedBatchSize = 0;
        lock (_operationLock)
        {
            _trackedDrains.AddRange(start.ExistingDrains);
            if (start.ExistingDrains.Count > 0)
            {
                _drainGeneration++;
            }

            TrackSelectionLocked(_workerManager.SelectWorkers(
                start.Incarnation,
                start.NonReadyWorkerIds,
                acceptedAtUtc + InvocationGracePeriod));

            if (operation.Mode is RuntimeDrainMode.Phased)
            {
                phasedBatchSize = Math.Max(1, (start.NonHttpWorkerIds.Count + 4) / 5);
                SelectPhasedBatchLocked(start, phasedBatchSize, batchIndex: 0);
                _phasedScheduleSource = new();
                phasedScheduleSource = _phasedScheduleSource;
            }
            else
            {
                SelectAllRemainingLocked(acceptedAtUtc);
                _selectionFinalized = true;
                SignalOperationChangedLocked();
            }
        }

        Task lifecycle = RunLifecycleWithFailureLatchAsync(fencedState);
        _ = ObserveLifecycleAsync(lifecycle);
        if (phasedScheduleSource is not null)
        {
            Task phasedSchedule = RunPhasedScheduleAsync(
                start,
                phasedBatchSize,
                acceptedAtUtc,
                phasedScheduleSource.Token);
            _ = ObservePhasedScheduleAsync(phasedSchedule);
        }

        Log.Accepted(
            _logger,
            operation.OperationId,
            operation.Mode,
            operation.Reason,
            start.Incarnation.Value,
            acceptedAtUtc);

        return receipt;
    }

    private async Task<RuntimeDrainReceipt> EscalateAsync(
        OperationRecord operation,
        Task<RuntimeDrainReceipt> initialAdmission)
    {
        await initialAdmission;
        DateTimeOffset acceptedAtUtc = _timeProvider.GetUtcNow();

        lock (_operationLock)
        {
            ThrowIfLifecycleFailedLocked();
            if (_finalizationStarted)
            {
                throw new RuntimeDrainConflictException(
                    "The phased Runtime drain has already entered final removal publication.");
            }

            _activeMode = RuntimeDrainMode.Immediate;
            _phasedScheduleSource?.Cancel();
            SelectAllRemainingLocked(acceptedAtUtc);
            _selectionFinalized = true;
            SignalOperationChangedLocked();
        }

        Log.Escalated(
            _logger,
            operation.OperationId,
            operation.Reason,
            _workerManager.Incarnation.Value,
            acceptedAtUtc);

        return new(
            operation.OperationId,
            operation.Mode.ToString(),
            acceptedAtUtc);
    }

    private async Task RunLifecycleWithFailureLatchAsync(HttpWorkerScaleInState fencedState)
    {
        try
        {
            await RunLifecycleAsync(fencedState);
        }
        catch (Exception exception)
        {
            lock (_operationLock)
            {
                _lifecycleFailure = exception;
            }

            throw;
        }
    }

    private async Task RunPhasedScheduleAsync(
        AppVersionRuntimeDrainStart start,
        int batchSize,
        DateTimeOffset acceptedAtUtc,
        CancellationToken cancellationToken)
    {
        try
        {
            for (int batchIndex = 1; batchIndex < NonHttpBatchOffsets.Length - 1; batchIndex++)
            {
                await DelayUntilAsync(acceptedAtUtc + NonHttpBatchOffsets[batchIndex], cancellationToken);
                lock (_operationLock)
                {
                    if (_activeMode is not RuntimeDrainMode.Phased)
                    {
                        return;
                    }

                    SelectPhasedBatchLocked(start, batchSize, batchIndex);
                }
            }

            await DelayUntilAsync(acceptedAtUtc + HttpSelectionOffset, cancellationToken);
            lock (_operationLock)
            {
                if (_activeMode is not RuntimeDrainMode.Phased)
                {
                    return;
                }

                TrackSelectionLocked(_workerManager.SelectWorkers(
                    start.Incarnation,
                    start.HttpWorkerIds,
                    _timeProvider.GetUtcNow() + InvocationGracePeriod));
            }

            await DelayUntilAsync(acceptedAtUtc + NonHttpBatchOffsets[^1], cancellationToken);
            lock (_operationLock)
            {
                if (_activeMode is not RuntimeDrainMode.Phased)
                {
                    return;
                }

                SelectPhasedBatchLocked(start, batchSize, NonHttpBatchOffsets.Length - 1);
                _selectionFinalized = true;
                SignalOperationChangedLocked();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            lock (_operationLock)
            {
                _selectionFailure = exception;
                _selectionFinalized = true;
                SignalOperationChangedLocked();
            }

            throw;
        }
    }

    private async Task RunLifecycleAsync(HttpWorkerScaleInState fencedState)
    {
        long lastPublishedVersion = fencedState.Version;
        while (true)
        {
            Task<bool[]>? drains = null;
            int generation = 0;
            Task operationChanged;
            Exception? selectionFailure;
            lock (_operationLock)
            {
                selectionFailure = _selectionFailure;
                if (_selectionFinalized)
                {
                    drains = Task.WhenAll([.. _trackedDrains]);
                    generation = _drainGeneration;
                }

                operationChanged = _operationChanged.Task;
            }

            if (selectionFailure is not null)
            {
                throw new InvalidOperationException(
                    "The app-version Runtime drain schedule failed.",
                    selectionFailure);
            }

            if (drains is null)
            {
                await operationChanged;
                continue;
            }

            bool restart = false;
            while (!drains.IsCompleted)
            {
                HttpWorkerScaleInState state = _scaleInManager.Current;
                if (state.Version != lastPublishedVersion)
                {
                    await PublishDrainingStateAsync(state);
                    lastPublishedVersion = state.Version;
                }

                Task<long> stateChanged = _scaleInManager.WaitForChangeAsync(state.Version);
                lock (_operationLock)
                {
                    operationChanged = _operationChanged.Task;
                }

                Task completed = await Task.WhenAny(drains, stateChanged, operationChanged);
                if (ReferenceEquals(completed, operationChanged))
                {
                    restart = true;
                    break;
                }

                if (ReferenceEquals(completed, stateChanged))
                {
                    await stateChanged;
                }
            }

            if (restart)
            {
                continue;
            }

            bool[] results = await drains;
            if (results.Any(static drained => !drained))
            {
                throw new InvalidOperationException(
                    "At least one Worker could not complete its app-version drain and unlink.");
            }

            lock (_operationLock)
            {
                if (!_selectionFinalized || generation != _drainGeneration)
                {
                    continue;
                }
            }

            while (true)
            {
                HttpWorkerScaleInState state = _scaleInManager.Current;
                if (state.Version != lastPublishedVersion)
                {
                    await PublishDrainingStateAsync(state);
                    lastPublishedVersion = state.Version;
                }

                if (state.LinkedWorkerCount == 0 &&
                    state.OutstandingWorkerRemovals.Count == 0)
                {
                    lock (_operationLock)
                    {
                        if (!_selectionFinalized || generation != _drainGeneration)
                        {
                            restart = true;
                            break;
                        }

                        _finalizationStarted = true;
                        SignalOperationChangedLocked();
                    }

                    AppServerHostStatePublishResult removalResult =
                        await _appServerClient.PublishLifecycleAsync(
                            RuntimeLifecycleStatus.RemovalRequested,
                            state,
                            CancellationToken.None);
                    EnsureApplied(removalResult, "publish app-version Runtime removal");
                    Log.RemovalRequested(_logger, _workerManager.Incarnation.Value);
                    return;
                }

                Task<long> stateChanged = _scaleInManager.WaitForChangeAsync(state.Version);
                lock (_operationLock)
                {
                    operationChanged = _operationChanged.Task;
                }

                Task completed = await Task.WhenAny(stateChanged, operationChanged);
                if (ReferenceEquals(completed, operationChanged))
                {
                    restart = true;
                    break;
                }

                await stateChanged;
            }

            if (!restart)
            {
                return;
            }
        }
    }

    private async Task PublishDrainingStateAsync(HttpWorkerScaleInState state)
    {
        AppServerHostStatePublishResult result = await _appServerClient.PublishLifecycleAsync(
            RuntimeLifecycleStatus.Draining,
            state,
            CancellationToken.None);
        EnsureApplied(result, "publish app-version Runtime drain progress");
    }

    private void SelectPhasedBatchLocked(
        AppVersionRuntimeDrainStart start,
        int batchSize,
        int batchIndex)
    {
        string[] workerIds = [.. start.NonHttpWorkerIds
            .Skip(batchIndex * batchSize)
            .Take(batchSize)];
        TrackSelectionLocked(_workerManager.SelectWorkers(
            start.Incarnation,
            workerIds,
            _timeProvider.GetUtcNow() + InvocationGracePeriod));
    }

    private void SelectAllRemainingLocked(DateTimeOffset selectionTime)
    {
        AppVersionRuntimeDrainStart start = _initialStart ??
            throw new InvalidOperationException("The app-version Runtime drain has not started.");
        TrackSelectionLocked(_workerManager.SelectAllRemainingWorkers(
            start.Incarnation,
            selectionTime + InvocationGracePeriod));
    }

    private void TrackSelectionLocked(AppVersionRuntimeDrainSelection selection)
    {
        if (selection.DrainTasks.Count == 0)
        {
            return;
        }

        _trackedDrains.AddRange(selection.DrainTasks);
        _drainGeneration++;
        SignalOperationChangedLocked();
    }

    private bool TryGetReplayLocked(
        Guid operationId,
        RuntimeDrainMode mode,
        RuntimeDrainReason reason,
        out Task<RuntimeDrainReceipt>? replay)
    {
        if (_initialOperation is not null && _initialOperation.OperationId == operationId)
        {
            replay = GetMatchingReplay(_initialOperation, mode, reason);
            return true;
        }

        if (_escalationOperation is not null && _escalationOperation.OperationId == operationId)
        {
            replay = GetMatchingReplay(_escalationOperation, mode, reason);
            return true;
        }

        replay = null;
        return false;
    }

    private static Task<RuntimeDrainReceipt> GetMatchingReplay(
        OperationRecord operation,
        RuntimeDrainMode mode,
        RuntimeDrainReason reason)
    {
        if (operation.Mode != mode || operation.Reason != reason)
        {
            throw new RuntimeDrainConflictException(
                "The operation identifier is already associated with a different drain payload.");
        }

        return operation.AdmissionTask;
    }

    private async Task DelayUntilAsync(DateTimeOffset target, CancellationToken cancellationToken)
    {
        TimeSpan delay = target - _timeProvider.GetUtcNow();
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, _timeProvider, cancellationToken);
        }
    }

    private void SignalOperationChangedLocked()
    {
        TaskCompletionSource changed = _operationChanged;
        _operationChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
        changed.TrySetResult();
    }

    private void ThrowIfLifecycleFailedLocked()
    {
        if (_lifecycleFailure is not null)
        {
            throw new RuntimeDrainAdmissionException(
                "The active Runtime drain lifecycle has already failed.",
                _lifecycleFailure);
        }
    }

    private async Task ObserveLifecycleAsync(Task lifecycle)
    {
        try
        {
            await lifecycle;
        }
        catch (Exception exception)
        {
            Log.LifecycleFailed(_logger, exception, _workerManager.Incarnation.Value);
        }
    }

    private async Task ObservePhasedScheduleAsync(Task schedule)
    {
        try
        {
            await schedule;
        }
        catch (Exception exception)
        {
            Log.ScheduleFailed(_logger, exception, _workerManager.Incarnation.Value);
        }
    }

    private static void EnsureApplied(AppServerHostStatePublishResult result, string operation)
    {
        if (result is not AppServerHostStatePublishResult.Applied)
        {
            throw new RuntimeDrainAdmissionException(
                $"AppServer could not {operation}: {result}.");
        }
    }

    private sealed class OperationRecord(
        Guid operationId,
        RuntimeDrainMode mode,
        RuntimeDrainReason reason)
    {
        public Task<RuntimeDrainReceipt> AdmissionTask { get; set; } = null!;

        public RuntimeDrainMode Mode { get; } = mode;

        public Guid OperationId { get; } = operationId;

        public RuntimeDrainReason Reason { get; } = reason;
    }

    private static partial class Log
    {
        // EventId range is 1210-1219
        [LoggerMessage(1210, LogLevel.Information,
            "Accepted app-version Runtime drain operation {OperationId} in {Mode} mode for reason {Reason}, Runtime incarnation {RuntimeIncarnation}, at {AcceptedAtUtc}.")]
        public static partial void Accepted(
            ILogger logger,
            Guid operationId,
            RuntimeDrainMode mode,
            RuntimeDrainReason reason,
            Guid runtimeIncarnation,
            DateTimeOffset acceptedAtUtc);

        [LoggerMessage(1211, LogLevel.Warning,
            "Escalated app-version Runtime drain operation {OperationId} to Immediate mode for reason {Reason}, Runtime incarnation {RuntimeIncarnation}, at {AcceptedAtUtc}.")]
        public static partial void Escalated(
            ILogger logger,
            Guid operationId,
            RuntimeDrainReason reason,
            Guid runtimeIncarnation,
            DateTimeOffset acceptedAtUtc);

        [LoggerMessage(1212, LogLevel.Warning,
            "Published RemovalRequested for app-version Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void RemovalRequested(ILogger logger, Guid runtimeIncarnation);

        [LoggerMessage(1213, LogLevel.Error,
            "App-version Runtime drain lifecycle failed for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void LifecycleFailed(
            ILogger logger,
            Exception exception,
            Guid runtimeIncarnation);

        [LoggerMessage(1214, LogLevel.Error,
            "Phased app-version Runtime drain schedule failed for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void ScheduleFailed(
            ILogger logger,
            Exception exception,
            Guid runtimeIncarnation);
    }
}

internal sealed class RuntimeDrainConflictException(string message) : InvalidOperationException(message);

internal sealed class RuntimeDrainAdmissionException : InvalidOperationException
{
    public RuntimeDrainAdmissionException(string message)
        : base(message)
    {
    }

    public RuntimeDrainAdmissionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
