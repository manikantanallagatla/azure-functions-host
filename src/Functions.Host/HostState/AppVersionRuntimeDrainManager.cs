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
    private static readonly TimeSpan FailedAdmissionPublishInterval = TimeSpan.FromMinutes(1);

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
    private Task? _failedAdmissionPublisherTask;
    private Exception? _drainFailure;
    private Exception? _lifecycleFailure;
    private Exception? _selectionFailure;
    private bool _selectionFinalized;

    public Task<RuntimeDrainReceipt> AcceptAsync(
        RuntimeDrainMode mode,
        RuntimeDrainReason reason)
    {
        lock (_operationLock)
        {
            if (_initialOperation is null)
            {
                if (!_workerManager.TryStartAppVersionDrain(out AppVersionRuntimeDrainStart? start))
                {
                    throw new RuntimeDrainConflictException(
                        "The Runtime is already executing a different retirement operation.");
                }

                _initialStart = start;
                _activeMode = mode;
                OperationRecord operation = new(mode, reason);
                _initialOperation = operation;
                operation.AdmissionTask = AdmitInitialAsync(operation, start);
                return operation.AdmissionTask;
            }

            if (_activeMode is RuntimeDrainMode.Immediate &&
                mode is RuntimeDrainMode.Phased)
            {
                throw new RuntimeDrainConflictException(
                    "The Runtime is already executing an Immediate drain operation.");
            }

            ThrowIfDrainFailedLocked();
            if (_escalationOperation is not null &&
                mode is RuntimeDrainMode.Immediate)
            {
                return _escalationOperation.AdmissionTask;
            }

            if (_activeMode == mode)
            {
                return _initialOperation.AdmissionTask;
            }

            if (_activeMode is RuntimeDrainMode.Phased &&
                mode is RuntimeDrainMode.Immediate &&
                _escalationOperation is null &&
                !_finalizationStarted)
            {
                OperationRecord operation = new(mode, reason);
                _escalationOperation = operation;
                operation.AdmissionTask = EscalateAsync(operation, _initialOperation.AdmissionTask);
                return operation.AdmissionTask;
            }

            throw new RuntimeDrainConflictException(
                "The Runtime is already executing a different drain operation.");
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
            EnsureApplied(fenceResult, "apply the Runtime drain fence");
        }
        catch (Exception exception)
        {
            StartFailedAdmissionPublisher();
            if (exception is HttpRequestException)
            {
                throw new RuntimeDrainAdmissionException(
                    "AppServer was unavailable while committing the Runtime drain fence.",
                    exception);
            }

            if (exception is OperationCanceledException)
            {
                throw new RuntimeDrainAdmissionException(
                    "AppServer timed out while committing the Runtime drain fence.",
                    exception);
            }

            throw;
        }

        DateTimeOffset acceptedAtUtc = _timeProvider.GetUtcNow();
        RuntimeDrainReceipt receipt = new(
            operation.Mode.ToString(),
            acceptedAtUtc);

        CancellationTokenSource? phasedScheduleSource = null;
        int phasedBatchSize = 0;
        lock (_operationLock)
        {
            TrackDrainsLocked(start.ExistingDrains);

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
            ThrowIfDrainFailedLocked();
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
            operation.Reason,
            _workerManager.Incarnation.Value,
            acceptedAtUtc);

        return new(
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
            Exception? drainFailure;
            Exception? selectionFailure;
            lock (_operationLock)
            {
                LatchCompletedDrainFailuresLocked();
                drainFailure = _drainFailure;
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
                    "The Runtime drain schedule failed.",
                    selectionFailure);
            }

            if (drainFailure is not null)
            {
                throw new InvalidOperationException(
                    "At least one Worker could not complete its Runtime drain and unlink.",
                    drainFailure);
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
                    "At least one Worker could not complete its Runtime drain and unlink.");
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
                    EnsureApplied(removalResult, "publish Runtime removal");
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
        EnsureApplied(result, "publish Runtime drain progress");
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
            throw new InvalidOperationException("The Runtime drain has not started.");
        TrackSelectionLocked(_workerManager.SelectAllRemainingWorkers(
            start.Incarnation,
            selectionTime + InvocationGracePeriod));
    }

    private void TrackSelectionLocked(AppVersionRuntimeDrainSelection selection)
    {
        TrackDrainsLocked(selection.DrainTasks);
    }

    private void TrackDrainsLocked(IReadOnlyList<Task<bool>> drains)
    {
        if (drains.Count == 0)
        {
            return;
        }

        _trackedDrains.AddRange(drains);
        _drainGeneration++;
        foreach (Task<bool> drain in drains)
        {
            _ = drain.ContinueWith(
                static (task, state) => ((AppVersionRuntimeDrainManager)state!).ObserveTrackedDrain(task),
                this,
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);
        }

        SignalOperationChangedLocked();
    }

    private void ObserveTrackedDrain(Task<bool> drain)
    {
        lock (_operationLock)
        {
            LatchDrainFailureLocked(drain);
        }
    }

    private void LatchCompletedDrainFailuresLocked()
    {
        foreach (Task<bool> drain in _trackedDrains)
        {
            if (drain.IsCompleted)
            {
                LatchDrainFailureLocked(drain);
            }
        }
    }

    private void LatchDrainFailureLocked(Task<bool> drain)
    {
        if (_drainFailure is not null)
        {
            return;
        }

        Exception? failure = drain.Status switch
        {
            TaskStatus.RanToCompletion when !drain.Result =>
                new InvalidOperationException("The Worker drain completed without a confirmed removal handoff."),
            TaskStatus.Faulted => drain.Exception?.GetBaseException() ??
                new InvalidOperationException("The Worker drain faulted without an exception."),
            TaskStatus.Canceled => new OperationCanceledException("The Worker drain was canceled."),
            _ => null,
        };
        if (failure is null)
        {
            return;
        }

        _drainFailure = failure;
        SignalOperationChangedLocked();
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

    private void ThrowIfDrainFailedLocked()
    {
        LatchCompletedDrainFailuresLocked();
        Exception? failure = _lifecycleFailure ?? _selectionFailure ?? _drainFailure;
        if (failure is not null)
        {
            throw new RuntimeDrainAdmissionException(
                "The active Runtime drain lifecycle has already failed.",
                failure);
        }
    }

    private void StartFailedAdmissionPublisher()
    {
        Task publisher;
        lock (_operationLock)
        {
            publisher = _failedAdmissionPublisherTask ??= RunFailedAdmissionPublisherAsync();
        }

        _ = ObserveFailedAdmissionPublisherAsync(publisher);
    }

    private async Task RunFailedAdmissionPublisherAsync()
    {
        while (true)
        {
            HttpWorkerScaleInState state;
            try
            {
                state = _scaleInManager.Current;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            try
            {
                AppServerHostStatePublishResult result =
                    await _appServerClient.PublishRetirementPendingAsync(state, CancellationToken.None);
                if (result is not AppServerHostStatePublishResult.Applied)
                {
                    Log.FailedAdmissionPublicationUnavailable(_logger, result);
                }
            }
            catch (Exception exception)
            {
                Log.FailedAdmissionPublicationFailed(_logger, exception);
            }

            using CancellationTokenSource intervalSource =
                new(FailedAdmissionPublishInterval, _timeProvider);
            try
            {
                await _scaleInManager.WaitForChangeAsync(state.Version, intervalSource.Token);
            }
            catch (OperationCanceledException) when (intervalSource.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException)
            {
                return;
            }
        }
    }

    private async Task ObserveFailedAdmissionPublisherAsync(Task publisher)
    {
        try
        {
            await publisher;
        }
        catch (Exception exception)
        {
            Log.FailedAdmissionPublisherStopped(_logger, exception);
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
        RuntimeDrainMode mode,
        RuntimeDrainReason reason)
    {
        public Task<RuntimeDrainReceipt> AdmissionTask { get; set; } = null!;

        public RuntimeDrainMode Mode { get; } = mode;

        public RuntimeDrainReason Reason { get; } = reason;
    }

    private static partial class Log
    {
        // EventId range is 1210-1219
        [LoggerMessage(1210, LogLevel.Information,
            "Accepted Runtime drain in {Mode} mode for reason {Reason}, Runtime incarnation {RuntimeIncarnation}, at {AcceptedAtUtc}.")]
        public static partial void Accepted(
            ILogger logger,
            RuntimeDrainMode mode,
            RuntimeDrainReason reason,
            Guid runtimeIncarnation,
            DateTimeOffset acceptedAtUtc);

        [LoggerMessage(1211, LogLevel.Warning,
            "Escalated Runtime drain to Immediate mode for reason {Reason}, Runtime incarnation {RuntimeIncarnation}, at {AcceptedAtUtc}.")]
        public static partial void Escalated(
            ILogger logger,
            RuntimeDrainReason reason,
            Guid runtimeIncarnation,
            DateTimeOffset acceptedAtUtc);

        [LoggerMessage(1212, LogLevel.Warning,
            "Published RemovalRequested for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void RemovalRequested(ILogger logger, Guid runtimeIncarnation);

        [LoggerMessage(1213, LogLevel.Error,
            "Runtime drain lifecycle failed for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void LifecycleFailed(
            ILogger logger,
            Exception exception,
            Guid runtimeIncarnation);

        [LoggerMessage(1214, LogLevel.Error,
            "Phased Runtime drain schedule failed for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void ScheduleFailed(
            ILogger logger,
            Exception exception,
            Guid runtimeIncarnation);

        [LoggerMessage(1215, LogLevel.Warning,
            "Runtime drain admission failed and pending Host-state publication returned {Result}.")]
        public static partial void FailedAdmissionPublicationUnavailable(
            ILogger logger,
            AppServerHostStatePublishResult result);

        [LoggerMessage(1216, LogLevel.Warning,
            "Runtime drain admission failed and pending Host-state publication failed.")]
        public static partial void FailedAdmissionPublicationFailed(
            ILogger logger,
            Exception exception);

        [LoggerMessage(1217, LogLevel.Error,
            "Runtime drain admission failure publisher stopped unexpectedly.")]
        public static partial void FailedAdmissionPublisherStopped(
            ILogger logger,
            Exception exception);
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
