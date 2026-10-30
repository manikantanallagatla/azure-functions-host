// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Rpc.Client;
using Microsoft.Extensions.Logging;

namespace Azure.Functions.Host.HostState;

internal sealed partial class RuntimeRetirementManager(
    IRuntimeRetirementWorkerManager workerManager,
    IHttpWorkerScaleInManager scaleInManager,
    AppServerHostStateClient appServerClient,
    RuntimeRetirementStatePublisher retirementStatePublisher,
    ILogger<RuntimeRetirementManager> logger) : IRuntimeRetirementManager
{
    private readonly AppServerHostStateClient _appServerClient =
        appServerClient ?? throw new ArgumentNullException(nameof(appServerClient));

    private readonly Lock _lifecycleLock = new();
    private readonly ILogger<RuntimeRetirementManager> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly IHttpWorkerScaleInManager _scaleInManager =
        scaleInManager ?? throw new ArgumentNullException(nameof(scaleInManager));

    private readonly RuntimeRetirementStatePublisher _retirementStatePublisher =
        retirementStatePublisher ?? throw new ArgumentNullException(nameof(retirementStatePublisher));

    private readonly IRuntimeRetirementWorkerManager _workerManager =
        workerManager ?? throw new ArgumentNullException(nameof(workerManager));

    private Task? _retirementTask;

    public RuntimeIncarnation Incarnation => _workerManager.Incarnation;

    public Task RequestRetirementAsync(SplitScriptHostActivationFaulted condition)
    {
        ArgumentNullException.ThrowIfNull(condition);

        lock (_lifecycleLock)
        {
            if (condition.Incarnation != Incarnation)
            {
                Log.StaleDetectorIgnored(_logger, condition.Incarnation.Value, Incarnation.Value);
                return Task.CompletedTask;
            }

            return _retirementTask ??= RetireAsync(condition);
        }
    }

    private async Task RetireAsync(SplitScriptHostActivationFaulted condition)
    {
        if (!_workerManager.TryCloseAdmission(condition))
        {
            Log.StaleDetectorIgnored(_logger, condition.Incarnation.Value, Incarnation.Value);
            return;
        }

        RuntimeLifecycleStatus? retainedStatus = null;
        try
        {
            AppServerHostStatePublishResult bindResult =
                await _appServerClient.BindRuntimeIncarnationAsync(CancellationToken.None);
            EnsureApplied(bindResult, "bind the Runtime incarnation");

            HttpWorkerScaleInState state = _scaleInManager.Current;
            AppServerHostStatePublishResult drainFenceResult = await _appServerClient.PublishLifecycleAsync(
                RuntimeLifecycleStatus.Draining,
                state,
                CancellationToken.None);
            EnsureApplied(drainFenceResult, "apply the Runtime drain fence");
            retainedStatus = RuntimeLifecycleStatus.Draining;
            Log.DrainFenceApplied(_logger, condition.Incarnation.Value);

            long lastPublishedVersion = state.Version;
            Task<RuntimeRetirementDrainResult> drainTask = _workerManager.DrainAllWorkersAsync(
                condition.Incarnation,
                CancellationToken.None);
            long observedVersion = state.Version;
            while (!drainTask.IsCompleted)
            {
                Task<long> stateChanged = _scaleInManager.WaitForChangeAsync(observedVersion);
                Task completed = await Task.WhenAny(drainTask, stateChanged);
                if (ReferenceEquals(completed, drainTask))
                {
                    break;
                }

                observedVersion = await stateChanged;
                state = _scaleInManager.Current;
                AppServerHostStatePublishResult progressResult = await _appServerClient.PublishLifecycleAsync(
                    RuntimeLifecycleStatus.Draining,
                    state,
                    CancellationToken.None);
                EnsureApplied(progressResult, "publish Runtime drain progress");
                lastPublishedVersion = state.Version;
            }

            RuntimeRetirementDrainResult drainResult = await drainTask;
            if (!drainResult.IsComplete)
            {
                throw new InvalidOperationException(
                    drainResult.Failure ?? "Runtime retirement drain did not complete.");
            }

            state = _scaleInManager.Current;
            if (state.LinkedWorkerCount != 0)
            {
                throw new InvalidOperationException(
                    "Runtime retirement drain completed before local links were cleared.");
            }

            bool hadFallbackHandoffs = state.OutstandingWorkerRemovals.Count != 0;
            while (state.OutstandingWorkerRemovals.Count != 0)
            {
                if (state.Version != lastPublishedVersion)
                {
                    AppServerHostStatePublishResult fallbackResult = await _appServerClient.PublishLifecycleAsync(
                        RuntimeLifecycleStatus.Draining,
                        state,
                        CancellationToken.None);
                    EnsureApplied(fallbackResult, "publish Runtime worker-removal fallback");
                    lastPublishedVersion = state.Version;
                }

                long fallbackVersion = state.Version;
                await _scaleInManager.WaitForChangeAsync(fallbackVersion);
                state = _scaleInManager.Current;
                if (state.LinkedWorkerCount != 0)
                {
                    throw new InvalidOperationException(
                        "A local Worker Link appeared after Runtime retirement admission closed.");
                }
            }

            if (hadFallbackHandoffs && state.Version != lastPublishedVersion)
            {
                AppServerHostStatePublishResult clearedFallbackResult = await _appServerClient.PublishLifecycleAsync(
                    RuntimeLifecycleStatus.Draining,
                    state,
                    CancellationToken.None);
                EnsureApplied(clearedFallbackResult, "publish cleared Runtime drain state");
            }

            HttpWorkerScaleInState finalState = _scaleInManager.Current;
            if (finalState.Version != state.Version ||
                finalState.LinkedWorkerCount != 0 ||
                finalState.OutstandingWorkerRemovals.Count != 0)
            {
                throw new InvalidOperationException(
                    "Runtime retirement state changed before final removal publication.");
            }

            AppServerHostStatePublishResult removalResult = await _appServerClient.PublishLifecycleAsync(
                RuntimeLifecycleStatus.RemovalRequested,
                finalState,
                CancellationToken.None);
            EnsureApplied(removalResult, "publish Runtime removal");
            Log.RemovalRequested(_logger, condition.Incarnation.Value);
        }
        catch
        {
            _retirementStatePublisher.RetainOwnership(retainedStatus);
            throw;
        }
    }

    private static void EnsureApplied(AppServerHostStatePublishResult result, string operation)
    {
        if (result is not AppServerHostStatePublishResult.Applied)
        {
            throw new InvalidOperationException($"AppServer could not {operation}: {result}.");
        }
    }

    private static partial class Log
    {
        // EventId range is 1200-1299
        [LoggerMessage(1200, LogLevel.Debug,
            "Ignored stale Runtime retirement detector for incarnation {DetectorIncarnation}; current incarnation is {CurrentIncarnation}.")]
        public static partial void StaleDetectorIgnored(
            ILogger logger,
            Guid detectorIncarnation,
            Guid currentIncarnation);

        [LoggerMessage(1201, LogLevel.Information,
            "AppServer acknowledged the drain fence for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void DrainFenceApplied(ILogger logger, Guid runtimeIncarnation);

        [LoggerMessage(1202, LogLevel.Warning,
            "Published RemovalRequested for Runtime incarnation {RuntimeIncarnation}.")]
        public static partial void RemovalRequested(ILogger logger, Guid runtimeIncarnation);
    }
}
