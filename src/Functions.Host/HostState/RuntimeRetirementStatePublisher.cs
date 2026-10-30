// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Rpc.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Azure.Functions.Host.HostState;

internal sealed partial class RuntimeRetirementStatePublisher(
    IHttpWorkerScaleInManager scaleInManager,
    AppServerHostStateClient appServerClient,
    TimeProvider timeProvider,
    ILogger<RuntimeRetirementStatePublisher> logger) : IHostedService, IDisposable
{
    internal static readonly TimeSpan PublishInterval = TimeSpan.FromMinutes(1);

    private readonly AppServerHostStateClient _appServerClient =
        appServerClient ?? throw new ArgumentNullException(nameof(appServerClient));

    private readonly Lock _lifecycleLock = new();
    private readonly ILogger<RuntimeRetirementStatePublisher> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly IHttpWorkerScaleInManager _scaleInManager =
        scaleInManager ?? throw new ArgumentNullException(nameof(scaleInManager));

    private readonly CancellationTokenSource _shutdownSource = new();
    private readonly TimeProvider _timeProvider =
        timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

    private RuntimeLifecycleStatus? _status;
    private bool _disposed;
    private Task? _publisherTask;
    private TaskCompletionSource _statusChanged =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Task? publisher;
        lock (_lifecycleLock)
        {
            if (_disposed)
            {
                return;
            }

            _shutdownSource.Cancel();
            publisher = _publisherTask;
        }

        if (publisher is not null)
        {
            await publisher.WaitAsync(cancellationToken);
        }
    }

    public void Dispose()
    {
        lock (_lifecycleLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _shutdownSource.Cancel();
            _shutdownSource.Dispose();
        }
    }

    internal void RetainOwnership(RuntimeLifecycleStatus? status)
    {
        if (status is RuntimeLifecycleStatus.RemovalRequested)
        {
            throw new ArgumentOutOfRangeException(nameof(status));
        }

        lock (_lifecycleLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (status is RuntimeLifecycleStatus.Draining)
            {
                _status = status;
            }

            if (_publisherTask is null)
            {
                _publisherTask = Task.Run(() => RunAsync(_shutdownSource.Token));
                _ = ObservePublisherAsync(_publisherTask);
            }

            SignalStatusChangedLocked();
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            RuntimeLifecycleStatus? status;
            Task statusChanged;
            lock (_lifecycleLock)
            {
                status = _status;
                statusChanged = _statusChanged.Task;
            }

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
                    await _appServerClient.PublishRetirementStateAsync(
                        state,
                        status,
                        cancellationToken);
                if (result is not AppServerHostStatePublishResult.Applied)
                {
                    Log.PublicationUnavailable(_logger, result, status);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                Log.PublicationFailed(_logger, exception, status);
            }

            using CancellationTokenSource intervalSource =
                new(PublishInterval, _timeProvider);
            using CancellationTokenSource waitSource =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    intervalSource.Token);
            try
            {
                Task<long> stateChanged = _scaleInManager.WaitForChangeAsync(
                    state.Version,
                    waitSource.Token);
                await Task.WhenAny(stateChanged, statusChanged).WaitAsync(waitSource.Token);
                await waitSource.CancelAsync();
                try
                {
                    await stateChanged;
                }
                catch (OperationCanceledException) when (waitSource.IsCancellationRequested)
                {
                }
            }
            catch (OperationCanceledException) when (
                intervalSource.IsCancellationRequested &&
                !cancellationToken.IsCancellationRequested)
            {
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
        }
    }

    private async Task ObservePublisherAsync(Task publisher)
    {
        try
        {
            await publisher;
        }
        catch (Exception exception)
        {
            Log.PublisherStopped(_logger, exception);
        }
    }

    private void SignalStatusChangedLocked()
    {
        TaskCompletionSource changed = _statusChanged;
        _statusChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
        changed.TrySetResult();
    }

    private static partial class Log
    {
        [LoggerMessage(1220, LogLevel.Warning,
            "Runtime retirement Host-state publication returned {Result} for lifecycle status {Status}.")]
        public static partial void PublicationUnavailable(
            ILogger logger,
            AppServerHostStatePublishResult result,
            RuntimeLifecycleStatus? status);

        [LoggerMessage(1221, LogLevel.Warning,
            "Runtime retirement Host-state publication failed for lifecycle status {Status}.")]
        public static partial void PublicationFailed(
            ILogger logger,
            Exception exception,
            RuntimeLifecycleStatus? status);

        [LoggerMessage(1222, LogLevel.Error,
            "Runtime retirement Host-state publisher stopped unexpectedly.")]
        public static partial void PublisherStopped(
            ILogger logger,
            Exception exception);
    }
}
