// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Script;
using Microsoft.Azure.WebJobs.Script.AppCapabilities;
using Microsoft.Azure.WebJobs.Script.Config;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Diagnostics;
using Microsoft.Azure.WebJobs.Script.Eventing;
using Microsoft.Azure.WebJobs.Script.Grpc;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using Microsoft.Azure.WebJobs.Script.Http;
using Microsoft.Azure.WebJobs.Script.Workers;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Azure.WebJobs.Script.Workers.SharedMemoryDataTransfer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Azure.Functions.Rpc.Client;

/// <summary>
/// Runs the shared worker protocol over a client-owned FunctionRpc channel.
/// </summary>
/// <remarks>
/// Concurrent and repeated starts share one initialization attempt. A token already canceled when <see cref="StartAsync"/> is called does not
/// begin initialization. Once initialization begins, cancellation only stops the individual caller's wait; shared initialization continues
/// until it succeeds, fails, times out, or the channel is disposed.
/// </remarks>
internal sealed class RpcClientWorkerChannel : WorkerChannel
{
    internal const string FunctionGroupNameCapability = "FunctionGroupName";
    internal const string IsAlwaysReadyCapability = "IsAlwaysReady";

    private readonly Dictionary<Guid, InvocationPermit> _committedInvocations = [];
    private readonly TaskCompletionSource<Exception> _functionLoadFailure = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Lock _invocationAdmissionLock = new();
    private readonly Lock _lifecycleLock = new();
    private readonly ChannelWriter<StreamingMessage> _requestWriter;
    private readonly TaskCompletionSource _drainCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _startCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TimeProvider _timeProvider;
    private bool _draining;
    private long? _idleSinceTimestamp;
    private bool? _isAlwaysReady;
    private bool _scaleInReady;
    private Guid? _scaleInReservationId;
    private bool _terminalAdmissionClosed;
    private string _functionGroupName;
    private LifecycleState _lifecycleState;

    public RpcClientWorkerChannel(
        string workerId,
        DuplexChannel<StreamingMessage> ownedChannel,
        IScriptEventManager eventManager,
        IScriptHostManager hostManager,
        RpcWorkerConfig workerConfig,
        ILogger logger,
        IMetricsLogger metricsLogger,
        int attemptCount,
        IEnvironment environment,
        IOptionsMonitor<ScriptApplicationHostOptions> applicationHostOptions,
        ISharedMemoryManager sharedMemoryManager,
        IOptions<WorkerConcurrencyOptions> workerConcurrencyOptions,
        IOptions<FunctionsHostingConfigOptions> hostingConfigOptions,
        IAppCapabilitiesStore appCapabilitiesStore,
        IHttpProxyService httpProxyService,
        TimeProvider timeProvider = null)
        : base(
            workerId,
            ownedChannel,
            eventManager,
            hostManager,
            workerConfig,
            logger,
            metricsLogger,
            attemptCount,
            environment,
            applicationHostOptions,
            sharedMemoryManager,
            workerConcurrencyOptions,
            hostingConfigOptions,
            appCapabilitiesStore,
            httpProxyService)
    {
        _requestWriter = ownedChannel.Writer;
        _timeProvider = timeProvider ?? TimeProvider.System;
        Completion = ownedChannel.Reader.Completion;
    }

    private enum LifecycleState
    {
        NotStarted,
        Started,
        Disposed,
    }

    internal Task Completion { get; }

    internal Task<Exception> FunctionLoadFailure => _functionLoadFailure.Task;

    internal bool IsTerminated => Completion.IsCompleted || FunctionLoadFailure.IsCompleted;

    internal bool ContributesHttpCapacity
    {
        get
        {
            lock (_invocationAdmissionLock)
            {
                return IsHttpFunctionGroup &&
                    _scaleInReady &&
                    !_draining &&
                    _scaleInReservationId is null &&
                    !_terminalAdmissionClosed &&
                    !IsTerminated &&
                    IsChannelReadyForInvocations();
            }
        }
    }

    // Terminal signals can precede registry cleanup and the base channel's disposal state.
    internal bool IsReadyForInvocations =>
        Volatile.Read(ref _scaleInReady) &&
        !Volatile.Read(ref _draining) &&
        !IsScaleInReserved &&
        !IsTerminated &&
        IsChannelReadyForInvocations();

    private bool IsScaleInReserved
    {
        get
        {
            lock (_invocationAdmissionLock)
            {
                return _scaleInReservationId is not null;
            }
        }
    }

    private bool IsHttpFunctionGroup =>
        string.Equals(_functionGroupName, FunctionGroups.Http, StringComparison.OrdinalIgnoreCase);

    internal void CompleteRequests() => _requestWriter.TryComplete();

    internal bool TryDispatchInvocation(ScriptInvocationContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        InvocationPermit permit = null;
        bool posted = false;
        try
        {
            lock (_invocationAdmissionLock)
            {
                if (_draining ||
                    _scaleInReservationId is not null ||
                    _terminalAdmissionClosed ||
                    IsTerminated ||
                    !_scaleInReady ||
                    !IsChannelReadyForInvocations())
                {
                    return false;
                }

                string functionId = context.FunctionMetadata.GetFunctionId();
                if (!FunctionInputBuffers.TryGetValue(functionId, out BufferBlock<ScriptInvocationContext> inputBuffer))
                {
                    throw new InvalidOperationException(
                        $"Function '{context.FunctionMetadata.Name}' is not loaded by worker '{Id}'.");
                }

                Guid invocationId = context.ExecutionContext.InvocationId;
                permit = new(this, context);
                if (!_committedInvocations.TryAdd(invocationId, permit))
                {
                    throw new InvalidOperationException($"Invocation '{invocationId}' is already committed to worker '{Id}'.");
                }

                posted = inputBuffer.Post(context);
                if (posted)
                {
                    _idleSinceTimestamp = null;
                }
            }

            if (!posted)
            {
                return false;
            }

            permit.ObserveResult(context.ResultSource.Task);
            permit = null;

            return true;
        }
        finally
        {
            permit?.Dispose();
        }
    }

    /// <summary>
    /// Starts inbound protocol processing and waits for the worker initialization handshake.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels this caller's wait without canceling shared initialization.</param>
    /// <returns>A task that completes after a successful WorkerInitResponse, is canceled for this caller, or faults when initialization cannot complete.</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        lock (_lifecycleLock)
        {
            ObjectDisposedException.ThrowIf(_lifecycleState is LifecycleState.Disposed, this);

            if (_lifecycleState is LifecycleState.NotStarted)
            {
                _lifecycleState = LifecycleState.Started;

                try
                {
                    MarkWorkerInitializing();
                    BeginInboundProcessing(ValidateStartStreamTimeout(WorkerConfig.CountOptions.ProcessStartupTimeout));
                    _ = CompleteStartAsync();
                }
                catch (Exception exception)
                {
                    _startCompletion.TrySetException(exception);
                }
            }
        }

        return _startCompletion.Task.WaitAsync(cancellationToken);
    }

    protected override void OnFunctionLoadError(Exception exception) => _functionLoadFailure.TrySetResult(exception);

    protected override void OnMetadataRequestError(Exception exception) => FailPendingMetadataRequest(exception);

    protected override void OnChannelFailure(Exception exception) => FailPendingMetadataRequest(exception);

    internal override bool TryBeginInvocationDispatch(ScriptInvocationContext context)
    {
        lock (_invocationAdmissionLock)
        {
            if (_terminalAdmissionClosed ||
                !_committedInvocations.ContainsKey(context.ExecutionContext.InvocationId))
            {
                return false;
            }

            return true;
        }
    }

    internal override bool TryBeginInvocationSend(ScriptInvocationContext context, bool usesHttpProxy)
    {
        lock (_invocationAdmissionLock)
        {
            if (_terminalAdmissionClosed ||
                !_committedInvocations.TryGetValue(context.ExecutionContext.InvocationId, out InvocationPermit permit))
            {
                return false;
            }

            return permit.TryBeginSend(usesHttpProxy);
        }
    }

    internal override void OnInvocationDispatchCompleted(ScriptInvocationContext context, Task httpForwardingTask)
    {
        InvocationPermit permit;
        lock (_invocationAdmissionLock)
        {
            _committedInvocations.TryGetValue(context.ExecutionContext.InvocationId, out permit);
        }

        permit?.ObserveHttpForwarding(httpForwardingTask);
    }

    protected override void Dispose(bool disposing)
    {
        lock (_lifecycleLock)
        {
            _lifecycleState = LifecycleState.Disposed;
        }

        if (disposing)
        {
            // Metadata callers did not request cancellation; fail so host startup can retry.
            FailPendingMetadataRequest(new ObjectDisposedException(GetType().FullName));
        }

        base.Dispose(disposing);
    }

    internal void FailCommittedInvocations(Exception exception)
    {
        InvocationPermit[] failures;
        lock (_invocationAdmissionLock)
        {
            _terminalAdmissionClosed = true;
            _scaleInReservationId = null;
            Volatile.Write(ref _draining, true);
            failures = [.. _committedInvocations.Values];
            CompleteDrainIfReadyLocked();
        }

        Exception shutdownException = exception;
        if (exception is null || exception is FunctionTimeoutException)
        {
            string suffix = exception?.Message is not null ? " Reason: " + exception.Message : string.Empty;
            shutdownException = new FunctionTimeoutAbortException(
                "Worker channel is shutting down. Aborting function." + suffix,
                exception);
        }

        foreach (InvocationPermit permit in failures)
        {
            permit.Fail(shutdownException);
        }
    }

    private void CompleteDrainIfReadyLocked()
    {
        if (_draining && _committedInvocations.Count == 0)
        {
            _drainCompletion.TrySetResult();
        }
    }

    private void ReleaseInvocation(Guid invocationId, InvocationPermit permit)
    {
        lock (_invocationAdmissionLock)
        {
            if (_committedInvocations.TryGetValue(invocationId, out InvocationPermit current) &&
                ReferenceEquals(current, permit))
            {
                _committedInvocations.Remove(invocationId);
                if (_committedInvocations.Count == 0 && _scaleInReady && !_draining && !_terminalAdmissionClosed)
                {
                    _idleSinceTimestamp ??= _timeProvider.GetTimestamp();
                }

                CompleteDrainIfReadyLocked();
            }
        }
    }

    private async Task CompleteStartAsync()
    {
        try
        {
            if (!await WorkerInitialization.ConfigureAwait(false))
            {
                throw new InvalidOperationException("The worker reported unsuccessful initialization.");
            }

            _functionGroupName = WorkerCapabilities.GetCapabilityState(FunctionGroupNameCapability);
            string isAlwaysReady = WorkerCapabilities.GetCapabilityState(IsAlwaysReadyCapability);
            _isAlwaysReady = bool.TryParse(isAlwaysReady, out bool parsedIsAlwaysReady)
                ? parsedIsAlwaysReady
                : null;

            _startCompletion.TrySetResult();
        }
        catch (OperationCanceledException exception)
        {
            _startCompletion.TrySetCanceled(exception.CancellationToken);
        }
        catch (Exception exception)
        {
            _startCompletion.TrySetException(exception);
        }
    }

    internal void SetScaleInReady(bool ready)
    {
        lock (_invocationAdmissionLock)
        {
            Volatile.Write(ref _scaleInReady, ready);
            if (!ready)
            {
                _scaleInReservationId = null;
                _idleSinceTimestamp = null;
            }
            else if (_committedInvocations.Count == 0 &&
                !_draining &&
                _scaleInReservationId is null &&
                !_terminalAdmissionClosed)
            {
                _idleSinceTimestamp ??= _timeProvider.GetTimestamp();
            }
        }
    }

    internal bool TryReserveIdleScaleIn(TimeSpan minimumIdleDuration, out Guid reservationId)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumIdleDuration, TimeSpan.Zero);

        lock (_invocationAdmissionLock)
        {
            long now = _timeProvider.GetTimestamp();
            if (!IsHttpFunctionGroup ||
                _isAlwaysReady is not false ||
                !_scaleInReady ||
                _draining ||
                _scaleInReservationId is not null ||
                _terminalAdmissionClosed ||
                IsTerminated ||
                _committedInvocations.Count != 0 ||
                _idleSinceTimestamp is not { } idleSince ||
                _timeProvider.GetElapsedTime(idleSince, now) < minimumIdleDuration)
            {
                reservationId = default;
                return false;
            }

            reservationId = Guid.NewGuid();
            _scaleInReservationId = reservationId;
            return true;
        }
    }

    internal bool CancelScaleInReservation(Guid reservationId)
    {
        lock (_invocationAdmissionLock)
        {
            if (_scaleInReservationId != reservationId || _draining || _terminalAdmissionClosed)
            {
                return false;
            }

            _scaleInReservationId = null;
            return true;
        }
    }

    internal Task CommitScaleInReservation(Guid reservationId)
    {
        lock (_invocationAdmissionLock)
        {
            if (_scaleInReservationId != reservationId || _terminalAdmissionClosed)
            {
                return null;
            }

            _scaleInReservationId = null;
            Volatile.Write(ref _draining, true);
            CompleteDrainIfReadyLocked();
            return _drainCompletion.Task;
        }
    }

    private static TimeSpan ValidateStartStreamTimeout(TimeSpan startStreamTimeout)
    {
        if (startStreamTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(startStreamTimeout), "The StartStream timeout must be greater than zero.");
        }

        return startStreamTimeout;
    }

    private sealed class InvocationPermit : IDisposable
    {
        private readonly ScriptInvocationContext _context;
        private readonly Guid _invocationId;
        private readonly RpcClientWorkerChannel _owner;
        private int _httpCompletionObserved;
        private int _released;
        private int _remainingCompletions = 1;
        private bool _requiresHttpCompletion;
        private bool _sendStarted;

        public InvocationPermit(RpcClientWorkerChannel owner, ScriptInvocationContext context)
        {
            _owner = owner;
            _context = context;
            _invocationId = context.ExecutionContext.InvocationId;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                _owner.ReleaseInvocation(_invocationId, this);
            }
        }

        public bool TryBeginSend(bool usesHttpProxy)
        {
            if (_sendStarted)
            {
                return false;
            }

            _sendStarted = true;
            if (usesHttpProxy)
            {
                _requiresHttpCompletion = true;
                Interlocked.Increment(ref _remainingCompletions);
            }

            return true;
        }

        public void Fail(Exception exception) => _context.ResultSource.TrySetException(exception);

        public void ObserveHttpForwarding(Task forwardingTask)
        {
            if (!_requiresHttpCompletion || Interlocked.Exchange(ref _httpCompletionObserved, 1) != 0)
            {
                return;
            }

            if (forwardingTask is null)
            {
                Complete();
                return;
            }

            _ = forwardingTask.ContinueWith(
                static (_, state) => ((InvocationPermit)state).Complete(),
                this,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        public void ObserveResult(Task resultTask)
        {
            _ = resultTask.ContinueWith(
                static (_, state) => ((InvocationPermit)state).Complete(),
                this,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        private void Complete()
        {
            if (Volatile.Read(ref _released) == 0 && Interlocked.Decrement(ref _remainingCompletions) == 0)
            {
                Dispose();
            }
        }
    }
}
