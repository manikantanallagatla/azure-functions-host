// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.HostState;
using Azure.Functions.Host.Models;
using Azure.Functions.Host.WorkerScaleIn;
using Azure.Functions.Rpc.Client;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.WebHost.Security.Authorization.Policies;
using Microsoft.Extensions.Logging;

namespace Azure.Functions.Host.Controllers;

/// <summary>
/// Provides authoritative count-only Worker scale-in for compute separation mode.
/// </summary>
public sealed partial class WorkerScaleInController : Controller
{
    private readonly ILogger<WorkerScaleInController> _logger;
    private readonly IWorkerCountScaleInManager _scaleInManager;
    private readonly IWorkerScaleInStatePublisher _statePublisher;

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkerScaleInController"/> class.
    /// </summary>
    /// <param name="logger">The logger.</param>
    /// <param name="scaleInManager">The manager that owns Worker selection and drain commitment.</param>
    /// <param name="statePublisher">The ordinary Host-state publisher.</param>
    public WorkerScaleInController(
        ILogger<WorkerScaleInController> logger,
        IWorkerCountScaleInManager scaleInManager,
        IWorkerScaleInStatePublisher statePublisher)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _scaleInManager = scaleInManager ?? throw new ArgumentNullException(nameof(scaleInManager));
        _statePublisher = statePublisher ?? throw new ArgumentNullException(nameof(statePublisher));
    }

    /// <summary>
    /// Commits the excess matching Workers to the existing drain and unlink lifecycle.
    /// </summary>
    /// <param name="request">The exact function group, capacity class, and absolute retained count.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The number of Workers newly committed to drain by this request.</returns>
    [HttpPost]
    [Route("admin/workers/scale-in")]
    [Authorize(Policy = PolicyNames.AdminAuthLevel)]
    public async Task<IActionResult> ScaleInWorkers(
        [FromBody] WorkerScaleInRequest? request,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<RequestValidationError> errors = ValidateRequest(request);
        if (errors.Count > 0)
        {
            return BadRequest(new RequestValidationResponse(errors));
        }

        cancellationToken.ThrowIfCancellationRequested();
        string functionGroup = request!.FunctionGroup!;
        bool isAlwaysReady = request.IsAlwaysReady!.Value;
        int desiredWorkerCount = request.DesiredWorkerCount!.Value;
        WorkerCountScaleInResult result = _scaleInManager.ScaleInWorkers(
            functionGroup,
            isAlwaysReady,
            desiredWorkerCount);

        if (result.NewlyDrainingWorkerCount > 0 &&
            string.Equals(functionGroup, FunctionGroups.Http, StringComparison.OrdinalIgnoreCase))
        {
            await PublishHttpStateBestEffortAsync(result.ResultingState);
        }

        Log.ScaleInCommitted(
            _logger,
            functionGroup,
            isAlwaysReady,
            desiredWorkerCount,
            result.NewlyDrainingWorkerCount);

        return Ok(new WorkerScaleInResponse(result.NewlyDrainingWorkerCount));
    }

    private IReadOnlyList<RequestValidationError> ValidateRequest(WorkerScaleInRequest? request)
    {
        List<RequestValidationError> errors = [];
        if (!ModelState.IsValid || request is null)
        {
            errors.Add(new(ErrorCodes.InvalidBody, "request"));
            return errors;
        }

        if (string.IsNullOrWhiteSpace(request.FunctionGroup))
        {
            errors.Add(new(ErrorCodes.Required, "functionGroup"));
        }

        if (request.IsAlwaysReady is null)
        {
            errors.Add(new(ErrorCodes.Required, "isAlwaysReady"));
        }

        if (request.DesiredWorkerCount is null)
        {
            errors.Add(new(ErrorCodes.Required, "desiredWorkerCount"));
        }
        else if (request.DesiredWorkerCount < 0)
        {
            errors.Add(new(ErrorCodes.InvalidValue, "desiredWorkerCount"));
        }

        return errors;
    }

    private async Task PublishHttpStateBestEffortAsync(HttpWorkerScaleInState state)
    {
        try
        {
            if (!await _statePublisher.PublishAuthoritativeScaleInAsync(state, CancellationToken.None))
            {
                Log.HttpStatePublicationUnavailable(_logger);
            }
        }
        catch (Exception exception)
        {
            Log.HttpStatePublicationFailed(_logger, exception);
        }
    }

    private static class ErrorCodes
    {
        public const string InvalidBody = nameof(InvalidBody);
        public const string InvalidValue = nameof(InvalidValue);
        public const string Required = nameof(Required);
    }

    private static partial class Log
    {
        // EventId range is 710-719

        [LoggerMessage(710, LogLevel.Information,
            "Authoritative Worker scale-in for function group {FunctionGroup}, always-ready class {IsAlwaysReady}, and desired count {DesiredWorkerCount} newly committed {NewlyDrainingWorkerCount} Workers.")]
        public static partial void ScaleInCommitted(
            ILogger logger,
            string functionGroup,
            bool isAlwaysReady,
            int desiredWorkerCount,
            int newlyDrainingWorkerCount);

        [LoggerMessage(711, LogLevel.Warning,
            "Ordinary Host-state publication after authoritative HTTP Worker scale-in was unavailable.")]
        public static partial void HttpStatePublicationUnavailable(ILogger logger);

        [LoggerMessage(712, LogLevel.Error,
            "Ordinary Host-state publication after authoritative HTTP Worker scale-in failed.")]
        public static partial void HttpStatePublicationFailed(ILogger logger, Exception exception);
    }
}
