// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Functions.Host.HostState;
using Azure.Functions.Host.Models;
using Azure.Functions.Host.RuntimeDrain;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Script.WebHost.Security.Authorization.Policies;
using Microsoft.Extensions.Logging;

namespace Azure.Functions.Host.Controllers;

/// <summary>
/// Accepts Runtime drain operations for compute separation mode.
/// </summary>
public sealed partial class RuntimeDrainController : Controller
{
    private readonly ILogger<RuntimeDrainController> _logger;
    private readonly IAppVersionRuntimeDrainManager _manager;

    /// <summary>
    /// Initializes a new instance of the <see cref="RuntimeDrainController"/> class.
    /// </summary>
    /// <param name="manager">The Runtime drain coordinator.</param>
    /// <param name="logger">The logger.</param>
    public RuntimeDrainController(
        IAppVersionRuntimeDrainManager manager,
        ILogger<RuntimeDrainController> logger)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Commits one idempotent Runtime drain operation.
    /// </summary>
    /// <param name="request">The exact mode and reason.</param>
    /// <param name="cancellationToken">Cancels only before irreversible admission begins.</param>
    /// <returns>The immutable Host acceptance receipt.</returns>
    [HttpPost]
    [Route("admin/functions/drain")]
    [Authorize(Policy = PolicyNames.AdminAuthLevel)]
    public async Task<IActionResult> DrainRuntime(
        [FromBody] RuntimeDrainRequest? request,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<RequestValidationError> errors = ValidateRequest(
            request,
            out RuntimeDrainMode mode,
            out RuntimeDrainReason reason);
        if (errors.Count > 0)
        {
            return BadRequest(new RequestValidationResponse(errors));
        }

        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            RuntimeDrainReceipt receipt = await _manager.AcceptAsync(mode, reason);
            return Ok(receipt);
        }
        catch (RuntimeDrainConflictException exception)
        {
            Log.Conflict(_logger, mode, exception.Message);
            return StatusCode(
                StatusCodes.Status409Conflict,
                new RuntimeDrainErrorResponse(new(ErrorCodes.DrainConflict, exception.Message)));
        }
        catch (RuntimeDrainAdmissionException exception)
        {
            Log.AdmissionFailed(_logger, exception, mode);
            return StatusCode(
                StatusCodes.Status503ServiceUnavailable,
                new RuntimeDrainErrorResponse(new(
                    ErrorCodes.DrainAdmissionFailed,
                    "The Runtime drain admission could not be committed.")));
        }
    }

    private IReadOnlyList<RequestValidationError> ValidateRequest(
        RuntimeDrainRequest? request,
        out RuntimeDrainMode mode,
        out RuntimeDrainReason reason)
    {
        mode = default;
        reason = default;
        List<RequestValidationError> errors = [];
        if (!ModelState.IsValid ||
            request is null)
        {
            errors.Add(new(ErrorCodes.InvalidBody, "request"));
            return errors;
        }

        if (string.IsNullOrWhiteSpace(request.Mode))
        {
            errors.Add(new(ErrorCodes.Required, "mode"));
        }
        else if (!TryParseMode(request.Mode, out mode))
        {
            errors.Add(new(ErrorCodes.InvalidValue, "mode"));
        }

        if (string.IsNullOrWhiteSpace(request.Reason))
        {
            errors.Add(new(ErrorCodes.Required, "reason"));
        }
        else if (!TryParseReason(request.Reason, out reason))
        {
            errors.Add(new(ErrorCodes.InvalidValue, "reason"));
        }

        if (errors.Count == 0 &&
            reason is RuntimeDrainReason.TriggerDiscoveryComplete &&
            mode is not RuntimeDrainMode.Immediate)
        {
            errors.Add(new(ErrorCodes.InvalidValue, "mode"));
        }

        return errors;
    }

    private static bool TryParseMode(string value, out RuntimeDrainMode mode)
    {
        if (string.Equals(value, nameof(RuntimeDrainMode.Phased), StringComparison.Ordinal))
        {
            mode = RuntimeDrainMode.Phased;
            return true;
        }

        if (string.Equals(value, nameof(RuntimeDrainMode.Immediate), StringComparison.Ordinal))
        {
            mode = RuntimeDrainMode.Immediate;
            return true;
        }

        mode = default;
        return false;
    }

    private static bool TryParseReason(string value, out RuntimeDrainReason reason)
    {
        if (string.Equals(value, nameof(RuntimeDrainReason.ConfigurationChange), StringComparison.Ordinal))
        {
            reason = RuntimeDrainReason.ConfigurationChange;
            return true;
        }

        if (string.Equals(value, nameof(RuntimeDrainReason.Stop), StringComparison.Ordinal))
        {
            reason = RuntimeDrainReason.Stop;
            return true;
        }

        if (string.Equals(value, nameof(RuntimeDrainReason.Delete), StringComparison.Ordinal))
        {
            reason = RuntimeDrainReason.Delete;
            return true;
        }

        if (string.Equals(value, nameof(RuntimeDrainReason.Restart), StringComparison.Ordinal))
        {
            reason = RuntimeDrainReason.Restart;
            return true;
        }

        if (string.Equals(value, nameof(RuntimeDrainReason.TriggerDiscoveryComplete), StringComparison.Ordinal))
        {
            reason = RuntimeDrainReason.TriggerDiscoveryComplete;
            return true;
        }

        reason = default;
        return false;
    }

    private static class ErrorCodes
    {
        public const string DrainAdmissionFailed = nameof(DrainAdmissionFailed);
        public const string DrainConflict = nameof(DrainConflict);
        public const string InvalidBody = nameof(InvalidBody);
        public const string InvalidValue = nameof(InvalidValue);
        public const string Required = nameof(Required);
    }

    private static partial class Log
    {
        // EventId range is 720-729
        [LoggerMessage(720, LogLevel.Warning,
            "Rejected conflicting Runtime drain request in {Mode} mode: {Detail}.")]
        public static partial void Conflict(
            ILogger logger,
            RuntimeDrainMode mode,
            string detail);

        [LoggerMessage(721, LogLevel.Error,
            "Failed to commit Runtime drain request in {Mode} mode.")]
        public static partial void AdmissionFailed(
            ILogger logger,
            Exception exception,
            RuntimeDrainMode mode);
    }
}
