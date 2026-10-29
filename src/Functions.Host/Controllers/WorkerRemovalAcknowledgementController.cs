// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Azure.Functions.Rpc.Client;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Script.WebHost.Security.Authorization.Policies;

namespace Azure.Functions.Host.Controllers;

/// <summary>
/// Accepts exact acknowledgements for Runtime fallback worker-removal actions.
/// </summary>
public sealed class WorkerRemovalAcknowledgementController : Controller
{
    private readonly IHttpWorkerScaleInManager _scaleInManager;

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkerRemovalAcknowledgementController"/> class.
    /// </summary>
    /// <param name="scaleInManager">The root-owned worker lifecycle state.</param>
    public WorkerRemovalAcknowledgementController(IHttpWorkerScaleInManager scaleInManager)
    {
        _scaleInManager = scaleInManager ?? throw new ArgumentNullException(nameof(scaleInManager));
    }

    /// <summary>
    /// Acknowledges Legion's accepted deletion handoff for one exact worker-removal action.
    /// </summary>
    /// <param name="acknowledgement">The exact action previously published by this Runtime process.</param>
    /// <returns>
    /// <c>204</c> after an exact action is atomically cleared or for a retained identical replay,
    /// <c>400</c> for a malformed action, or <c>404</c> when no exact action matches.
    /// </returns>
    [Authorize(Policy = PolicyNames.AdminAuthLevel)]
    [HttpPost]
    [Route("admin/functions/split/acknowledgeWorkerRemoval")]
    public IActionResult AcknowledgeWorkerRemoval(
        [FromBody] FunctionsSplitWorkerRemovalAcknowledgement? acknowledgement)
    {
        if (!ModelState.IsValid || acknowledgement?.Action?.IsValid() is not true)
        {
            return BadRequest();
        }

        WorkerRemovalAcknowledgementResult result =
            _scaleInManager.AcknowledgeWorkerRemoval(acknowledgement.Action);

        return result is WorkerRemovalAcknowledgementResult.Accepted or WorkerRemovalAcknowledgementResult.Duplicate
            ? NoContent()
            : NotFound();
    }
}
