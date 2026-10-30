# BYOC P1 count-only Worker scale-in reviewer guide

> **Scope:** Host/WorkerProxy companion for Legion's P1 count-only Worker scale-in decision.
> **Public API:** `POST /admin/workers/scale-in`.
> **Stack:** Built directly on the existing R2 HTTP drain path, exact Worker removal fallback, and R4 Runtime retirement path.

## 1. Context

Legion already owns app-wide Worker-count distribution. It sends one local Runtime an absolute
retained count for one exact function group and always-ready class. The Host must select the excess
local Workers, close their new invocation admission before acknowledging them, and reuse the
existing drain, unlink, WorkerProxy removal, and fallback-removal lifecycle.

Before P1, the Host could:

- autonomously reserve and drain one idle HTTP on-demand Worker after AppServer applied the R2
  capacity fence;
- drain all attached Workers during R4 Runtime retirement;
- preserve exact Worker identity for uncertain-unlink fallback removal.

It could not accept an authoritative count reduction from Legion or preserve the newest Durable
Worker by its original WorkerProxy startup time.

## 2. Non-goals

- No app-wide Worker-count distribution; Legion owns that calculation.
- No standing desired-count target, reconciliation loop, or retry policy in Host.
- No new Worker lifecycle path, per-Worker drain command, forced cancellation, or drain deadline.
- No Docker, nginx, Go AppServer scheduling, or physical pod-deletion changes.
- No reuse of the R2 `x-ms-functions-host-state-capacity-fence: required` decision for P1.
- No startup-time inference from Link time, channel construction, signal revision, or Host
  observation time.

## 3. Before and after

### Before

```mermaid
flowchart LR
    Legion[Legion] -. app-wide desired count only .-> Host[Runtime / Host]
    Host -->|R2 idle HTTP only| Registry[Worker registry]
    Registry --> Proxy[WorkerProxy]
    Proxy --> Worker[Customer Worker]
```

The Host had no public count-only API. R2 chose at most one idle HTTP on-demand Worker and required
AppServer to approve the projected capacity reduction before drain commitment.

### After

```mermaid
flowchart LR
    Legion[Legion app-wide distribution] -->|functionGroup + isAlwaysReady + desiredWorkerCount| API[Host P1 API]
    API --> Registry[Worker registry]
    Registry -->|atomic selection and intake closure| Channels[Matching ready Worker channels]
    Channels -->|existing committed-work drain| Proxy[WorkerProxy]
    Proxy -->|RemovalRequested| Legion
    Registry -->|uncertain unlink with exact identity| Fallback[Existing fallback-removal outbox]
    API -->|HTTP group: ordinary Host capacity snapshot| AppServer
    AppServer -->|retain allocation obligations; clamp free capacity| AppServer
```

## 4. Public contract

The route remains compute-only because it is discovered through the same
`ClientWorkerComposition` application part as Worker Link. It uses the existing
`AdminAuthLevel` authorization policy.

### Request

```http
POST /admin/workers/scale-in
Content-Type: application/json

{
  "functionGroup": "durable",
  "isAlwaysReady": false,
  "desiredWorkerCount": 1
}
```

The body has exactly three contract fields:

| Field | Meaning |
| --- | --- |
| `functionGroup` | Exact function-group value assigned by WorkerProxy. Candidate matching is ordinal; the special HTTP and Durable policy classes are recognized case-insensitively to match existing platform group semantics. |
| `isAlwaysReady` | Exact Worker capacity class assigned by WorkerProxy. |
| `desiredWorkerCount` | Absolute number of matching usable local Workers to retain for this one decision. |

`desiredWorkerCount` must be zero or greater. Missing fields, malformed bodies, and negative counts
return the existing validation-errors envelope with HTTP `400`.

### Success response

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "newlyDrainingWorkerCount": 2
}
```

`newlyDrainingWorkerCount` counts only Workers whose irreversible drain was newly committed by this
request. Already-draining, removed, or otherwise ineligible Workers are not counted again.

## 5. End-to-end flow

```mermaid
sequenceDiagram
    participant Legion
    participant API as Host scale-in API
    participant Registry as Worker registry
    participant Channel as Selected Worker channels
    participant AppServer
    participant Proxy as WorkerProxy

    Legion->>API: POST exact group, class, desired count
    API->>Registry: ScaleInWorkers(...)
    Registry->>Registry: Snapshot usable matching Workers
    Registry->>Registry: excess = max(0, usable - desired)
    Registry->>Registry: Apply Durable newest-retention rule
    Registry->>Channel: Prefer idle, then atomically mark draining
    Note over Registry,Channel: New invocation intake is closed before the count is returned
    Registry-->>API: newly committed count + resulting Host state

    alt HTTP function group and at least one new drain committed
        API->>AppServer: Ordinary Host-state capacity snapshot
        Note over API,AppServer: No R2 "required" capacity-fence header
        AppServer->>AppServer: Keep existing allocation obligations and clamp free capacity
    end

    API-->>Legion: 200 newlyDrainingWorkerCount
    Channel->>Channel: Wait for already committed work
    Channel->>Proxy: Existing graceful unlink
    Proxy->>Legion: Existing RemovalRequested
```

The API acknowledges selection commitment, not physical pod deletion. Drain and deletion continue
through the existing asynchronous lifecycle after the response.

## 6. Eligibility and selection

A Worker contributes to `usableCount` only when all of these are true at the decision:

- it is linked and has an initialized channel;
- function setup completed enough for invocation admission;
- its exact WorkerProxy-assigned function group matches with ordinal comparison;
- its exact WorkerProxy-assigned always-ready class matches;
- it is not reserved by R2, draining, terminal, Runtime-admission-closed, or removal-requested;
- its channel is not terminated.

Busy Workers remain usable because already committed invocations are allowed to finish. Idle is a
selection preference, not an eligibility requirement.

```mermaid
flowchart TD
    A[POST count target] --> B[Collect matching linked channels]
    B --> C{Execution-ready and non-draining?}
    C -- no --> X[Exclude]
    C -- yes --> D[usableCount]
    D --> E[excess = max(0, usableCount - desired)]
    E --> F{excess is zero?}
    F -- yes --> Z[Return newlyDrainingWorkerCount 0]
    F -- no --> G{Durable and desired > 0?}
    G -- no --> I[Order idle first]
    G -- yes --> H{Every candidate has authoritative startup time and one unique newest?}
    H -- no --> Z
    H -- yes --> J[Exclude unique newest from selection]
    J --> I
    I --> K[Commit up to excess under registry and channel locks]
    K --> L[Close new intake and mark draining]
    L --> M[Return newly committed count]
```

The absolute target is evaluated once. A Worker that links or becomes ready after the decision does
not trigger another Host-local action. Legion must send a later request if another app-wide decision
is needed. The retained count is not a floor: autonomous AppServer-fenced R2 scale-in remains
independently active after P1 and can later reduce eligible idle HTTP on-demand Workers.

## 7. Durable newest-Worker retention

For a positive Durable target, the Host preserves the one eligible local Worker with the newest
authoritative original WorkerProxy startup time.

The Link request adds `workerProxyStartupTime` beside the existing exact `worker` identity:

```json
{
  "workerGrpcEndpoint": "http://worker-proxy:50053",
  "worker": {
    "resourceNamespace": "namespace",
    "podName": "worker-pod",
    "podGuid": "47bf3a08-900a-48b3-a0f1-f6baa87191e7",
    "legionStampName": "legion-stamp",
    "podType": "FunctionsSplitWorkerPod",
    "app": {
      "functionAppName": "app-name",
      "functionAppSiteId": 123,
      "antaresStampName": "antares-stamp"
    }
  },
  "workerProxyStartupTime": "2026-10-30T01:02:03.4567890+00:00"
}
```

The coordinated Legion Link-identity change supplies the exact Kubernetes
`V1ContainerStatus.State.Running.StartedAt` value for the `functionsworkerproxy` container. SCP
captures it during Pod Ready polling and carries it unchanged through the full Legion Link path to
the site-token-authenticated `PUT /admin/workers/{WorkerPodName}` request as the optional RFC 3339
`workerProxyStartupTime` field, paired with the exact `worker` identity. It must not substitute:

- Worker Link receipt time;
- channel creation or connection time;
- Worker signal revision or observation time;
- a timestamp derived locally by Host.

Older Link callers remain valid. An exact identity-bearing retry may enrich an existing Link that
has no startup time; a conflicting supplied startup time is a Link conflict. The timestamp is not
added to `FunctionsSplitPodIdentity`, so exact deletion identity and ordering metadata remain
separate.

For a positive Durable request, missing startup metadata or a tie for newest fails closed and
selects nothing because newest retention cannot be proven. A zero Durable target remains
authoritative and drains all eligible Workers without requiring startup metadata.

## 8. Atomicity and lifecycle reuse

The registry owns the decision under its existing state lock. Each selected channel revalidates the
exact group, class, readiness, and lifecycle state under the same channel lock used by invocation
admission. It then marks the channel draining before the public response can be created.

```mermaid
stateDiagram-v2
    [*] --> Ready
    Ready --> Busy: Invocation commits first
    Busy --> Draining: P1 selects busy Worker
    Ready --> Draining: P1 selects idle Worker
    Draining --> Unlinking: Committed work reaches zero
    Unlinking --> Terminal: Graceful unlink completes
    Unlinking --> FallbackOutstanding: Unlink outcome uncertain and exact identity exists
```

If invocation admission wins the channel lock first, that invocation remains committed and P1
drains after it finishes. If P1 wins first, later dispatch cannot enter the selected channel.

P1 calls the same `CompleteDrainAsync` and `UnlinkAsync` path as R2 and R4. It does not duplicate
WorkerProxy state transitions, fallback-removal records, or disposal logic.

## 9. HTTP Host-state publication

For the HTTP function group, the API publishes the resulting ordinary Host-state snapshot after at
least one selection commitment:

```mermaid
flowchart LR
    Commit[P1 commits selected HTTP drains] --> State[Registry computes resulting httpCapacity]
    State --> Publish[PUT ordinary Host state]
    Publish --> NoFence[No x-ms-functions-host-state-capacity-fence header]
    NoFence --> Obligations[AppServer retains existing allocation obligations]
    Obligations --> Free[Free capacity clamps to zero when obligations consume the reduced capacity]
```

P1 is an authoritative target reduction, not a reversible autonomous R2 proposal. Therefore the
publication intentionally omits the R2 `required` capacity fence and treats an ordinary successful
AppServer response as applied.

Selection is already irreversible when publication begins. A publication rejection, transport
failure, or shared-publication-gate timeout is logged and does not change the acknowledged
newly-draining count or reopen intake. Returning an error after commitment would hide a real drain
from Legion and invite incorrect re-accounting. The complete publication attempt, including gate
acquisition, is bounded to five seconds independently of request cancellation. No-op and repeated
requests do not enqueue another publication. The existing Host-state publisher continues to expose
current state on later passes.

## 10. Ownership boundaries

| Component | Owns in P1 | Does not own |
| --- | --- | --- |
| Legion | App-wide count distribution, exact three-field request, aggregation of acknowledged newly-draining counts, and physical deletion. | Host-local Worker choice or invocation admission. |
| Host API | Authentication, request validation, one local count decision, response count, and HTTP ordinary-state publication. | Standing convergence or app-wide retries. |
| Worker registry/channel | Exact eligibility, idle preference, Durable newest retention, atomic intake closure, drain, unlink, and fallback-removal reuse. | App-wide count allocation or physical pod deletion. |
| WorkerProxy | Authoritative function-group/class capabilities, original startup-time source, and existing sticky removal state. | Host-local selection. |
| AppServer | Ordinary Host-state consumption, existing allocation obligations, and free-capacity calculation. | P1 Worker selection or approval fencing. |

## 11. Mixed-version behavior

| Combination | Behavior |
| --- | --- |
| Older Link caller with no exact identity or startup time | Link remains valid. HTTP P1 and zero-target Durable P1 can proceed. Positive Durable P1 fails closed. |
| Exact identity but no startup time | Existing R4 fallback-removal behavior remains available. Positive Durable P1 fails closed. |
| Exact retry adds the same original startup time | Existing Link is enriched without replacing identity. |
| Retry supplies a conflicting startup time | Link returns the existing conflict path. |
| Older AppServer | Receives an ordinary Host-state request with no new JSON fields and no P1-specific fence requirement. |
| Repeated P1 request | Already-draining or deleted Workers are absent from usable count and are not counted again. |
| Mixed-case `HTTP` or `Durable` assignment | Exact matching still requires the request to use the same value; special HTTP publication and Durable retention policy are recognized case-insensitively. |

## 12. Assumptions

- Legion sends a local desired count after it has performed app-wide distribution.
- WorkerProxy capability replacement remains the trusted source of function group and always-ready
  class.
- `workerProxyStartupTime` is the original WorkerProxy process startup time paired with the exact Worker
  identity.
- A unique newest timestamp is required to prove positive Durable retention; ambiguity fails closed.
- A successful response acknowledges irreversible Host-local drain commitment, not drain completion
  or deletion.
- AppServer preserves existing allocation obligations when ordinary HTTP capacity decreases.
- Ignoring request cancellation after drain commitment for the independently bounded five-second
  AppServer publication avoids reporting an ambiguous partial result.
- P1 does not disable autonomous R2; the one-shot retained count is not a lasting minimum.

## 13. Known limitations

- There is no standing target or Host retry loop; later changes require a later Legion request.
- Positive Durable scale-in cannot proceed when any eligible local candidate lacks authoritative
  startup time or when the newest timestamp is tied.
- The API does not wait for committed invocations, graceful unlink, fallback acknowledgement, or
  physical deletion before returning.
- Existing drain and unlink paths still have no forced invocation deadline.
- Ordinary HTTP state publication is best effort and bounded to five seconds after irreversible
  commitment.
- No-op requests intentionally skip Host-state publication because capacity did not change.
- This PR does not validate deployment ordering or real private-stamp behavior.

## 14. File-by-file change map

| File | Review purpose |
| --- | --- |
| `Functions.Host/Controllers/WorkerScaleInController.cs` | Public authenticated POST contract, validation, response, and best-effort ordinary HTTP state publication. |
| `Functions.Host/Models/WorkerScaleInRequest.cs` | Exact three-field request and one-field response wire shapes. |
| `Functions.Host/Controllers/WorkerLinkController.cs` | Accepts optional authoritative startup time beside exact identity. |
| `Functions.Host/Models/WorkerLinkRequest.cs` | Adds optional `workerProxyStartupTime` without changing `FunctionsSplitPodIdentity`. |
| `Functions.Host/HostState/IWorkerScaleInStatePublisher.cs` | Narrow public controller dependency for ordinary Host-state publication. |
| `Functions.Host/HostState/AppServerHostStateClient.cs` | Adds no-fence ordinary publication while preserving R2 and R4 fence behavior. |
| `Functions.Rpc.Client/Registry/IWorkerChannelRegistry.cs` | Preserves Link metadata overloads without adding P1 policy to the root registry contract. |
| `Functions.Rpc.Client/ScaleIn/IWorkerCountScaleInManager.cs` | Narrow P1 selection contract and result shape. |
| `Functions.Rpc.Client/Registry/WorkerChannelRegistry.cs` | Computes excess once, preserves newest Durable Worker, commits selection, and reuses drain/unlink/fallback. |
| `Functions.Rpc.Client/WorkerChannel/RpcClientWorkerChannel.cs` | Revalidates exact eligibility and atomically closes invocation intake. |
| `Functions.Rpc.Client.Tests/Registry/WorkerChannelRegistryTests.CountScaleIn.cs` | Five public-API behavioral tests through TestServer and real registry/channel lifecycle. |

## 15. Behavioral validation

The five added public-API behavioral methods cover:

1. exact group/class isolation plus Starting/readiness-ineligible exclusion;
2. desired count at current, reduction, repeated request, and no-fence HTTP publication;
3. zero Durable target with older Link metadata;
4. positive Durable retention of the newest authoritative startup time while preferring an idle
   candidate over a busy candidate;
5. invalid request rejection and positive Durable fail-closed behavior when ordering metadata is
   missing or the newest timestamp is ambiguous.

Council findings incorporated before final validation:

- special HTTP and Durable policy classification is case-insensitive while candidate matching
  remains exact;
- the complete post-commit publication attempt is bounded, no-op requests skip publication, and
  publication failure still returns the committed count;
- the controller depends on a narrow `IWorkerCountScaleInManager` rather than expanding the root
  registry interface;
- fail-closed Durable metadata and Runtime-retirement outcomes emit reason-specific structured logs;
- P1's one-shot retained count is explicitly not a floor over later autonomous R2 decisions.

Final local evidence:

- `dotnet build src\Functions.Host\Functions.Host.csproj -c Debug --no-restore --nologo`
  - Result: passed with zero warnings.
- `dotnet build test\Functions.Rpc.Client.Tests\Functions.Rpc.Client.Tests.csproj -c Debug --no-restore --nologo`
  - Result: passed with zero warnings.
- `dotnet build test\Functions.Host.Tests\Functions.Host.Tests.csproj -c Debug --no-restore --nologo`
  - Result: passed with zero warnings.
- `dotnet test test\Functions.Rpc.Client.Tests\Functions.Rpc.Client.Tests.csproj -c Debug --no-build --no-restore --filter "FullyQualifiedName~WorkerChannelRegistryTests.PostScaleIn" --nologo`
  - Result: passed, 5/5.
- `dotnet test test\Functions.Rpc.Client.Tests\Functions.Rpc.Client.Tests.csproj -c Debug --no-build --no-restore --filter "FullyQualifiedName~WorkerChannelRegistryTests|FullyQualifiedName~RuntimeRetirementManagerTests" --nologo`
  - Result: passed, 119/119.
- `dotnet test test\Functions.Host.Tests\Functions.Host.Tests.csproj -c Debug --no-build --no-restore --filter "FullyQualifiedName~WorkerLink|FullyQualifiedName~ClientWorkerCompositionTests" --nologo`
  - Result: passed, 159/159.
- `git diff --check`
  - Result: passed.
- Coordinated Legion Link validation
  - Result: passed, 21 focused Link tests and 33 growth/replay cases.

These are local in-memory public-API behavioral tests. They do not replace a private-stamp
Host/AppServer/WorkerProxy/Legion validation.
