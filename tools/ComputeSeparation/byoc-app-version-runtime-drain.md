# BYOC Runtime drain reviewer guide

> **Scope:** Functions Host and WorkerProxy-side lifecycle reuse for P2 Phased reconciliation,
> P3 Immediate stop/delete/restart handling, and Immediate trigger-discovery cleanup.
> **Public API:** authenticated `POST /admin/functions/drain`.
> **Stack:** built directly on the existing R2 drain path, exact worker-removal fallback, R4 Runtime
> retirement fence, and P1 count-only Worker scale-in.

## 1. Context and non-goals

Legion owns app-version detection, trigger-discovery completion, durable operation state,
Runtime-set capture, Recreate versus RollingUpdate policy, and discovery behavior. AppServer relays
one authenticated request to Host. Host owns only the local Runtime admission fence, Worker
scheduling, invocation grace, graceful unlink, fallback handoff, and final Runtime
`RemovalRequested`.

This change does not add:

- Legion reconciliation or durable operation state;
- AppServer retry or a Go-to-Host callback beyond the coordinated relay;
- a second Worker removal path;
- forced cancellation before a Worker's committed-invocation grace deadline;
- generalized operation history or Immediate-to-Phased downgrade;
- Host-process schedule recovery.

## 2. Shared contract

The request requires two known fields. Additive unknown fields are accepted for forward
compatibility. Missing known fields, wrong JSON types, case-variant known field names, duplicate
properties, malformed JSON, and invalid values return HTTP `400`.

```http
POST /admin/functions/drain
Content-Type: application/json

{
  "mode": "Phased",
  "reason": "ConfigurationChange"
}
```

| Field | Accepted values |
| --- | --- |
| `mode` | `Phased` or `Immediate`. |
| `reason` | `ConfigurationChange`, `Stop`, `Delete`, `Restart`, or `TriggerDiscoveryComplete`. |

`TriggerDiscoveryComplete` is valid only with `Immediate`; pairing it with `Phased` returns HTTP
`400`.

The success response is HTTP `200` and contains exactly two fields:

```json
{
  "mode": "Phased",
  "acceptedAtUtc": "2026-10-30T00:00:00+00:00"
}
```

`acceptedAtUtc` is the Host-authoritative RFC 3339 acceptance time for the active mode. Scheduling
details, reason, batch size, deadlines, Worker IDs, and Runtime incarnation remain private Host
state.

| Result | HTTP behavior |
| --- | --- |
| First valid mode | `200` only after local Link/invocation admission is closed and AppServer acknowledges `lifecycle-required`. |
| Request for the active mode with any valid reason | Existing immutable receipt; snapshots, timers, selections, and deadlines are not reset. |
| `Phased` while `Immediate` is active | `409`; the Runtime cannot downgrade or restart. |
| `Immediate` while `Phased` is active | One allowed escalation with a new Immediate receipt. |
| Invalid mode, reason, required field, body, or duplicate property | `400`. |
| Admission or background lifecycle failure | Non-success response with no receipt, including repeated active-mode requests. |

The coordinated Go relay at commit `7350d9e1e6273810569d1320df140576c3098804`
forwards the request once, preserves Host non-success status/body/content type, and treats an invalid
or unexpected Host `200` as `502`. Host therefore owns all state-based idempotency, conflict, and
escalation behavior.

## 3. Shared admission and retirement sequence

```mermaid
sequenceDiagram
    participant Legion
    participant AppServer
    participant API as Host drain API
    participant Coordinator as Host drain coordinator
    participant Registry as Worker registry
    participant Workers

    Legion->>AppServer: Runtime drain operation
    AppServer->>API: POST mode, reason
    API->>Coordinator: Accept requested mode
    Coordinator->>Registry: Close Link and invocation admission
    Registry-->>Coordinator: Runtime incarnation + one-time Worker snapshot
    Coordinator->>AppServer: Bind runtimeIncarnationId
    AppServer-->>Coordinator: capacity fence applied
    Coordinator->>AppServer: Draining, acquireClosed=true, httpCapacity=0
    AppServer-->>Coordinator: lifecycle-required applied
    Note over Coordinator: acceptedAtUtc is committed here
    Coordinator->>Registry: Select Workers according to active mode
    Coordinator-->>API: Immutable receipt
    API-->>AppServer: HTTP 200 receipt
    AppServer-->>Legion: Active-mode receipt
    Registry->>Workers: Existing drain, graceful unlink, fallback if uncertain
    Coordinator->>AppServer: Draining progress and fallback actions
    Coordinator->>AppServer: RemovalRequested only after links and fallbacks clear
```

Closing admission and taking the initial snapshot occur under the registry state lock. Each channel
classifies its pre-closure invocation readiness under the same channel lock used by invocation
admission, then permanently closes Runtime admission. A concurrent P1 or R2 drain either commits
first and is tracked as pre-existing work, or observes Runtime retirement and cannot create a later
selection.

## 4. P2 Phased scheduling

The initial non-HTTP snapshot contains linked, invocation-ready Workers that were not already
draining. It includes on-demand and always-ready classes. HTTP Workers are kept in a separate
snapshot. Worker IDs are sorted ordinally for deterministic selection. Linked Workers that are not
invocation-ready are excluded from `N` and both timed snapshots, but receive immediate drain
ownership after the lifecycle fence. Because Runtime admission is already closed, they cannot gain
new committed invocations and cannot strand final Runtime removal.

For `N` initial non-HTTP Workers:

```text
batchSize = max(1, ceil(N * 20%))
```

```mermaid
gantt
    title Phased Worker selection from Host acceptance
    dateFormat X
    axisFormat %Ss
    section Non-HTTP
    Batch 1 :milestone, 0, 0
    Batch 2 :milestone, 10, 0
    Batch 3 :milestone, 20, 0
    Batch 4 :milestone, 30, 0
    Batch 5 :milestone, 40, 0
    section HTTP
    All initial HTTP Workers :milestone, 35, 0
```

Each selected Worker receives an immutable 60-minute deadline from its own actual selection time.
The deadline bounds committed-invocation grace, not AppServer acknowledgement latency. Workers
already draining through R2 or P1 retain their existing drain and remain part of Runtime completion;
P2 does not replace or reset that work.

A repeated Phased request with any valid reason returns the original receipt and does not recreate
the snapshot, restart the 0/10/20/30/35/40-second schedule, or move any Worker deadline.

## 5. Immediate cleanup and escalation

A first `Immediate` request uses the same admission closure and AppServer lifecycle fence. After
fence acknowledgement, Host selects every remaining linked, invocation-ready Worker immediately
across HTTP, non-HTTP, on-demand, and always-ready classes. Each new selection receives a
60-minute deadline from Immediate acceptance. Any linked Worker that was not invocation-ready at
admission closure is also terminally owned immediately without changing the ready-Worker selection
set or deadline policy.

`Stop`, `Delete`, `Restart`, and `TriggerDiscoveryComplete` have identical Immediate Host scheduling
semantics. Trigger discovery must use `Immediate`, including when the discovery Runtime has no
Worker. The existing manager publishes `Draining`, drains and unlinks any discovery Worker, waits
for an uncertain unlink fallback acknowledgement when necessary, and publishes
`RemovalRequested` only after all links and fallback actions clear. The reason remains structured
diagnostic context for the first accepted mode or escalation; Legion owns its external meaning.

```mermaid
stateDiagram-v2
    [*] --> Phased: first Phased accepted
    [*] --> Immediate: first Immediate accepted
    Phased --> Immediate: first Immediate request
    Phased --> Phased: repeated Phased request
    Immediate --> Immediate: repeated Immediate request
    Immediate --> Conflict: Phased downgrade
```

During the one allowed Phased-to-Immediate escalation:

1. Host cancels or neutralizes future Phased timer selections.
2. Already-selected Workers keep their original P2 deadlines.
3. Every remaining eligible Worker is selected once with an escalation-time deadline.
4. Host returns and retains the new Immediate receipt.
5. Repeated Immediate requests return that receipt regardless of valid reason.
6. Phased cannot resume; a later Phased request returns `409`.

The coordinator retains only the active-mode acceptance tasks needed for state idempotency. It is
not a generalized operation-history or workflow framework.

## 6. Deadline, unlink, and fallback behavior

```mermaid
stateDiagram-v2
    [*] --> Selected
    Selected --> GracefulUnlink: committed work completes before deadline
    Selected --> DeadlineClose: 60-minute grace expires
    DeadlineClose --> GracefulUnlink: close channel through existing unlink path
    GracefulUnlink --> PrimaryRemoval: WorkerProxy termination confirms handoff
    GracefulUnlink --> FallbackOutstanding: termination outcome is uncertain and exact identity exists
    FallbackOutstanding --> FallbackCleared: Legion acknowledges exact action
    PrimaryRemoval --> RuntimeProgress
    FallbackCleared --> RuntimeProgress
```

At deadline, Host stops waiting for committed invocation completion and calls the existing unlink
path. That path fails remaining local invocation permits, completes the FunctionRpc request stream,
waits for WorkerProxy termination, disposes the channel, and records the existing exact
`RuntimeUnlink` fallback action when primary handoff is uncertain. No second removal command or
identity scheme is introduced.

Pre-existing R2/P1 drain tasks and newly selected drain tasks must all complete successfully. Final
Runtime `RemovalRequested` additionally requires:

- no linked Worker channel remains;
- no exact fallback action remains outstanding;
- the current selection schedule is final for the active mode.

A timer alone cannot bypass fallback ownership.

## 7. Ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Legion | Action detection, trigger-discovery completion, Runtime-set capture, durable operation state, Recreate/RollingUpdate policy, discovery, and physical deletion. | Host-local Worker scheduling or invocation admission. |
| Go AppServer relay | Authenticated one-shot relay, Host response preservation, and invalid-`200` rejection. | State idempotency, conflict, escalation, or Worker selection. |
| Host controller/coordinator | Strict request validation, active-mode idempotency, one optional escalation, receipt, fence ordering, schedule, progress, and final Runtime lifecycle. | Durable recovery or external app-version policy. |
| Worker registry/channel | Atomic admission closure, deterministic eligibility, existing-drain tracking, invocation grace, unlink, and fallback creation. | AppServer lifecycle acknowledgement or physical deletion. |
| WorkerProxy | Existing sticky primary Worker `RemovalRequested`. | P2/P3 scheduling or Runtime finalization. |

## 8. Compatibility and failure direction

- R2 and P1 stop creating new selections after Runtime retirement closes admission.
- A committed R2/P1 drain remains tracked and can still produce the existing exact fallback action.
- R4 duplicate activation-fault retirement remains idempotent for R4-owned retirement. R4 cannot
  replace an active app-version schedule with an uncoordinated second retirement flow.
- Link requires the worker namespace and route-matching pod name so an uncertain unlink always has
  durable fallback ownership.
- AppServer lifecycle publication remains serialized through the existing Host-state client gate.
- Any bind, lifecycle fence, progress, or final-removal publication failure leaves the Runtime
  locally closed and does not create a success-shaped response or completion.
- A failed schedule or lifecycle task is latched. Repeated active-mode requests and later escalation
  fail instead of returning a receipt for a schedule that no longer has a finalization owner.

## 9. Assumptions and known limitations

- AppServer uses the coordinated one-shot relay with the same two-field request and two-field
  receipt contract.
- Bala's Legion change owns external reason semantics, trigger-discovery completion, Runtime-set
  reconciliation, and any durable retry policy.
- WorkerProxy capability finalization remains the trusted source of function group and always-ready
  class.
- Host-process loss during an active Phased or Immediate drain is deferred. The AppServer lifecycle
  fence remains sticky and must not reopen acquisition, but Host does not restore timers, receipts,
  snapshots, or in-memory fallback state after process replacement.
- This local validation does not replace coordinated private-stamp validation with the Go relay and
  Legion reconciliation enabled.

## 10. File map

| File | Review purpose |
| --- | --- |
| `Functions.Host/Controllers/RuntimeDrainController.cs` | Authenticated API, validation, exact receipt, `400`/`409`/failure mapping. |
| `Functions.Host/Models/RuntimeDrainRequest.cs` | Required known request fields with additive-field compatibility, receipt, enum, and error wire shapes. |
| `Functions.Host/HostState/IAppVersionRuntimeDrainManager.cs` | Narrow public controller dependency. |
| `Functions.Host/HostState/AppVersionRuntimeDrainManager.cs` | One in-memory P2/P3 coordinator, active-mode idempotency, escalation, timers, progress, and final lifecycle. |
| `Functions.Host/HostState/AppServerHostStateServiceCollectionExtensions.cs` | Root coordinator registration. |
| `Functions.Rpc.Client/Retirement/IAppVersionRuntimeDrainWorkerManager.cs` | Narrow Host-to-registry admission and selection seam. |
| `Functions.Rpc.Client/Registry/WorkerChannelRegistry.cs` | Atomic snapshot/closure, deterministic selection, existing-drain tracking, deadline handoff, and fallback reuse. |
| `Functions.Rpc.Client/WorkerChannel/RpcClientWorkerChannel.cs` | Channel-local eligibility, admission closure, and app-version drain commitment. |
| `Functions.Rpc.Client/Extensions/RpcClientServiceCollectionExtensions.cs` | Registry seam registration. |
| `Functions.Rpc.Client.Tests/Registry/WorkerChannelRegistryTests.AppVersionDrain.cs` | Eight public-API behavioral tests with a deterministic test clock. |

## 11. Behavioral evidence

The eight focused in-memory public-API methods prove:

1. synchronous lifecycle fence commitment, exact two-field receipt, active-mode idempotency,
   additive-field compatibility, ordinal required property names, duplicate/type/malformed
   rejection, non-ready linked-Worker ownership, and closed late Link admission;
2. Immediate-only trigger-discovery cleanup for an empty Runtime with active-mode idempotency;
3. trigger-discovery completion waits for exact Worker fallback acknowledgement;
4. deterministic 20% non-HTTP batches at 0/10/20/30/40 seconds, HTTP selection at 35 seconds, and
   repeated Phased requests without schedule reset;
5. per-selection 60-minute deadline fallback plus completion ownership for a pre-existing P1 drain;
6. first Immediate selection across HTTP/non-HTTP and on-demand/always-ready classes, repeated
   Immediate requests across valid reasons, and downgrade conflict;
7. Phased-to-Immediate escalation with original P2 deadline preservation, escalation-time deadlines
   for new selections, repeated Immediate idempotency, and one-way mode progression.
8. lifecycle-progress publication failure is latched and prevents repeated-mode or escalation
   receipts.

Validation commands and final results are recorded in the branch handoff after all affected P1, R4,
Host, and RPC suites complete.
