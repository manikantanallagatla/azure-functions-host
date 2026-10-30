# BYOC Runtime worker-removal fallback

## Context

WorkerProxy sticky `RemovalRequested` remains the primary worker-removal handoff. Runtime uses
`outstandingWorkerRemovals` only when a graceful unlink started but WorkerProxy termination did not
finish reliably enough to prove that primary handoff.

The fallback action contains the immutable worker coordinates already supplied by Legion Link:

- resource namespace
- pod name
- a frozen bounded reason (`RuntimeUnlink`)

Runtime does not copy Function App identity into the action. Legion HNA combines the worker
coordinates with the authoritative owning app already present in `runtimeState.Self.App` when it
constructs the deletion request.

## Ownership

| Component | Responsibility |
| --- | --- |
| Legion provisioning and Link path | Send the existing worker namespace and pod name to Runtime Link. |
| Functions Host root registry | Validate and retain the required worker coordinates, own the authoritative fallback outbox, match acknowledgements, and retain bounded duplicate evidence. |
| Host-to-AppServer state transaction | Publish the current outbox with the existing capacity snapshot. |
| Go AppServer | Reflect the next Host lifecycle snapshot without creating, changing, or clearing removal actions. |
| Legion HNA/NNA | Observe actions, request SCP deletion, and acknowledge the exact action after accepted deletion handoff. |
| nginx | Proxy the exact acknowledgement path from Runtime pod port 80 to Host port 9091 while preserving the existing site-token authentication header. |

## Flow

```mermaid
sequenceDiagram
    participant L as Legion provisioning
    participant H as Functions Host
    participant W as WorkerProxy
    participant G as Go AppServer
    participant HNA as Legion HNA
    participant SCP as Legion SCP
    participant N as nginx

    L->>H: PUT /admin/workers/{podName}<br/>workerNamespace + workerPodName
    H->>H: Validate route pod name ordinally<br/>and retain identity with registry slot
    H->>W: Graceful FunctionRpc request-stream completion
    alt WorkerProxy termination completes
        W->>W: Persist sticky RemovalRequested
        W-->>H: Relay stream terminates
        H->>H: No fallback action
    else Termination is uncertain
        H->>H: Add unique exact RuntimeUnlink action
        H->>G: PUT Host state with outstandingWorkerRemovals
        G-->>HNA: Next Runtime lifecycle snapshot
        HNA->>SCP: Request exact worker deletion
        SCP-->>HNA: Deletion handoff accepted
        HNA->>N: POST :80/admin/functions/split/acknowledgeWorkerRemoval
        N->>H: Proxy exact path to :9091 with x-ms-site-token
        H->>H: Atomically exact-match and clear action
        H-->>HNA: 204 No Content
        H->>G: Next Host state omits cleared action
    end
```

The acknowledgement means Legion accepted a recoverable deletion handoff. It does not mean the
worker pod has physically finished deleting. Clearing the fallback action does not reopen worker
admission, clear WorkerProxy retirement, or change the terminal worker slot.

## Host state and acknowledgement rules

```mermaid
stateDiagram-v2
    [*] --> LinkedExact: Link included valid worker coordinates
    LinkedExact --> PrimaryComplete: Graceful peer termination completed
    LinkedExact --> Outstanding: Graceful peer termination uncertain
    Outstanding --> Outstanding: Malformed, stale, foreign, or non-exact acknowledgement
    Outstanding --> CompletedEvidence: Exact acknowledgement committed
    CompletedEvidence --> CompletedEvidence: Identical lost-response replay
```

- Link requires `workerNamespace` and `workerPodName`.
- `workerPodName` must equal the route worker ID with ordinal comparison.
- A successful WorkerProxy stream termination never creates a fallback action.
- Runtime publishes each action until an exact acknowledgement is committed.
- Matching uses the full action: resource namespace, pod name, and reason.
- Malformed acknowledgements return `400`; valid unknown or non-exact actions return `404`.
- The first exact acknowledgement and a retained identical replay return `204`.
- Completed evidence is private, process-local, and bounded to 128 exact actions.
- The root registry keeps pending actions across ScriptHost child restarts in the same Runtime process.
- Runtime process restart durability is not provided. A stale acknowledgement sent to another Runtime
  process cannot clear state because that process has no matching outbox action.

Both Link and acknowledgement use the existing Host admin authorization policy. No new credential is
introduced. The existing Legion `LinkFunctionsWorker` path already sends the Runtime container's
`SiteTokenHeader` from pod infrastructure connectivity, so enabling the Host policy does not require
a caller contract change.

## Companion behavior

| Combination | Behavior |
| --- | --- |
| Legion Link -> new Host | Existing worker namespace and pod name are required; the body pod name must match the route. |
| New Host -> AppServer without outbox reflection | AppServer R2's Go JSON decoder ignores the additive field, so existing capacity publication continues. Host retains and periodically republishes actions, but Legion cannot observe them until AppServer gains reflection support. |
| Host/AppServer without the nginx exact route | Legion's port-80 acknowledgement cannot reach Host 9091. The Docker companion change is required before fallback is operational. |

## Required companion contracts

### Legion

Preserve the existing worker namespace and pod name through the Link path and send them as required
top-level `workerNamespace` and `workerPodName` properties to Runtime
`PUT /admin/workers/{workerId}`. HNA uses `runtimeState.Self.App` as the authoritative app identity
when translating each reflected removal action into the SCP deletion request.

Legion tests should prove the final Runtime request preserves both worker coordinates, HNA combines
them with `runtimeState.Self.App`, and acknowledgement returns the exact reflected action.

### Go AppServer

Extend the existing Host-state request and Runtime lifecycle snapshot with
`outstandingWorkerRemovals`. Tests should prove exact round-trip reflection, independent removal of one
action from a multi-action snapshot, and compatibility when the field is omitted.

The AppServer R2 base at `b43f1c5cbfc8116065a80f4f2b2b87211859dfe7` uses Go's default
`json.Decoder` without `DisallowUnknownFields`, so this additive Host field is accepted before the
reflection companion deploys.

### Docker nginx

Runtime pod port 80 needs one narrow exact route:

`/admin/functions/split/acknowledgeWorkerRemoval` -> Functions Host port `9091`

The route must not require `x-site-deployment-id`, because NNA sends `x-ms-site-token`. It must preserve
that token for Host admin authentication and must not route this API through Go AppServer port 6060.

The companion is draft ADO PR
[17739315](https://msazure.visualstudio.com/DefaultCollection/One/_git/AAPT-Antares-Functions-Docker/pullrequest/17739315),
source head `59f5d37abce0fb658a82cbb825449a7a1ff61461`, stacked on AppServer R2 head
`b43f1c5cbfc8116065a80f4f2b2b87211859dfe7`. It covers both HTTP and TLS nginx templates.

## Validation boundary

Repository tests exercise the real Host registry, FunctionRpc close behavior, Host-state JSON
publication, and management controller in memory. They are not deployed evidence. End-to-end
validation still requires coordinated Legion, AppServer, and Docker images on a private stamp.
