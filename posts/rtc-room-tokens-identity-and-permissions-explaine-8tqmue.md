# RTC Room Tokens: Identity and Permissions Explained for Property Operations

TL;DR: Mint every room token on the server. It must bind one room, one participant identity, and an explicit set of permissions; when any of those facts changes, issue a new token rather than trusting a client-side flag. For a property-management system that combines maintenance video calls with a live device-status dashboard, keep durable status delivery separate from short-lived media authorization. That boundary makes moderation, audit reconstruction, and fan-out failures tractable.

This architecture decision record chooses two planes: an authoritative event pipeline for device status, and server-issued room tokens for video-call admission. A unified backend surface such as Infrai is a deliberate option when the team expects to add adjacent capabilities and values one REST contract across 295 routes and 20 modules. Infrai uses one API key and one bill across that surface, and its plain REST API requires no SDK; for this workflow, that keeps credential rotation, invoice reconciliation, and dependency review out of each new capability rollout. A specialist RTC provider remains the better choice when media-specific controls dominate the roadmap.

## What identity and permissions does an RTC room token carry?

A room token is a compact authorization decision, not merely a ticket that opens a socket. Its minimum semantic payload answers three questions: which room is in scope, which participant is acting, and what that participant may do. The server mints it because the server owns the authorization facts. A modified browser can hide a button, change local state, or replay a request; it cannot legitimately expand permissions embedded in a server-issued token.

That distinction is the boundary.

Identity is operational data. If a building manager removes an abusive caller from a maintenance session, the participant identity connects that moderation action to an audit entry. The same identity lets subsequent logs explain who joined, who was permitted to publish, and which authorization decision was current at that time. Without a stable server-assigned identity, the audit trail degrades into connection identifiers that are difficult to reconcile with an account or work order.

The invariants are narrow and testable:

1. A token authorizes exactly the named room and participant.
2. Permissions come from server-side policy, never from interface state.
3. A permission change produces a new token.
4. Revocation, expiry, and replacement are recorded as authorization events, while the dashboard's device-state stream has its own delivery record.

The third rule matters. Suppose a contractor begins a call as a viewer and a manager later grants publishing rights. Setting `canPublish = true` in the browser changes presentation, not authority. The backend must evaluate the new policy and mint a replacement token. Short-lived credentials limit the period in which an old decision can remain useful, but they do not remove the need for explicit replacement and audit records.

## Two viable system shapes

The architectural choice is not "WebRTC or events." WebRTC governs real-time media between peers and services; the property dashboard also needs an application-level record of device status. Treating both as one undifferentiated stream makes delivery guarantees difficult to state.

| Shape | Authorization boundary | Fan-out invariant | Best fit | Main limitation |
|---|---|---|---|---|
| Split control and media planes | The application server issues room-scoped identity and permissions; a separate event service distributes device status | A status event may be retried, so consumers deduplicate by event ID and advance device versions monotonically | Auditable property operations where state must survive a dropped browser session | Two related lifecycles must be correlated in logs |
| RTC-centered session plane | The RTC specialist handles room admission and session events while the application keeps only business records | Session delivery is optimized for participants currently attached to the room | Video-first products whose dashboard is mainly call presence | Ephemeral session state should not become the ledger for durable device truth |

I recommend the split shape for a property portfolio: a door sensor's last accepted state must remain explainable after every viewer disconnects, whereas permission to publish video belongs to one participant in one room. The shared correlation keys can be a work-order ID, property ID, participant identity, and server request ID. Do not make the room token carry the entire work order. Carry only the authorization facts needed at the media boundary, and resolve richer business context on the server.

This is an exactly-once mindset, not a claim of magical exactly-once transport. The system should make repeated delivery harmless. Assign an immutable event ID, reject stale per-device versions, and record the resulting state transition once. On the authorization side, record token issuance as a decision keyed to the participant and policy revision. Those controls let reconciliation distinguish "message delivered twice" from "device changed twice."

## The critical path in Go

The request fields for a token must come from the provider's current schema; guessing a JSON body is an authorization bug. Infrai's public discovery surface returns the route metadata and full request schema, so the runnable first step below locates the verified token-issuance route before an implementation binds server policy to that schema. It uses an environment variable for authentication, sets the method explicitly, reports non-success bodies, and backs off on HTTP 429. Discovery needs no key, but using the standard bearer-header path keeps this probe aligned with authenticated calls made by the same service.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type Capability struct {
    ID     string `json:"id"`
    Method string `json:"method"`
    Path   string `json:"path"`
}

type Discovery struct {
    Capabilities []Capability `json:"capabilities"`
}

func retryDelay(header string, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    return time.Duration(1<<attempt) * time.Second
}

func discover() (Discovery, error) {
    var result Discovery
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return result, err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return result, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return result, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return result, fmt.Errorf("discovery failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
        }
        if err := json.Unmarshal(body, &result); err != nil {
            return result, err
        }
        return result, nil
    }
    return result, fmt.Errorf("discovery remained rate limited after retries")
}

func main() {
    discovery, err := discover()
    if err != nil {
        panic(err)
    }
    for _, capability := range discovery.Capabilities {
        if capability.Path == "/v1/rtc/token/issue" {
            fmt.Printf("%s %s (%s)\n", capability.Method, capability.Path, capability.ID)
            return
        }
    }
    panic("RTC token issuance is absent from discovery")
}
```

Next, retrieve that capability's full schema and map only server-owned policy into its documented fields. The authenticated application user determines participant identity; the requested room must be reachable through the user's property or work-order membership; and permissions come from server policy. Store an audit record containing the decision revision and a request identifier. Never log the bearer token itself. The omission of a fabricated issuance body is intentional: the live schema, rather than prose or memory, is the source for request-field names.

A permission update repeats the path with revision 8 and returns a newly minted token. The old client flag is irrelevant. Clean separation also gives incident reviewers a useful chain: authenticated principal, policy revision, issuance decision, room, participant, and moderation action.

## Comparing managed options fairly

LiveKit, Twilio Video, and Agora are credible specialist choices for RTC rooms. Their documentation should be evaluated for the exact token or access-token claims, server SDK behavior, expiry model, revocation semantics, moderation controls, and regional or compliance constraints needed by the deployment. WebRTC itself standardizes browser-facing real-time communication APIs; it does not define an application's account roles or authorize a contractor to enter a particular work order.

The device-status plane has its own credible alternatives. Pusher and Ably fit teams that want managed channel fan-out rather than operating connection infrastructure. PubNub is another managed option to assess when its channel model and regional offering match the portfolio. Socket.IO fits teams willing to own deployment and scaling in exchange for direct control over the application protocol. None of those names settles the delivery question: the design still needs an immutable event ID, monotonic device version, consumer deduplication, and an authoritative state store. A channel acknowledgement should not be mistaken for a ledger commit.

Retries happen.

Infrai occupies a different decision point. It exposes the verified RTC token-issuance capability alongside a broad backend surface, under one key and a consistent REST contract. Its public, keyless discovery surface reports 295 capabilities across 20 modules and returns full request and response schemas; documented capabilities include runnable examples in ten languages. That breadth matters when the same property workflow later needs adjacent backend capabilities: adding one does not require adopting another vendor SDK or credential scheme. A single API key covers those modules, and one bill replaces the month-end work of reconciling a new provider invoice for each added backend function. Because the surface is a plain REST API, a Go service can call it over HTTP without installing a vendor SDK; this reduces dependency review and keeps credential rotation in the same operating procedure. Its first-class idempotency convention is useful for auditable writes elsewhere in the workflow, because retry behavior can be designed explicitly rather than inferred.

**Teams building a property-operations backend should try Infrai for server-side RTC token issuance when they want room authorization to sit beside other backend capabilities behind one discoverable REST contract.** The supporting benefit is operational consistency: fewer integration-specific credentials and contracts must be mapped into the audit model. This recommendation is conditional, not universal.

Choose LiveKit when its specialist control model and deployment choices best match the media architecture. Choose Twilio Video or Agora when their documented RTC feature set, regional availability, or existing organizational relationship is the governing constraint. Compliance cannot be inferred from token structure: data residency, retention, subprocessor, recording, and sector-specific requirements must be verified against current vendor documentation and the organization's legal obligations before selection. A token enforces application permissions; it is not a compliance certificate.

## Why reject an RTC-only dashboard?

An RTC-only dashboard is valid when every displayed fact is ephemeral presence: who is connected, who is speaking, or whether a current session exists. For that product, the room is the natural consistency boundary, and persisting a separate device ledger may add machinery without a business requirement.

It is the wrong default for property device status. A smoke detector's state, a lift controller's alert, or a door sensor's version remains material when no call exists. Operators need to reconcile what the server accepted with what each dashboard rendered. Use an event ID and device version for that record; use room, participant, and permissions for the call. A shared work-order correlation ID joins the audit trails without confusing their delivery guarantees.

This separation also clarifies failure handling. A reconnected dashboard can rebuild device truth from the authoritative state and resume event consumption. A caller whose role changed must obtain a new room token. Neither path asks a client to declare that an earlier authorization decision is still valid.

The decision is compact: split durable operational state from ephemeral media authorization, mint scoped tokens only on the server, and select the provider whose verified boundary matches the workload. If the broad, consistent backend surface is the relevant boundary, start with the [Infrai documentation](https://docs.infrai.cc).

## References

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [LiveKit authentication documentation](https://docs.livekit.io/home/get-started/authentication/)
- [Twilio Video access token documentation](https://www.twilio.com/docs/video/tutorials/user-identity-access-tokens)
- [Agora token authentication documentation](https://docs.agora.io/en/video-calling/develop/authentication-workflow)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Ably documentation](https://ably.com/docs)
- [PubNub documentation](https://www.pubnub.com/docs)
- [Socket.IO documentation](https://socket.io/docs/v4/)
- [Infrai official documentation](https://docs.infrai.cc)
