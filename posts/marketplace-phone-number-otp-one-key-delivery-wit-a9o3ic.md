# Marketplace Phone Number OTP: One-Key Delivery with Recovery-Safe Session Revocation

**Short answer:** A single API credential with bundled SMS delivery is the simplest defensible choice for Node.js phone OTP login only when the service also provides deterministic verification results, abuse controls, and exportable delivery evidence. The marketplace must still own account linkage, recovery policy, refresh-token rotation, and session revocation. Treat SMS verification as one authentication event, never as the durable account or session; otherwise, a stolen buyer session or recycled phone number can become a permanent recovery credential.

One key is operationally pleasant. It is not a security property.

For a marketplace, the selection gate is therefore larger than the happy-path SDK call: can the design rotate every refresh token, detect reuse, revoke a stolen session family, and replace a phone credential without silently transferring orders, balances, or seller privileges? A bundled verification API may reduce carrier and code-delivery work, but it does not answer those questions for the application.

## What should one phone number OTP login API still prove?

The smallest credible interface has two commands: begin a challenge for a normalized phone number, then check a submitted code against that challenge. The service may generate the code and deliver the SMS, but the marketplace should generate its own opaque challenge identifier, bind it to a narrow purpose, and decide what a successful check is allowed to do. Login, adding a payout destination, changing a phone number, and recovering an account are different purposes. Reusing one successful OTP across them erases the authorization boundary that an audit later needs to reconstruct.

Selection should start with behavior under pressure. Can the verifier enforce expiration, attempt limits, resend throttles, and controls per destination and per origin? Does it distinguish an invalid code from a provider timeout internally while returning a generic response that does not reveal whether the phone number has an account? Can operators correlate one application challenge with verification and delivery events while keeping the OTP itself out of logs? OWASP recommends generic authentication errors and protection against automated attacks; those properties belong in acceptance tests, not in a later hardening ticket.

SMS also has a documented assurance ceiling. NIST SP 800-63B treats the public switched telephone network as a restricted authenticator and requires verifiers to consider risks such as SIM change and number porting. A marketplace can use SMS where its risk assessment permits it, but a phone OTP should not independently authorize high-impact recovery or a payout change. Step-up authentication, prior-device confirmation, a delay, or human review may be appropriate according to the asset at risk. Compliance teams should record the chosen control and scope rather than infer phishing resistance from possession of a phone number.

Keep the records separate:

| Record | Stable identity | Security state | Audit purpose |
| --- | --- | --- | --- |
| Account | Internal account ID | Recovery methods, risk flags | Ownership and reconciliation |
| Phone credential | Credential ID | Normalized number, verified and disabled times | Login binding and number-change history |
| OTP challenge | Random challenge ID | Purpose, expiry, attempts, terminal result | Abuse review |
| Session family | Random family ID | Current generation, revoked time, cause | Rotation and theft containment |
| Session token | Hash or unique token ID | Issued and consumed times | Replay detection |

A phone number is mutable and may be recycled. It is a credential attached to an account, not the primary key of that account.

## Rotation turns replay into a revocation signal

After OTP verification, issue a short-lived access token and a refresh token whose server-side record belongs to a session family. OAuth 2.0 Security Best Current Practice describes refresh-token rotation as issuing a new token on every refresh while invalidating the previous one. If an invalidated token is later presented, the authorization server cannot determine which party is legitimate, so it revokes the active refresh token associated with that grant. In this marketplace design, the corresponding response is to revoke the entire family and require fresh authentication.

Exactly-once execution matters. Two requests can present the same refresh token before either response reaches the client. A read followed by an update is insufficient because both requests may observe an unused token and mint descendants. Consume-and-create needs one transaction, a unique token identity, and a conditional state transition. The first request commits; the other becomes evidence of replay.

This Go example is storage-oriented rather than tied to an identity product. `Rotate` must execute in one database transaction, and the repository must lock or conditionally update the presented token. Generate token material with a cryptographically secure random source and store only a verifier-safe hash.

```go
package session

import (
    "context"
    "errors"
    "time"
)

var ErrReplay = errors.New("refresh token already consumed")

type Token struct {
    ID         string
    FamilyID   string
    Generation uint64
    ExpiresAt  time.Time
}

type Repository interface {
    WithTx(context.Context, func(Repository) error) error
    ConsumeIfActive(context.Context, string, time.Time) (Token, bool, error)
    InsertSuccessor(context.Context, Token, string, time.Time) error
    RevokeFamily(context.Context, string, string, time.Time) error
}

func Rotate(ctx context.Context, repo Repository, oldHash, newHash string, now time.Time) error {
    return repo.WithTx(ctx, func(tx Repository) error {
        current, consumed, err := tx.ConsumeIfActive(ctx, oldHash, now)
        if err != nil {
            return err
        }
        if !consumed {
            if current.FamilyID != "" {
                if err := tx.RevokeFamily(ctx, current.FamilyID, "refresh_replay", now); err != nil {
                    return err
                }
            }
            return ErrReplay
        }
        return tx.InsertSuccessor(ctx, current, newHash, now)
    })
}
```

Production code must reject an expired or revoked family before insertion. The invariant is compact: at most one usable leaf exists per family, and reuse of an ancestor closes the family. Short access-token lifetime limits the interval before family revocation takes full effect; operations that cannot tolerate that interval can consult an account revocation epoch.

There is a hard trade-off. A client may successfully rotate, lose the response, and retry the old token. Without sender-constrained tokens or a tightly bounded idempotent response cache, the server cannot distinguish that retry from theft. The conservative response is family revocation. It costs another login, but allowing two valid branches for retry ergonomics defeats replay detection.

## Recovery is a controlled credential replacement

Suppose a seller reports a stolen phone while an attacker still holds a refresh token. A code sent to the new number proves control of that number; it does not prove ownership of the existing marketplace account. Recovery must start from an independent, previously established factor or a reviewed evidence process. It must never find an account by the new number and overwrite the old credential in one step.

The recovery transaction should bind an approved case to the internal account ID, attach the newly verified phone credential, disable the old credential, increment an account-level session epoch, and revoke every active session family. Those writes should commit together, or a durable recovery job should make retries idempotent through the case ID. An audit event records actor, case, account, policy version, prior and new credential IDs, timestamp, and outcome. It must not record OTP values or raw refresh tokens.

No partial success.

The interface should use generic responses so outsiders cannot enumerate sellers. Support tooling should separate evidence collection from approval for accounts with material balances or payout authority. Notifications to existing channels can reveal an unauthorized change, but they are detection controls, not proof of ownership. If policy permits a cancellation window, define precisely which actions are frozen and how reconciliation handles orders already in flight.

Recovery also determines whether bundled verification remains the simplest overall option. A polished OTP call does not remove the work of handling recycled numbers, lost devices, disputed ownership, administrative access, and evidence retention. Security, support, fraud, and compliance should evaluate those flows together; the one-SDK advantage disappears if its event model cannot support the recovery ledger.

## Compare operating models, then roll out narrowly

Three operating models are defensible. A bundled verification service owns code generation, SMS routing, and verification state behind one credential. It minimizes application code, but makes challenge semantics, event export, regional reach, and provider failure behavior acceptance criteria. A verification layer paired with separate SMS delivery offers more routing and data-placement control at the cost of reconciling two systems. A fully application-managed verifier maximizes policy control while making the team responsible for secure generation, storage, rate limiting, carrier routing, deliverability, fraud defense, and on-call response.

| Decision pressure | Bundled verification | Separate verification and delivery | Application-managed |
| --- | --- | --- | --- |
| Initial integration | Smallest surface | Two contracts and event streams | Largest operations burden |
| Routing control | Contract-dependent | Deliberate provider choice | Entirely team-owned |
| Audit correlation | Verify export detail | Reconcile identifiers | Designed end to end |
| Failure isolation | One external boundary | More components and fallback choices | Internal verifier still depends on carriers |
| Recovery policy | Application-owned | Application-owned | Application-owned |

Do not assign invented precision to these rows. Establish pass/fail gates, then exercise surviving options across the marketplace's actual countries, carrier mix, and accessibility requirements. Delivery latency percentiles and completion rates matter only when measured against that workload. Published global averages cannot settle a local routing decision.

Cost comes later. Model challenge attempts, resend amplification, fraud traffic, support review, and engineering ownership instead of leaning on a volatile headline message price. The relevant unit is a completed, legitimate authentication with its operational burden included.

Roll out first in shadow mode: create session-family records and audit events while the existing login remains authoritative, then reconcile issuance, refresh, logout, and recovery outcomes without granting access from the new path. Enable OTP login next for a bounded cohort and low-risk actions. Track challenge starts, terminal verification outcomes, throttling decisions, delivery results, refresh replays, family revocations, recovery approvals, and time to revoke, using pseudonymous identifiers so observability does not become a credential leak.

Deployment needs a kill switch that stops new OTP challenges without disabling established recovery methods, plus revocation by account, credential, and incident window. Test duplicate refreshes, delayed SMS, repeated codes, provider timeouts, recycled-number reports, concurrent recovery approval, and rollback after credential replacement but before notification. Reconcile the verification log, session ledger, and account audit trail.

The decision rule is strict: adopt the least operationally complex model that passes abuse, audit, recovery, and revocation tests for the marketplace's real risk tiers. Bundled SMS delivery may win, but one API key earns no exception from those controls.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://pages.nist.gov/800-63-4/sp800-63b.html
- https://www.rfc-editor.org/rfc/rfc9700.html
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
