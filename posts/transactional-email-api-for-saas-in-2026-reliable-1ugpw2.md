# Transactional Email API for SaaS in 2026 — Reliable Course Receipts

Choose a transactional email API for an edtech receipt only after defining the evidence required to close one settled order. The decisive capability is not a pleasant Node.js SDK or a low advertised rate; it is whether the complete system can preserve a durable, auditable chain from payment settlement through message acceptance and later delivery evidence when requests time out, events repeat, and workers restart.

Short answer: treat Resend, Postmark, SendGrid, and Mailgun as interchangeable candidates until each passes the same failure protocol. A settled order should create one immutable receipt obligation, every attempt should append evidence, and an ambiguous API result should remain visibly unresolved until retry or reconciliation supplies more evidence. Compare the four services on those tested boundaries, along with current US/EU contractual handling, instead of declaring a universal winner from feature pages.

This is an exactly-once mindset applied to an at-least-once world. It does not promise one physical email. It establishes one logical obligation and makes every uncertain transition inspectable.

## What should a SaaS transactional email API prove for welcome receipts?

The architecture decision record begins with 4 invariants. First, settlement is the sole eligibility event: authorization, browser return, and an optimistic client state cannot authorize a receipt. Second, `(order_id, settlement_version, purpose)` identifies one logical obligation. Third, attempts are append-only facts; retrying cannot erase the timeout that caused the retry. Fourth, delivery evidence may advance notification state but can never rewrite payment truth. Those invariants create 3 distinct failure boundaries. Inside the payment database, the system must prevent a receipt obligation from existing without the corresponding settled state. Across the email API boundary, a lost response creates uncertainty because local timeout does not prove remote rejection. Across the event boundary, duplicated or reordered callbacks must not cause a completed receipt to move backward. Each boundary needs its own identifier and audit record. The terminal word matters as well: "sent" is too vague for a ledger-shaped workflow because it can mean that a worker began a request, that a remote API accepted it, or that a recipient server reported delivery. Use explicit states such as `pending`, `attempting`, `accepted`, `delivered`, `failed`, and `needs_review`, and retain the evidence that justified each transition. The state names are local policy, not claims about a provider contract.

Keep student data out of operational dimensions. An audit row can carry an internal recipient reference, template revision, settlement version, attempt identifier, external identifier when one is returned, evidence type, and UTC timestamp without putting an email address into metric labels. Retention, deletion, access control, subprocessors, data location, and cross-border transfer still require a current legal and security review for US/EU operation. A region label alone cannot answer those questions.

This is the hard boundary: DKIM can authenticate responsibility for a message domain, but it cannot prove that a course order settled or that a missing HTTP response means rejection. RFC 6376 specifies the signing and verification mechanism, including canonicalization and DNS-published keys. Financial eligibility and reconciliation remain application concerns.

## Compare closure evidence before developer convenience

The useful comparison is an acceptance test, not a feature census. Resend, Postmark, SendGrid, and Mailgun should receive identical test messages from isolated accounts configured for the intended geography and domain. Product behavior and contracts can change, so the decision record should name the account configuration, document date, test date, and result rather than preserving a context-free check mark.

| Evidence required for a settled course order | Resend | Postmark | SendGrid | Mailgun |
| --- | --- | --- | --- | --- |
| Request identity | Verify documented deduplication scope and expiry; test it | Verify documented deduplication scope and expiry; test it | Verify documented deduplication scope and expiry; test it | Verify documented deduplication scope and expiry; test it |
| Lost-response recovery | Inject a timeout after request transmission | Inject a timeout after request transmission | Inject a timeout after request transmission | Inject a timeout after request transmission |
| Event integrity | Validate current signing guidance, replay behavior, and key rotation | Validate current signing guidance, replay behavior, and key rotation | Validate current signing guidance, replay behavior, and key rotation | Validate current signing guidance, replay behavior, and key rotation |
| Reconciliation evidence | Test lookup or export against stored external identifiers | Test lookup or export against stored external identifiers | Test lookup or export against stored external identifiers | Test lookup or export against stored external identifiers |
| US/EU handling | Review current region, retention, subprocessor, and transfer terms | Review current region, retention, subprocessor, and transfer terms | Review current region, retention, subprocessor, and transfer terms | Review current region, retention, subprocessor, and transfer terms |

The cells intentionally describe work rather than award scores. The available primary source establishes that Resend presents an email API with SDK documentation; it does not establish comparable limits or legal terms for all four candidates. Filling this table with unsupported yes/no claims would create false precision. Procurement should attach the current primary documents for every finalist, then preserve the test artifacts with the decision.

Price belongs after reliability qualification. Model the actual workload, including retries, test traffic, retained evidence, operational review, and any required support or infrastructure, but do not let a volatile unit price override an inability to explain an ambiguous receipt. Developer convenience receives similar treatment: a concise Node.js call is useful, yet it is not the recovery contract.

Stop there.

## Make evidence reduction the critical path

The critical implementation is a deterministic reducer that accepts authenticated, deduplicated evidence and permits only monotonic transitions. Provider adapters can translate their payloads into this small vocabulary. The domain code then stops depending on dashboard labels or event ordering.

```go
package receipt

import (
	"errors"
	"time"
)

type State string

const (
	Pending     State = "pending"
	Attempting  State = "attempting"
	Accepted    State = "accepted"
	Delivered   State = "delivered"
	Failed      State = "failed"
	NeedsReview State = "needs_review"
)

type EvidenceKind string

const (
	AttemptStarted EvidenceKind = "attempt_started"
	APIResponse    EvidenceKind = "api_accepted"
	DeliveryEvent EvidenceKind = "delivery_reported"
	HardFailure   EvidenceKind = "permanent_failure"
	AttemptUnknown EvidenceKind = "attempt_unknown"
)

type Evidence struct {
	ID         string
	LogicalKey string
	Kind       EvidenceKind
	ObservedAt time.Time
	ExternalID string
}

func Reduce(current State, e Evidence) (State, error) {
	if e.ID == "" || e.LogicalKey == "" || e.ObservedAt.IsZero() {
		return current, errors.New("incomplete receipt evidence")
	}

	switch e.Kind {
	case AttemptStarted:
		if current == Pending {
			return Attempting, nil
		}
	case APIResponse:
		if current == Attempting || current == NeedsReview {
			return Accepted, nil
		}
	case DeliveryEvent:
		if current == Accepted || current == NeedsReview {
			return Delivered, nil
		}
	case HardFailure:
		if current == Attempting || current == Accepted {
			return Failed, nil
		}
	case AttemptUnknown:
		if current == Attempting {
			return NeedsReview, nil
		}
	}

	return current, nil
}
```

The reducer is deliberately boring. That helps.

Its database transaction should first reject an already-recorded evidence ID, then lock the receipt obligation, append the evidence, calculate the next state, and commit both the evidence and new state together. A unique constraint on the logical key prevents duplicate obligations; a separate unique constraint on evidence ID absorbs callback replay. Neither constraint proves that only one physical message crossed the network.

An API adapter should classify results narrowly: definite acceptance, definite permanent rejection, retryable rejection, or unknown. Network cancellation and timeout usually belong in `unknown` unless the documented remote contract proves otherwise. Retrying with the same logical identity may reduce duplicates where a provider offers documented idempotency, but the local audit trail must not assume semantics, scope, or expiry that have not been verified.

The dispatch worker leases an obligation for a bounded interval and records `AttemptStarted` before making the network call. It must not keep a database transaction open during that call. If the process disappears after remote acceptance and before local recording, the expired lease permits recovery while the unresolved attempt remains available to reconciliation. Backoff needs jitter and a maximum notification age; once that policy limit expires, the obligation moves to `needs_review` instead of retrying indefinitely.

## Prove recovery with a settlement matrix

A happy-path integration test says very little about delivery reliability. Build a matrix whose rows are interruption points and whose assertions concern accounting state. Duplicate settlement events must leave one logical obligation. A rolled-back settlement must leave none. Two workers must not hold the same live lease. A repeated event ID must not append evidence twice. A delivery event arriving before the acceptance record must not corrupt the aggregate, and a late failure must not move `delivered` backward.

Then exercise uncertainty. Terminate a worker after request transmission but before it records the result. Delay an event beyond the first reconciliation run. Replay it after key rotation. Exhaust the configured service limit in the isolated test account. Every case should finish with an unchanged settled order, one receipt obligation, an explainable sequence of attempts, and either closure evidence or an explicit review state.

No invented certainty.

Observability follows the same model. Correlate settlement event ID, order ID, logical key, attempt ID, evidence ID, and external identifier where available. Measure obligation age, lease age, outcomes by classification, unresolved-attempt age, and reconciliation lag. Alert on aging evidence gaps rather than raw send volume, because a busy queue can be healthy while one old settled order is quietly missing its receipt.

Deployment should begin in record-only mode: create obligations for new settlements, suppress dispatch, and reconcile the counts. Next, send to controlled internal recipients while running the evidence reducer and reconciliation job from day one. Expand by a small tenant cohort only after duplicated events, worker termination, and key rotation have produced the expected audit history. The release gate is closure under failure, not one successful request.

## Rejected option: let callbacks define receipt truth

The rejected design stores the latest callback as the receipt status and treats that mutable row as the record. It looks attractive because the provider event stream appears to supply the whole lifecycle, while the application needs little more than an endpoint and one status column. It fails the audit requirement: events can repeat or arrive out of order, the latest arrival is not necessarily the latest fact, and provider vocabulary can leak into financial workflow decisions.

This option does have a valid use case. For low-consequence product announcements where a lost or duplicated message does not assert payment and no per-order reconciliation duty exists, a current-status projection may be adequate. Even there, authenticate events according to current documentation and deduplicate them. A settled course-order receipt has a higher bar because it communicates a financial fact to a student or purchaser.

The adopted design therefore keeps callbacks as evidence, never authority. The payment record authorizes the obligation, the append-only evidence log explains attempts, and the reducer derives a reproducible notification state. Any of the four candidate APIs can fit only after its observed behavior and current terms satisfy that boundary. The brand is replaceable; the proof chain is not.

The limitation is operational weight: an evidence log, reducer, lease recovery, reconciliation job, and review queue create more storage, code, and on-call responsibility than a direct send. That trade-off is justified for receipts that assert a settled purchase. It is not suitable for every message; a low-consequence welcome note with no payment assertion may reasonably use a smaller current-status design, provided the team accepts weaker per-message reconstruction.

## References

- RFC 6376, DomainKeys Identified Mail (DKIM): https://datatracker.ietf.org/doc/html/rfc6376
- Resend documentation, Introduction: https://resend.com/docs/introduction
