# Can You Prove Game Cleanup Ran? A Node.js Queue Example for Delayed Webhook Publishing

A nightly game cleanup must return control without keeping a web request open. The consequential choice is therefore not the cron expression or the Node.js scheduler library; it is where the system can prove that every pending webhook crossed from durable intent into independently retryable work.

Short answer: commit each cleanup result and its webhook intent together, use the cron trigger only to publish stable delivery identifiers to a queue in bounded batches, and let idempotent workers advance an auditable state machine. Transport may be at least once. The business effect must converge once.

This decision treats an unacknowledged handoff as uncertainty rather than success or failure. A timer may overlap with its next run, a publisher may not know whether a send was accepted, and a worker may see the same identifier again. None of those events may erase the obligation or manufacture a second one. The transactional outbox pattern supplies the central boundary: the database transaction stores the business change and the message to be sent; a separate relay publishes it, and consumers remain idempotent because publication can occur more than once.

## How can a nightly cron trigger publish each pending webhook without losing queue work?

It must prove less than teams often claim. The trigger can prove that it selected an eligible record and attempted a handoff; it cannot prove that a partner applied the webhook. Queue acceptance can establish another observation, while only the worker and the destination protocol can classify the external outcome. Collapsing those observations into one `sent` flag destroys the evidence needed for reconciliation.

For a gaming backend, imagine an overnight process that expires unclaimed tournament rewards and schedules partner notifications. The authoritative record should have a stable delivery ID, the reward event ID, a due time, a current state, an attempt count, lease metadata, and transition timestamps. The queue message carries the delivery ID, not the reward payload. That separation permits the worker to reload current, access-controlled data and prevents queue retention from silently becoming the retention policy for player-related records.

The invariants are compact:

- committing a reward expiration creates one logical notification obligation;
- claiming or publishing the same delivery ID twice does not create another obligation;
- a completed delivery cannot return to an active state;
- every state change records its time and actor;
- an inconclusive remote outcome remains explicit until reconciliation resolves it.

No silent gaps.

This is an exactly-once mindset applied at the state transition the application controls, not a promise that a timer, queue, network, and external receiver collectively transport a message exactly once. If the receiver recognizes the stable delivery ID as an idempotency key, retries can converge on one remote effect. Without that receiver contract, a timeout after the remote side has acted is genuinely ambiguous; a larger retry count cannot prove otherwise.

## Data governance starts with evidence, not the scheduler

Begin with an append-only attempt record and a guarded current-state record. The current row answers operational questions quickly; the attempt history explains how it arrived there. A useful sequence is `pending`, `leased`, `published`, `processing`, then either `delivered`, `retryable`, `ambiguous`, or a review state defined by policy. Names may differ, but `published` and `delivered` must not mean the same thing.

The failure boundaries become reviewable once each component owns only one transition. The cleanup transaction creates `pending`. A short scheduled invocation leases due IDs. The publisher records its observed acknowledgement without declaring delivery. A worker claims one ID, loads the authoritative body, sends it under a destination-specific timeout, and records a classified outcome. Reconciliation finds overdue nonterminal records, expired leases, and ambiguous attempts; it does not infer truth from the absence of a process log.

This matters during deployment too. Messages already in flight must remain intelligible to both old and new workers, so the stable envelope needs a version and a delivery ID, while schema changes should be expanded before a producer writes new states. Operational views should emphasize oldest due age and time to terminal resolution, alongside counts by state. A count of 12 pending notifications looks calm even when one has been waiting for two days.

Auditability has a compliance boundary. Attempt histories can support reconciliation, but they can also contain identifiers governed by deletion, residency, contractual, or access-control requirements. Keep webhook bodies and credentials out of queue envelopes and logs, restrict who can inspect attempts, and let an approved retention policy determine how long evidence survives. Queue defaults are not a compliance decision.

## Test failure guarantees by what can be demonstrated

The relevant comparison is evidentiary: after a crash at the worst possible instant, what durable fact remains?

| Mechanism | Durable fact after interruption | Duplicate boundary | Appropriate scope |
|---|---|---|---|
| Direct execution in the scheduled request | Schedule history and any application writes completed before interruption | A retry may repeat partially completed external calls | Small, bounded, local, idempotent maintenance |
| Outbox plus task queue | Business intent survives before publication; stable IDs permit redispatch | Publication and task delivery may repeat | Independent retries for uneven webhook destinations |
| Replayable event stream | Retained events and consumer positions support replay | Consumers may reprocess records | Several independent consumers needing the same ordered history |
| Durable workflow | Persisted orchestration state records step progress | Activity execution may still require idempotency | Multi-step timers, compensation, or human review |

The table does not rank products. It locates the acknowledgement that can be lost and the evidence available afterward. For this cleanup, an outbox plus queue is the decision because partner latency must not extend the scheduled request, per-destination retries need isolation, and one durable delivery ledger can reconcile both missed timer runs and uncertain publication.

The catch is operational weight. Leases expire, workers require deployment, poison work needs review, supporting indexes need maintenance, and audit retention needs approval. The pattern is not suitable for a cleanup implemented as one bounded, local, idempotent database statement with no external effect; direct scheduled execution is clearer there. A durable workflow is a better fit when a single notification evolves into several timed steps, compensation, or human approval. An event stream is justified when multiple consumers require independent replay, not merely because one webhook worker needs retries.

## Put the critical path at the handoff boundary

The application may be called from Node.js, but the contract is language-independent, and this durable note uses Go to expose ownership without borrowing a commercial queue schema. `ClaimDue` must atomically lease eligible rows. `Publish` accepts stable IDs and may receive one again. Crucially, a successful publish does not complete a delivery.

```go
package cleanupdelivery

import (
	"context"
	"errors"
	"time"
)

type Claim struct {
	DeliveryID string
	LeaseToken string
}

type Ledger interface {
	ClaimDue(ctx context.Context, at time.Time, limit int, leaseFor time.Duration) ([]Claim, error)
	RecordPublished(ctx context.Context, deliveryID, leaseToken string, at time.Time) error
}

type Queue interface {
	Publish(ctx context.Context, deliveryID string) error
}

type Dispatcher struct {
	Ledger   Ledger
	Queue    Queue
	Now      func() time.Time
	Limit    int
	LeaseFor time.Duration
}

func (d Dispatcher) DispatchDue(ctx context.Context) (int, error) {
	if d.Ledger == nil || d.Queue == nil || d.Now == nil {
		return 0, errors.New("dispatcher dependencies are required")
	}
	if d.Limit < 1 || d.LeaseFor <= 0 {
		return 0, errors.New("limit and lease duration must be positive")
	}

	now := d.Now().UTC()
	claims, err := d.Ledger.ClaimDue(ctx, now, d.Limit, d.LeaseFor)
	if err != nil {
		return 0, err
	}

	published := 0
	for _, claim := range claims {
		if claim.DeliveryID == "" || claim.LeaseToken == "" {
			return published, errors.New("claim lacks delivery identity or lease token")
		}
		if err := d.Queue.Publish(ctx, claim.DeliveryID); err != nil {
			return published, err
		}
		if err := d.Ledger.RecordPublished(
			ctx, claim.DeliveryID, claim.LeaseToken, d.Now().UTC(),
		); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}
```

There is an unavoidable interval between queue publication and `RecordPublished`. If the process ends there, the ledger can later expose the expired lease and republish the same ID. The first message may already exist, so the worker must treat duplication as normal. It should acquire a guarded processing claim, reload the delivery, refuse to reopen a terminal row, use the same delivery ID for every destination attempt, and commit the outcome with a compare-and-set transition. That is a recovery protocol, not an incidental error handler.

Batch size should be configurable and selected from measured database capacity, queue limits, invocation duration, and backlog age; there is no defensible universal number here. I'm not sure what limit fits a particular deployment until those measurements exist. The invariant is independent of the number: each scheduled invocation claims a bounded page and returns after handoff, never after waiting for partner responses.

Test the intervals, not just the happy path. Run overlapping dispatchers; interrupt one after leasing and before publishing; interrupt another after publishing and before recording the acknowledgement; deliver one ID twice; and hold a destination response beyond the worker timeout. The expected result is not that every function ran once. Each obligation remains discoverable, expired work becomes eligible again, a terminal transition cannot be repeated, and the attempt history explains every observation.

## Why direct execution was rejected

Reject direct webhook calls from the nightly trigger for this gaming workload. One slow partner would extend unrelated cleanup work, retries would be coupled to a schedule run, and partial progress would be recorded in the wrong place. Schedule history describes timer execution; it is not a delivery ledger.

Keep direct execution when the job is a small local mutation with a known bound and no separately retryable external side effect. Otherwise, choose the smallest mechanism that preserves intent before handoff, gives each item a stable identity, makes duplicate processing safe, and leaves enough evidence to reconcile uncertainty. The scheduler starts work. The ledger proves what happened.

## Sources

- https://microservices.io/patterns/data/transactional-outbox.html
- https://cloud.google.com/tasks/docs/dual-overview
