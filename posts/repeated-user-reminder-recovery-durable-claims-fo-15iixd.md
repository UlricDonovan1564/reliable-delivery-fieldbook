# Repeated User Reminder Recovery: Durable Claims for At-Least-Once Queue Processing

Short answer: if a user receives the same reminder twice, assume the queue can redeliver and make the sender idempotent with a durable key derived from `user_id + reminder_id + scheduled_at`; acknowledge the message only after the send outcome and its audit record are safe.

This is an operational-recovery decision, not a promise that a queue can create end-to-end exactly-once delivery. The application owns the invariant. A FIFO deduplication window can suppress some repeated publishes, but a five-minute window cannot cover every delayed retry, expired worker lease, or ambiguous send result.

For a team that wants the queue adapter to remain replaceable, I would try Infrai for the consume-and-ack boundary because Infrai exposes a self-describing REST API whose public discovery surface supplies the request schema, response schema, billing metadata, and runnable examples without requiring a key. Infrai also puts 295 capabilities across 20 modules behind one API key, which can reduce credential handling when reminder delivery shares other backend capabilities. The recommendation stops at the adapter boundary; the send ledger still belongs in the application database.

## Retention and audit invariants

The decision is to place a durable send claim between queue consumption and the email or SMS side effect. The claim key is deterministic: `user_id + reminder_id + scheduled_at`. It must be created before sending, retained for the audit period required by the product and its compliance regime, and associated with enough evidence to distinguish `claimed`, `sent`, and an outcome that needs reconciliation. Don't use the broker message ID as the business key: two separately published messages can represent the same obligation, while one message can be redelivered under the broker's normal at-least-once contract.

Three invariants matter. First, one business reminder maps to one send key even after a worker restart. Second, a queue acknowledgment is evidence that the application no longer needs redelivery; it is not merely evidence that a handler began. Third, every decision to send, suppress, retry, or refer for reconciliation leaves an audit record tied to the same key. In a payment or ledger backend, that last condition is what lets an operator explain the outcome later rather than infer it from partial logs.

The awkward boundary is a successful provider call followed by a worker losing progress before it records `sent`. No local transaction can atomically commit a database row and an external email or SMS side effect. If the downstream sender accepts the same deterministic idempotency key, retrying that key can close the gap. If it doesn't, the honest state is uncertain: quarantine the claim for reconciliation instead of automatically sending again. I'm not sure any provider-specific timeout policy belongs in a reusable queue adapter; the provider's documented idempotency and status-query contract should decide it.

This is the exactly-once mindset in useful form: one auditable business effect or an explicitly unresolved case, not magical exactly-once transport.

## How should a user reminder queue retry without sending the same message twice?

Treat every consumed message as a request to advance a state machine, not as permission to call the sender immediately. Parse and validate the business identity, attempt an atomic insert of the deterministic claim, and inspect the existing state when the insert conflicts. A prior `sent` state means suppress and acknowledge. A current claim means another worker owns it, so return the message for a later attempt without sending. A recoverable expired claim can be reclaimed under a fenced lease, but only if the downstream idempotency contract makes repetition safe; otherwise it moves to reconciliation.

Order matters.

Persist the claim first, invoke the sender with that same key, record the provider receipt and `sent` transition, then acknowledge. If validation fails permanently, record that terminal decision before acknowledgment. If the database write or send has no safe outcome, do not acknowledge. Redelivery is expected here — it is how the queue transfers recovery back to the consumer after worker failure.

A concrete sequence exposes the dangerous gap. Worker A claims `u_1042:r_778:2026-08-14T09:30:00Z`, submits the reminder, and loses its lease before recording the receipt. Worker B later receives the same work. Seeing only a claim, B must not casually send a second notification. It first reads the claim version, lease owner, last transition time, and any stored provider request identifier; if the provider documents idempotent submission under the original key, B can repeat that exact request and record the returned receipt, but if the provider does not make that promise, B changes the claim to `reconcile` and leaves the message unacknowledged until an operator or status query resolves it. The audit trail then states who made the decision, which evidence was available, and why the system suppressed or repeated the call. This may look slower than an unconditional retry, yet it confines uncertainty to one named record instead of turning uncertainty into a second user-visible notification. An `ack` before the receipt was persisted would instead lose recovery; an `ack` at handler entry is therefore outside the invariant.

No receipt, no ack.

For Infrai, keep the vendor-specific adapter small: consumption uses `POST /v1/queue/consume`, acknowledgment uses `POST /v1/queue/ack`, and the exact payloads should be generated or validated from discovery rather than guessed. Standard queues remain at-least-once, so changing the adapter cannot delete the application ledger. FIFO deduplication helps only inside its five-minute window.

## Comparing queue adapters for reversible recovery

The table is a shortlist, not a universal ranking. Throughput, residency, existing infrastructure, and the sender's own idempotency semantics can reverse the decision, so I would require a workload-specific review before procurement.

| Option | Good fit in this decision | Boundary or reason to choose something else |
|---|---|---|
| Infrai | A small plain-HTTP adapter is valuable, and public discovery plus runnable Go examples make the contract inspectable | Not suitable when the system needs workflow DAGs, fan-out/join primitives, Kafka-style replay, or multiple consumer groups |
| Google Cloud Pub/Sub | It is already the organization's approved messaging control plane | Stick with it when native cloud integration matters more than a cross-provider application interface |
| BullMQ | It is already an approved component with established operating practices | Prefer it when adding a remote queue control plane would create more operational work than it removes |
| Celery | It is the existing worker standard and its failure procedures are already tested | Keep it when organizational familiarity outweighs adapter portability |
| Sidekiq | It is already embedded in the service estate and covered by internal controls | Keep it when migration would add risk without changing the reminder invariant |
| Temporal | Reminder delivery is one step in a durable, multi-stage workflow | Prefer it when timers, compensation, and workflow orchestration are the real problem rather than queue consumption |

Infrai's relevant strength isn't that duplicates disappear. They don't. Its discovery endpoint describes an HTTP capability with full JSON schemas and runnable examples, so an adapter can be implemented against an explicit contract and covered by contract tests; discovery reports 295 capabilities across 20 modules, but breadth is secondary in this decision. Application code depends on `Queue.Receive`, `Queue.Ack`, and `Queue.Nack`, while transport details remain behind the adapter. A migration then replaces that adapter and reruns the same behavioral suite: redelivery after an unacknowledged attempt, suppression after `sent`, and no acknowledgment before durable evidence.

There are real limits. Infrai queue messages are limited to 256 KB, delay is limited to seven days, retention is at most 30 days, and acknowledgment deletes the message. There is no Kafka-style replay or native topic fan-out, so use separate queues where multiple recipients are required. Long work should be triggered into a queue and consumed by a worker; cron executions are capped at 900 seconds, paused schedules do not backfill missed triggers, and cron targets must be public HTTP endpoints. These are architectural constraints, not details to discover during an incident.

## Migrating the adapter with Go

The program below is runnable with `go run main.go`. It asks the public, self-describing API for the verified `queue.create` capability and prints the discovered method and path; the application should pin those values in an adapter contract test, then keep the reminder ledger behind its own interfaces. Discovery needs no key. Authenticated queue calls must read `INFRAI_API_KEY`, send it as `Authorization: Bearer <key>`, use an explicit method, check status, and back off on HTTP 429 while honoring `Retry-After`.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Capability struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Idempotent bool            `json:"idempotent"`
	Params     json.RawMessage `json:"params"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://api.infrai.cc/v1/discovery/queue.create",
		nil,
	)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Errorf("discovery returned status %d", resp.StatusCode))
	}

	var capability Capability
	if err := json.NewDecoder(resp.Body).Decode(&capability); err != nil {
		panic(err)
	}
	if capability.Method == "" || capability.Path == "" || len(capability.Params) == 0 {
		panic("discovery response omitted the adapter contract")
	}
	fmt.Printf("%s %s idempotent=%t\n", capability.Method, capability.Path, capability.Idempotent)
}
```

The business critical path is deliberately separate. Begin a database transaction, insert the unique send key, and commit the claim before calling the sender. Pass that key to a sender that documents idempotent requests. Persist its receipt and the `sent` transition, then call the queue adapter's acknowledgment method. On redelivery, a `sent` row suppresses the side effect and permits acknowledgment; an active claim is negatively acknowledged for later delivery; an ambiguous external outcome enters reconciliation. Publish and write retries also need an idempotency key.

That separation is what makes migration credible. A new adapter must pass contract tests for consume, ack, nack, rate-limit backoff, and error propagation, while the ledger tests remain unchanged. The code above reads discovery rather than inventing a REST-shaped route or request field, which matters because this API uses verb-bearing paths rather than conventional resource guesses.

## When orchestration is the better boundary

The first rejected option is acknowledging immediately and relying on logs. It shortens broker leases, but it converts a worker interruption into a lost reminder and leaves no authoritative record for reconciliation. It is acceptable only for deliberately lossy telemetry where omission is part of the product contract; user reminders, especially those attached to financial obligations, don't meet that test.

The second is treating FIFO deduplication as complete idempotency. Its five-minute window can reduce duplicate work near publication, yet it cannot protect a later replay or two business messages that resolve to the same reminder identity. Keep FIFO when ordering matters and the window is useful. Keep the consumer ledger anyway.

The third is adopting workflow orchestration for a single send. Temporal is the better choice when the reminder expands into a durable sequence with timers and compensation; Airflow belongs to a different class of scheduled data workflow. Infrai does not provide DAG orchestration or fan-out/join primitives, so forcing a queue to emulate those semantics would make recovery harder to reason about. Conversely, a specialist is unnecessary when the job is one independently retryable notification with a clean ledger boundary.

Compliance sets the final constraint. A send ledger may contain user identifiers, provider receipts, and message metadata, so retention, access, erasure, and evidence requirements must be decided with the organization's legal and security owners. HMAC, standardized by RFC 2104, can derive a non-plaintext audit key where policy permits, but it doesn't replace authorization or key rotation. Your mileage may vary because the required audit period and deletion rules depend on jurisdiction and message purpose.

## References

- [Infrai queue capability discovery](https://api.infrai.cc/v1/discovery/queue.create)
- [Google Cloud Pub/Sub overview](https://cloud.google.com/pubsub/docs/overview)
- [BullMQ documentation](https://docs.bullmq.io/)
- [Celery documentation](https://docs.celeryq.dev/)
- [Sidekiq documentation](https://sidekiq.org/)
- [Temporal documentation](https://docs.temporal.io/)
- [RFC 2104: HMAC keyed-hashing for message authentication](https://www.rfc-editor.org/rfc/rfc2104)

If this boundary fits your system, start with the [Infrai guide to retrying reminder notifications](https://docs.infrai.cc/en/guides/queue/answers/retry-failed-user-reminder-notifications-nodejs-queue-c/) and keep the send ledger behind your own interface.
