# Provision Tenant Subdomain Inside Signup Transaction with CNAME Rollback Retention

Short answer: Keep signup's database transaction local, reserve the tenant hostname there, and publish its CNAME asynchronously through an idempotent worker. For a B2B SaaS migration away from a registrar-specific API, the least complex reliable design is a durable desired-state row, a small delivery queue, and a reconciler. A failed publication does not roll back the tenant; it leaves a visible, retryable state. The bill to examine first is the retained change history: if there are N tenants, U DNS updates per tenant per retention window, and R windows retained, an append-only per-update ledger holds approximately N x U x R entries, before indexes and backups. A current-state row holds approximately N entries; a bounded audit log preserves only the history needed for investigation. These are planning formulas, not measured prices or traffic estimates.

## What does a CNAME change actually cost to retain?

The records themselves are rarely the whole storage story. Retaining every retry attempt, provider response, previous target, and reconciliation observation indefinitely can make the operational ledger grow with attempts rather than with hostnames. Count separately: one desired-state row per hostname, one durable pending operation per outstanding change, and audit events for significant transitions. For example, 10,000 tenants with two target changes each generate 20,000 intentional changes; five delivery attempts for each change would generate 100,000 attempt events if every attempt were retained. The numbers illustrate cardinality, not a prediction of any registrar's fees.

The dominant term changes when a migration script repeatedly retries a slow control plane. Bound detailed attempt retention according to the organization's audit and incident requirements; keep the latest error, attempt count, and next retry time on the operation while preserving creation, requested target, successful publication, and reversal decisions as audit events. Storage policy must follow applicable contractual and regulatory retention obligations, which vary by organization and jurisdiction. No DNS RFC sets an application audit-retention period.

Retry volume matters.

## Can I provision a tenant subdomain inside the signup transaction?

A database commit cannot atomically commit a change in an external DNS control plane. A synchronous CNAME upsert inside a signup transaction therefore holds database locks while waiting on a network call, and even a successful API response cannot guarantee that recursive resolvers see the new answer immediately. DNS caching is bounded by TTL at the protocol level; negative answers can also be cached. A rollback of the database row after an ambiguous timeout may leave a published CNAME pointing at a tenant the application no longer knows about.

Use a uniqueness constraint on the normalized hostname and an outbox entry written in the same database transaction as the tenant reservation. The outbox is an intent, not proof of publication. An idempotency key derived from tenant ID and desired-state version identifies the operation across retries. A CNAME owner must not coexist with other data at the same name, as specified by RFC 1034. Validate that constraint before reserving a name, and do not assume an API's upsert verb resolves a conflicting record type safely.

The limitation of asynchronous provisioning is immediate availability: a tenant who needs a working hostname before signup completes must instead wait for verified publication, or use an already provisioned hostname pool with a separate assignment transaction. A pool trades faster signup for capacity planning, reservation expiry, and careful control of stale assignments. Neither design can make resolver caches participate in the database transaction. For a migration with a strict cutover deadline, prepublishing and verifying the new target before assigning tenants can reduce the time spent pending, although it increases the number of names reserved ahead of actual demand. This is a deployment decision, not a property of an upsert API.

The application can return a signup result marked pending and activate the hostname only after authoritative verification and application routing both agree on the tenant. No distributed exactly-once claim is necessary: delivery can be at least once, while the effect converges on one versioned desired state. This distinction matters when an HTTP timeout happens after the control plane accepted a write.

Timeouts do not imply failure.

## A bounded reconciliation loop

The following Go sketch isolates the control-plane boundary. A real implementation must persist the version and operation state transactionally, acquire work with a lease, and implement the DNS client against the chosen control plane. The worker rereads desired state before acting, so an old queued operation cannot restore a target that a newer migration step has replaced.

```go
package dnswork

import (
	"context"
	"errors"
)

type Desired struct {
	Name    string
	Target  string
	Version uint64
}

type Store interface {
	Current(ctx context.Context, name string) (Desired, error)
	RecordAttempt(ctx context.Context, name string, version uint64, err error) error
	MarkVerified(ctx context.Context, name string, version uint64) error
}

type DNS interface {
	EnsureCNAME(ctx context.Context, name, target string) error
	AuthoritativeCNAME(ctx context.Context, name string) (string, error)
}

var ErrSuperseded = errors.New("operation superseded")
var ErrNotConverged = errors.New("authoritative answer not yet converged")

func Reconcile(ctx context.Context, store Store, dns DNS, job Desired) error {
	current, err := store.Current(ctx, job.Name)
	if err != nil {
		return err
	}
	if current.Version != job.Version || current.Target != job.Target {
		return ErrSuperseded
	}
	if err := dns.EnsureCNAME(ctx, job.Name, job.Target); err != nil {
		return errors.Join(err, store.RecordAttempt(ctx, job.Name, job.Version, err))
	}
	actual, err := dns.AuthoritativeCNAME(ctx, job.Name)
	if err != nil || actual != job.Target {
		return ErrNotConverged
	}
	return store.MarkVerified(ctx, job.Name, job.Version)
}
```

This example deliberately does not delete a record on a generic error. If a tenant cancels signup, record a new desired-state version representing removal and verify ownership before issuing a delete; never replay a stale compensating action against a hostname that has since been reassigned. A production worker also needs retry backoff, terminal validation errors, lease expiry, structured logs keyed by hostname and version, and an alert for operations that remain pending beyond the agreed cutover window. `MarkVerified` must compare the version again in its database update, since the state can change between the initial read and the authoritative lookup.

## How fast should the migration cut over?

Speed has a ceiling set by cached answers, not merely by worker throughput. Lowering a TTL immediately before cutover cannot shorten copies already cached under the former TTL. RFC 1035 defines TTL as the interval for which a resource record may be cached; RFC 2308 specifies negative caching behavior. Measure authoritative convergence separately from sampled recursive-resolver observations, and make the activation policy explicit. A fast cutover accepts a period of mixed answers; a conservative cutover waits for the prior cache lifetime and validates the application endpoint before switching traffic assumptions.

For a registrar-API exit, first represent existing zone records in a provider-neutral desired-state inventory, then compare it with authoritative answers without writing. Deploy the worker in observation mode, resolve conflicts, and stage publication for a small cohort. Keep a count of pending operations, oldest pending age, version conflicts, and verified CNAME mismatches. During the actual transition, preserve the old routing target until the new endpoint can serve the tenant. Reconciliation is a continuing control, not a one-time migration script.

Deletion is the hard edge. If rollback means undoing an unverified signup, retain the reservation long enough to prevent immediate hostname reuse while an earlier DNS write may still be in flight. If rollback means reverting an already active tenant, create a new version targeting the prior healthy endpoint and wait for verification; the database cannot revoke resolver caches. Choose the wait according to the previous TTL, observed propagation, and the business tolerance for mixed routing, not an assumed instant rollback.

Do not recycle the hostname while an older write is unresolved.

## What history can be discarded?

Keep the latest desired state, the outstanding operation, and a durable record of who requested a target change and when it was verified or reversed. Discard routine successful retry traces after the required retention window, subject to local compliance policy. That decision reduces the N x U x R history term, but it also means a later investigation may establish the requested target and final verified answer without reconstructing every transient API response. Preserve aggregate retry metrics and explicit failure events if that loss of detail would impede incident review.

An authoritative answer is evidence of publication, not proof of every resolver's view. An audit trail establishes intent and decisions, not a global exactly-once DNS transaction. Those boundaries should be written into the signup contract so support and engineering describe a pending hostname honestly while the migration proceeds.

## Further reading

- RFC 1034, Domain Names: Concepts and Facilities: https://datatracker.ietf.org/doc/html/rfc1034
- RFC 1035, Domain Names: Implementation and Specification: https://datatracker.ietf.org/doc/html/rfc1035
- RFC 2308, Negative Caching of DNS Queries: https://datatracker.ietf.org/doc/html/rfc2308
