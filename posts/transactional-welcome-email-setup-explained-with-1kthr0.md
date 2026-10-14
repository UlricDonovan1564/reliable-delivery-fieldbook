# Transactional Welcome Email Setup Explained with 30 Day Custom Domain Evidence

The least complex defensible transactional welcome email setup for a beginner SaaS is one authenticated custom domain, one durable message record, and a 30-day operational evidence window that can be extended when policy requires it. For an edtech contact form serving US and EU users, the backend should first route the request to the correct support queue, then enqueue the acknowledgement; the browser must never become responsible for sending email, and no DNS recipe can guarantee deliverability.

TL;DR: publish one SPF policy for the actual envelope sender, sign each message with DKIM, and publish DMARC for the visible From domain with reporting enabled. Keep message identifiers, routing decisions, delivery events, and authentication configuration versions, but stop retaining rendered bodies after the defined window unless a legal or support requirement says otherwise. That choice reduces the dominant storage and privacy burden, although it makes later content-level investigations harder.

## What does the bill actually contain?

The expensive term is rarely a DNS record. It is retained evidence multiplied by messages, events per message, replicas, and retention days. If a contact-form acknowledgement produces a queued event, an acceptance event, and perhaps a bounce or complaint event, storing the complete MIME body beside every event repeats the largest and most sensitive object. Attachments make that error much worse. The same model applies to a welcome email sent after account creation, although the business event and consent analysis may differ from a support acknowledgement; those flows should not share an idempotency key merely because they use the same custom domain.

Content dominates.

A useful capacity model is `daily messages × retained bytes per message × retention days × replication factor`. Put in illustrative planning inputs, not claimed benchmarks: 100,000 messages per day, 4 KiB of normalized metadata, 30 days, and two copies yield about 24 GB. Retaining a 200 KiB rendered body under the same assumptions yields about 1.2 TB. The change that moves the dominant term is therefore data minimization: store one compact message record and append small events keyed to it, rather than copying content into every delivery event.

This is an architectural estimate. Measure actual serialized records, indexes, backups, and provider event volume before setting capacity.

The audit record should answer who requested the send, which template and locale were selected, which support queue received the case, which sender domain and DKIM selector were active, and how the transport disposition changed. It should not quietly become a second mailbox. An opaque user identifier is usually more useful than an address scattered through logs, provided an authorized system can resolve it during an investigation.

## How should a beginner approach custom domain transactional welcome email setup?

SPF, DKIM, and DMARC solve related but different problems, and applying all three is among the basic deliverability practices for custom-domain email. SPF authorizes hosts to use a domain in the SMTP envelope; forwarding can disrupt that path. DKIM attaches a cryptographic signature to selected headers and the body, allowing a receiver to validate that the signed material survived transit. DMARC evaluates alignment between the domain visible to the reader in `From` and a domain authenticated by SPF or DKIM, then lets the domain owner publish a requested handling policy and reporting addresses.

Alignment is the beginner trap. A message can pass an SPF check for a transport domain yet fail DMARC because that domain does not align with the visible From domain. The practical design is to assign a dedicated subdomain such as `notify.example.edu`, configure its return path and DKIM signing deliberately, and keep human support replies on a separate path. DNS ownership also belongs in change control: selector rotation and SPF edits need reviewers, timestamps, and rollback records.

Start DMARC observation with a reporting address your organization is prepared to operate, inspect legitimate sources, and tighten policy only after the inventory is credible. RFC 7489 defines `p=none` as requesting no specific receiver action, while `quarantine` and `reject` request stronger treatment. A published policy is not proof of inbox placement, and receiver behavior remains outside the sender's control.

## Keep routing and sending as separate transactions

The contact form has two outcomes: create and route the support case, then acknowledge receipt. Treating those as one synchronous HTTP operation makes a mail timeout look like a failed case submission and invites duplicate tickets when the student retries. The better boundary is a database transaction that stores the case, its queue decision, and an outbox item together; a worker later claims that item and sends it.

Exactly-once delivery across a database and an SMTP system is not a realistic primitive. An exactly-once mindset still helps: define one idempotency key, make state transitions monotonic, and assume a worker can stop after the remote system accepts a message but before the local commit. Retries may still produce a duplicate, so the template should avoid language that turns an acknowledgement into a second business action.

Duplicates remain possible.

```go
package mail

import (
	"context"
	"errors"
	"time"
)

type WelcomeJob struct {
	ID             string
	CaseID         string
	Queue          string
	RecipientRef   string
	Template       string
	SenderDomain   string
	DKIMSelector   string
	CreatedAt      time.Time
}

type Store interface {
	Claim(ctx context.Context, jobID string) (WelcomeJob, error)
	MarkAccepted(ctx context.Context, jobID, transportID string, at time.Time) error
	MarkRetryable(ctx context.Context, jobID, reason string, at time.Time) error
}

type Transport interface {
	Send(ctx context.Context, job WelcomeJob, idempotencyKey string) (string, error)
}

func Deliver(ctx context.Context, store Store, transport Transport, jobID string) error {
	job, err := store.Claim(ctx, jobID)
	if err != nil {
		return err
	}

	transportID, err := transport.Send(ctx, job, job.ID)
	if err != nil {
		return errors.Join(err, store.MarkRetryable(ctx, job.ID, "transport_error", time.Now()))
	}
	return store.MarkAccepted(ctx, job.ID, transportID, time.Now())
}
```

This interface intentionally records references and configuration identity, not the student's message text. The transport adapter can change without changing the outbox contract, which keeps integration effort bounded and makes a local fake adequate for most tests.

## Test the evidence, not the open pixel

Acceptance by a transport is not delivery, delivery is not inbox placement, and an open is not proof that a person read the message. Apple Mail Privacy Protection downloads remote content in the background and prevents senders from seeing whether a recipient opened a message, so open rate is unsuitable as a correctness signal.

Build reconciliation around states you can explain: queued, claimed, transport accepted, bounced, complained, and expired. Alert on stuck age, repeated claims, authentication failure samples, and an unexpected rise in permanent bounces. Test DNS and message authentication from outside the production network; test the outbox with worker termination immediately before and after each durable write; and test that a repeated form submission reuses the intended case or is explicitly recorded as a new one. These best practices improve diagnosis, but they have a limitation: they cannot reveal every receiver's filtering logic or prove inbox placement. A team that cannot operate event reconciliation and DNS change control should choose a managed transport arrangement with suitable support, while a team with strict data-location or deep transport-control requirements may prefer infrastructure it can operate and audit directly; neither choice removes responsibility for domain authentication and retention policy.

Keep US and EU handling grounded in an actual data inventory and counsel-approved retention schedule rather than assuming geography alone determines one universal number. The engineering control is straightforward: attach a retention class when the record is created, erase or anonymize on schedule, log the deletion result, and restrict access to content-bearing evidence.

## The deliberate loss

After 30 days, delete rendered bodies and transient delivery payloads under the assumed operational policy, while preserving the smaller audit fields for whatever separately approved period applies. Thirty days is a design input here, not a statutory limit. Teams that need a different investigation window should change it explicitly and recalculate the storage and privacy exposure.

What is lost is real: after deletion, an investigator may prove that template version 12 was requested, signed with selector `s2`, accepted by the transport, and later bounced, but cannot reconstruct the exact personalized body. That is the price of refusing to retain a shadow mailbox. Preserve a content hash only if it supports a defined verification procedure; a hash cannot recreate content and may still deserve careful treatment when inputs are predictable.

The final production check is reconciliation. Every created outbox record must end in an explainable terminal state or appear in an aging report, every DNS change must have an owner and timestamp, and every deletion job must leave evidence that it ran. Authentication establishes domain-level trust signals; the ledger establishes operational accountability.

## Further reading

- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Apple Mail Privacy Protection guide: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
