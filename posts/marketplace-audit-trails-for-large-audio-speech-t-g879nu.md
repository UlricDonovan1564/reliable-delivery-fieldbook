# Marketplace Audit Trails for Large Audio Speech-to-Text API Upload Timeouts

Short answer: treat a large-audio speech-to-text timeout as an indeterminate ingestion outcome, not as permission to resend the multipart body; record the attempt, reconcile its status, and retry only from an idempotent boundary.

For a marketplace that turns seller interviews, dispute calls, or catalog narration into a private knowledge base, the hard problem is not raising one Node.js `fetch` timeout. The hard problem is proving which recording became which transcript when an upload, provider deadline, or client connection ends without a conclusive response. The design should preserve the source checksum, provider-neutral job identity, attempt history, and final transcript lineage before retrieval or reranking begins.

This is a correctness problem.

## Why can a speech-to-text API timeout on a large Node.js multipart audio upload?

A timeout label collapses several different events. The client may stop waiting while bytes are still in transit; an intermediary may enforce its own deadline; the receiver may accept the file but take longer than the caller's response budget; or the transcription operation may be asynchronous even though the initial request looked synchronous. File size and recording duration correlate with exposure to these boundaries, but neither value identifies which boundary fired.

That distinction changes the recovery rule. If the receiver accepted the recording and the caller blindly retries, two transcription jobs may exist. If the receiver accepted nothing and the caller merely waits, the marketplace knowledge base never receives the document. An HTTP status, when one exists, describes one exchange; it does not by itself establish the business fact that exactly one canonical transcript was committed.

Don't merge transport completion with transcription completion. Give them separate persisted states: `prepared`, `uploading`, `submitted`, `processing`, `succeeded`, `failed`, and `reconcile`. A client-side timeout moves an attempt to `reconcile`, because the outcome is unknown. A documented terminal rejection can move it to `failed`. Only a verified provider result, associated with the stored source identity, can move it to `succeeded`.

The source identity should be stable before any network call: marketplace tenant, recording identifier, content digest, byte count, media type, and ingestion generation. That record is also the audit trail. It answers the uncomfortable reconciliation question later: did a revised recording replace the old one, or did a retry create another transcript from identical bytes?

## Put the retry boundary before the network boundary

Multipart construction is an attempt detail, not the durable unit of work. Persist the intent first, derive a deterministic idempotency key from stable fields, then open the audio and build a fresh multipart body for each permitted attempt. A consumed stream cannot be assumed to be replayable; recreating the reader from the immutable source makes the retry explicit.

The following Go sketch keeps policy separate from any vendor SDK or route. It deliberately models an ambiguous timeout as reconciliation work. The adapter behind `Provider` owns HTTP details, while the ledger owns truth about attempts.

```go
package ingestion

import (
    "context"
    "errors"
    "time"
)

type State string

const (
    Prepared  State = "prepared"
    Submitted State = "submitted"
    Reconcile State = "reconcile"
    Succeeded State = "succeeded"
    Failed    State = "failed"
)

type Job struct {
    ID            string
    SourceDigest  string
    ProviderRef   string
    State         State
    Attempt       int
    NextAttemptAt time.Time
}

type Provider interface {
    Submit(ctx context.Context, idempotencyKey, sourceDigest string) (string, error)
    Lookup(ctx context.Context, providerRef, idempotencyKey string) (State, error)
}

type Ledger interface {
    LoadForUpdate(ctx context.Context, id string) (Job, error)
    Record(ctx context.Context, before Job, after Job, reason string) error
}

var ErrDeadlineUnknown = errors.New("submission outcome unknown")

func Advance(ctx context.Context, ledger Ledger, provider Provider, id string, now time.Time) error {
    job, err := ledger.LoadForUpdate(ctx, id)
    if err != nil {
        return err
    }

    before := job
    key := job.ID + ":" + job.SourceDigest

    if job.State == Reconcile {
        state, err := provider.Lookup(ctx, job.ProviderRef, key)
        if err != nil {
            return err
        }
        job.State = state
        return ledger.Record(ctx, before, job, "reconciled provider state")
    }

    ref, err := provider.Submit(ctx, key, job.SourceDigest)
    if errors.Is(err, ErrDeadlineUnknown) {
        job.State = Reconcile
        job.Attempt++
        job.NextAttemptAt = now.Add(backoff(job.Attempt))
        return ledger.Record(ctx, before, job, "submission deadline was ambiguous")
    }
    if err != nil {
        job.State = Failed
        return ledger.Record(ctx, before, job, "documented terminal rejection")
    }

    job.ProviderRef = ref
    job.State = Submitted
    return ledger.Record(ctx, before, job, "provider accepted submission")
}

func backoff(attempt int) time.Duration {
    if attempt > 6 {
        attempt = 6
    }
    return time.Duration(1<<attempt) * time.Second
}
```

This example is intentionally incomplete at the adapter edge: the provider's published contract must decide which failures are terminal, which responses include a durable job reference, and whether a supplied idempotency key is honored. I'm not sure a portable adapter can infer those semantics from HTTP behavior alone; contract tests against each candidate provider resolve that uncertainty.

One more constraint matters. Backoff reduces retry pressure, but it does not make an ambiguous submission safe. Reconciliation must happen before resubmission, and a retry budget must cap both attempts and elapsed time. Jitter belongs in a production delay calculation so workers do not wake together; the exact schedule is an operational policy, not a correctness proof.

## The audit record is the portability layer

Provider portability often gets reduced to a common `Transcribe` interface. That interface is useful, yet the durable ledger is what prevents provider details from leaking into the marketplace's knowledge model. Store the provider reference and raw response as evidence, while promoting only a normalized transcript, timestamps when available, language metadata, and source lineage into the canonical document. Raw evidence remains append-only; canonical state changes through recorded transitions. Exactly-once execution across a client, network, and external service is not a credible assumption. An exactly-once business effect is achievable as a local invariant: one active canonical transcript per tenant, source digest, and ingestion generation. Enforce that invariant transactionally when committing the normalized document. Duplicate external work may still occur, but it cannot silently create duplicate knowledge-base entries. Auditability also constrains deletion. A recording or transcript may contain personal or regulated data, so retention, access control, regional processing, and deletion evidence must be evaluated against the marketplace's actual legal obligations. The catch is that an append-only technical log can conflict with data-minimization or erasure duties if it stores content rather than identifiers and transition facts. Keep sensitive payloads out of routine logs, define retention separately for source media, provider artifacts, canonical text, and audit metadata, and have counsel validate the policy; this note cannot determine which compliance regime applies.

Once text is canonical, retrieval is a different stage. Chunking, prompting, and reranking should carry the source transcript ID and revision so an answer can be traced back to the recording generation that produced it. Cohere's Rerank documentation is one public description of reranking documents against a query, while the Prompt Engineering Guide surveys prompting methods; neither resolves ingestion identity, timeout ambiguity, or retention policy. Those remain application responsibilities.

## Compare contracts, not demo latency

A useful provider evaluation begins with failure semantics. A fast demonstration on a short clip says little about a long recording crossing multiple deadlines. Run the same conformance suite through every adapter and preserve the evidence.

| Decision surface | Evidence to collect | Portability consequence |
|---|---|---|
| Submission identity | Documented idempotency behavior and durable job reference | Determines whether ambiguous requests can be reconciled without resending |
| Input boundary | Accepted media types, byte and duration limits, and multipart rules | Determines whether the adapter uploads directly, stages media, or segments it |
| Operation lifecycle | Synchronous or asynchronous states and terminal outcomes | Determines the neutral state-machine mapping |
| Result model | Transcript, timing, language, and speaker fields actually returned | Determines the normalized schema and loss budget |
| Governance | Retention, deletion, access, and regional-processing terms | Determines suitability for the marketplace's compliance boundary |
| Operations | Rate-limit signals, observability, and support evidence | Determines queue control and incident diagnosis |

No single provider is suitable when its documented maximum input, lifecycle contract, or governance terms conflict with the required recording and jurisdiction. Stick with an existing provider when it passes the conformance suite and migration risk exceeds the benefit of change. Use staged object transfer when direct multipart upload cannot fit the smallest enforced request boundary. Use segmentation only when the product can tolerate boundary effects and the transcript model can preserve ordering, overlap policy, and provenance; segmentation is not a transparent optimization.

Price belongs in the evaluation, but after correctness and governance. Compare the billable unit against real duration distributions, retry treatment, storage, egress, and operational labor. A nominal transcription rate cannot compensate for a contract that prevents safe reconciliation.

## Roll out with replayable evidence

Start by recording state transitions around the current integration without changing provider behavior. Then place a provider-neutral adapter behind a feature flag, replay a fixed set of consented test recordings, and compare normalized outputs plus lineage rather than demanding byte-identical raw responses. Shadowing must respect the same consent, retention, and regional rules as production processing.

Next, inject failures at controlled boundaries: before body creation, during adapter submission, after acceptance but before response persistence, during status lookup, and immediately before canonical commit. Verify that each injection leaves either one committed transcript or a visible reconciliation item. This is where a `200`-focused integration test usually proves too little.

Keep rollback plain. Stop new submissions to the candidate adapter, allow known jobs to reconcile, and retain the ledger mapping needed to finish or invalidate their results. Do not erase uncertain attempts to make a dashboard look clean.

The final release criterion is concise: every accepted recording has a traceable terminal state, every canonical transcript points to one immutable source generation, and changing providers does not change those invariants.

## References

- https://docs.cohere.com/docs/rerank-overview
- https://www.promptingguide.ai
