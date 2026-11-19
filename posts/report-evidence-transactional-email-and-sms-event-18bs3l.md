# Report Evidence: Transactional Email and SMS Event Notifications API Explained for 2026

Distributing a generated logistics report as an email attachment puts convenience in tension with compliance evidence: a dispatch record needs to explain which report was sent, while an engagement signal can leave the recipient's actions unresolved. TL;DR: compare transactional email and SMS notification APIs by the claims their evidence can support. Use email for the attachment in this design, give an optional SMS notice its own record, and require a separate acknowledgment if the workflow needs an explicit recipient assertion. A quoted request price cannot establish any of those properties.

The useful comparison for a US/EU backend begins with the report's eventual review. Imagine an illustrative shipment `LOT-204`, report revision `3`, and a consignee who disputes receiving the correct attachment. The question is which records the team could produce without overstating what they prove. This note proposes an evidence review method; the example identifiers are test fixtures, not observations from a deployed system.

## How should we compare email and SMS providers for transactional event notifications?

Work backward from the assertion a reviewer needs. “The dispatcher submitted revision 3,” “the message passed an authentication check,” and “the consignee acknowledged the report” are different assertions, with different supporting records and different opportunities for uncertainty. Treating all three as `delivered` makes a dashboard tidy while making reconciliation harder. An exactly-once mindset is useful here because it demands a stable business identity for the intended action, even when the external observations arrive repeatedly or remain incomplete.

Keep claims narrow.

RFC 7489 describes DMARC through identifier alignment and the use of SPF and DKIM results. Its authentication machinery does not establish that the contents of a shipment report are accurate, nor does it establish that a recipient reviewed an attachment. Preserve an authentication result for the question it answers; do not promote it into evidence of a business acknowledgment.

Open tracking has another boundary. Apple's Mail Privacy Protection guide explains that the feature prevents senders from seeing whether a recipient opened an email. An application therefore cannot make open telemetry its sole proof of report review. Don't close a disputed dispatch merely because an open signal exists, and don't infer nonreceipt merely because one is absent.

For this proposed workflow, distinguish 4 evidence classes: artifact identity, submission observation, channel observation, and authenticated acknowledgment. These are application categories, not standardized provider statuses. The acknowledgment should state its own scope: confirming access to a particular revision is a narrower assertion than accepting the accuracy of the shipment data. Have the responsible compliance reviewer approve that wording and the evidence it requires before implementing a button that appears to settle the dispute.

Technical traceability alone cannot establish legal sufficiency across US and EU jurisdictions. This design makes no universal compliance claim.

## Build a review packet around the attachment

The proposed unit of review is a packet associated with one report revision and one resolved recipient. Record a protected reference to the retained attachment, its digest, the recipient snapshot, the dispatch intent, and subsequent observations. The digest helps compare candidate bytes against the retained artifact; it is not a substitute for an author's signature or an independently established creation time. A reviewer needs access to the relevant retained material, subject to the approved access and retention policy, to investigate a mismatch.

The most consequential fixture is a regenerated report. Queue revision `3`, then generate revision `4` before the worker processes the queue. If the worker fetches “latest,” the ledger can identify one revision while the outgoing attachment contains another. The proposed rule is to bind the intent to retained bytes before dispatch and require a new intent for a new revision. Recipient changes deserve the same treatment: substituting the consignee's new address inside an old attempt would obscure which destination that attempt actually used. This costs artifact storage and additional review logic because reconstructing a report from current shipment data cannot demonstrate what an earlier submission contained.

That is the trade-off.

Use an audit trail with separate entries for observations and decisions. An operator's reconciliation decision should identify the evidence consulted and the remaining uncertainty, rather than replace a historical observation with a more convenient status. Retention is an explicit constraint: ask the compliance owner which attachment records, recipient data, and acknowledgment records may be retained, for which purpose, and for how long. Routine logs should use protected correlation references instead of copying complete reports or addresses into every event.

For idempotency, define the business key using shipment, revision, recipient identity, and channel; enforce its uniqueness at the durable intent boundary. A duplicate enqueue should find the existing intent. An uncertain remote submission needs its own policy, however, because local uniqueness cannot establish whether an external side effect occurred. Evaluate documented recovery and idempotency semantics before deciding when a worker may retry. A short delay and a duplicate report are different business outcomes, and the appropriate choice depends on the report's purpose.

## Make unsupported conclusions hard to encode

The following Go example isolates the review boundary. It deliberately returns the claims supported by a packet instead of compressing them into one success flag. The caller supplies results from its artifact comparison, recorded submission response, and authenticated acknowledgment process; this function does not validate a callback or authenticate a person.

```go
package evidence

import "errors"

type Packet struct {
    ShipmentID          string
    Revision            uint64
    ArtifactMatches     bool
    SubmissionRecorded  bool
    AcknowledgedRevision uint64
    OpenObserved        bool
}

type Claims struct {
    ArtifactIdentified   bool
    SubmissionObserved   bool
    RevisionAcknowledged bool
}

func Review(p Packet) (Claims, error) {
    if p.ShipmentID == "" || p.Revision == 0 {
        return Claims{}, errors.New("missing report identity")
    }
    return Claims{
        ArtifactIdentified:   p.ArtifactMatches,
        SubmissionObserved:   p.SubmissionRecorded,
        RevisionAcknowledged: p.AcknowledgedRevision == p.Revision,
    }, nil
}
```

`OpenObserved` cannot change any of these conclusions. Its presence illustrates the boundary: collecting a signal does not authorize using it to support an unrelated assertion. An acknowledgment for revision `3` also cannot acknowledge revision `4`. The surrounding implementation must bind the acknowledgment to the intended recipient and retained artifact; a revision number supplied by an unauthenticated client would not satisfy that contract.

This is a projection, not an evidence store. Persist the underlying observations and their provenance so a reviewer can recompute it after a classification rule changes. Keep that rule version with exported review results. Otherwise a later code deployment could change the meaning of an old packet without leaving an explanation in the audit trail. A team migrating a NodeJS worker to Go should preserve that evidence contract across the language change.

## Compare API contracts after defining the claims

For a 2026 review, a transactional notification API comparison has a concrete acceptance criterion: can the team assemble and explain this packet for its email attachment and its optional SMS notice? The table compares channel roles in the proposed system. It does not assume that any commercial service supplies a particular feature or that two similarly named statuses have identical meanings.

| Review question | Email attachment role | SMS notice role |
| --- | --- | --- |
| What action is intended? | Submit the retained report to the resolved recipient | Announce that the identified report is available |
| What must the adapter preserve? | Attachment identity, destination, attempt identity, and documented submission observations | Destination, related report revision, attempt identity, and documented submission observations |
| What remains a separate assertion? | Recipient acknowledgment of the particular revision | Recipient acknowledgment of the particular revision |
| What requires candidate-specific evidence? | Attachment restrictions, event definitions, callback authentication, retention, and recovery semantics | Destination coverage, event definitions, callback authentication, retention, and recovery semantics |

Ask each candidate for the same evidence, including the boundary between accepting a request and whatever later status its documentation describes. Require a documented way to correlate an observation with an attempt, an explanation of callback verification, and the available behavior after a submission result becomes uncertain. If a contract leaves one of these questions unanswered, record that gap in the comparison rather than inventing a guarantee. No vendor ranking follows from an undocumented assumption.

If a procurement shortlist includes SendGrid, Postmark, and Mailgun, request an independent attachment-and-evidence review for each. Those names identify possible review subjects here, not verified feature differences: this note makes no claim about their respective attachment limits, retention periods, or recovery guarantees. Those boundaries need attributable documentation before a commercial comparison can be completed.

For US/EU use, include processing locations, subprocessors, access arrangements, and retention commitments in the contractual review. An endpoint location is only one input. The responsible reviewers must determine which legal obligations apply to the report and its recipients; an API's technical feature list cannot make that determination.

Price belongs alongside these constraints. Estimate total operating effort using the proposed attachment workload, SMS notice volume, retained evidence, and manual reconciliation burden. Adding a second adapter can broaden operational choices, but it also adds another event mapping, verification mechanism, and contract that the team must maintain. Compare that work explicitly. There is no defensible cheapest option without a workload and an acceptable evidence boundary.

## Roll out through a small evidence rehearsal

Start with the illustrative shipment and controlled recipients. Rehearse a repeated enqueue, a regenerated attachment, an acknowledgment for the wrong revision, an open observation without acknowledgment, and an uncertain submission. Require the review packet to preserve each distinction. Include a test that unauthorized acknowledgment input cannot become a trusted assertion; testing only the projection above would miss that boundary.

During migration, assign one active dispatcher to each intent and reconcile outstanding work before transferring ownership. Release a limited scope first, monitor the age of unresolved packets and reconciliation decisions, and keep an operator path for investigating uncertainty. Expand when the team can reconstruct an attachment's history within the approved retention policy. The release criterion is explainable evidence for the logistics report, with every claim bounded by what the records actually support.

## Sources

References:

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Apple, Use Mail Privacy Protection on iPhone: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
