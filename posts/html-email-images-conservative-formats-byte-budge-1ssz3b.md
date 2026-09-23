# HTML Email Images — Conservative Formats, Byte Budgets, and Moderation Gates

For images sent through an HTML email API, the conservative approach is to use JPEG for photographic marketplace thumbnails and PNG when transparency or sharp-edged artwork is essential, then enforce an explicit byte-size budget after transformation rather than trusting the uploaded file. This rule applies even when a Node.js service orchestrates the workflow rather than processing pixels itself. The more important architectural decision is less visible: an image must not become eligible for email until the exact derived bytes have passed moderation, been recorded in an immutable manifest, and can be served by a stable HTTPS URL.

TL;DR: Treat email imagery as a publishable artifact, not as a side effect of upload. Decode and normalize the source, generate bounded derivatives, moderate every derivative that can leave the system, and promote a manifest with a deterministic idempotency key. This favors broad, conservative consumption over format novelty and makes retries auditable without pretending that message delivery and media publication form one atomic transaction.

## How should a Node.js API prepare images for HTML email?

The primary invariant is straightforward: **no message may reference image bytes that moderation has not approved**. Approval of the original upload is insufficient because resizing, cropping, orientation correction, or animation handling can change what a recipient sees. The moderation subject therefore has to be identified by a digest of the final bytes, not merely by a marketplace listing ID or a source-object key.

Three records define the boundary: an immutable source record, a derivative record keyed by transformation policy and content digest, and a publication manifest that names only approved derivatives. The manifest is the handoff to the mail renderer. It should carry dimensions, media type, byte length, digest, moderation decision identifier, and policy version; it should never carry a mutable "latest thumbnail" pointer whose target can change after template rendering.

This is an exactly-once *effect* problem implemented on at-least-once machinery. Upload notifications can repeat, workers can time out after storing an object, and moderation responses can arrive after a listing has been edited. A unique key over source digest plus transformation-policy version makes repeated work converge, while compare-and-swap publication prevents an old approval from replacing a newer manifest. Keep both the attempted transition and the accepted transition in the audit trail. Quiet retries are still state changes worth reconciling.

Consider one concrete race in an API approach: upload A starts a thumbnail job, the seller replaces it with upload B, and approval for A arrives last. If the callback updates a listing by ID alone, the HTML email can expose a stale image even though both moderation calls behaved correctly. Binding the callback to the source digest, derivative digest, policy version, and expected listing version turns that vague timing problem into a rejected stale transition; the audit record can then show the attempted approval without allowing it to alter the current manifest. Node.js is entirely suitable for coordinating those records and queues, but language choice does not repair a missing version check.

Failure must be closed. If decoding fails, dimensions exceed the local intake policy, a derivative misses its byte ceiling, or moderation is absent or indeterminate, the email renderer receives a text-and-layout fallback rather than an unreviewed image. No image is safer than the wrong image.

Bytes are the unit.

## Decision record: format and byte policy

MDN distinguishes raster formats and documents characteristics such as compression and transparency; that taxonomy is useful input, but it is not an email delivery policy. A conservative policy deliberately accepts fewer output choices than a general-purpose browser might decode. The exact byte ceilings belong in a versioned local policy because template geometry, recipient mix, and total message payload differ; presenting one universal number would create false precision.

| Option | Appropriate content | Moderation consequence | Decision |
|---|---|---|---|
| JPEG derivative | Photographs and other continuous-tone listing imagery | Moderate the final compressed bytes and record the encoder policy | Default for photographic thumbnails |
| PNG derivative | Transparency, logos, diagrams, and sharp-edged artwork | Enforce a strict byte ceiling because lossless output may be large | Allowed by explicit content class |
| GIF derivative | A workflow with a documented need for animation | Every displayed frame expands the review surface | Reject by default; allow only under a separate policy |
| Newer raster derivative | Controlled audiences whose client evidence is maintained | Requires a measured compatibility matrix and fallback lifecycle | Keep outside the conservative baseline |

The byte rule has two layers. Each derivative must fit its slot-specific ceiling, and the assembled message must fit a separate aggregate image budget. Store both limits in the policy version recorded beside the artifact. This prevents a harmless-looking template change, such as adding a fourth recommendation tile, from bypassing the intent of the per-image check.

The trade-off is deliberate: conservative output gives up some compression opportunities and may require maintaining more than one derivative, while an aggressive format policy can reduce image size for a measured audience at the cost of a larger compatibility and fallback test surface. Teams with a closed recipient population and continuously tested clients may reasonably choose the latter. This baseline is unsuitable when animation itself carries required information, when recipients must work fully offline, or when the system cannot provide stable external asset URLs; those cases need a separately reviewed delivery model rather than a relaxed version of this one.

Avoid using filename extensions as evidence. The processor should decode the input, reject malformed data, remove metadata that is not required for rendering, apply orientation before cropping, and encode a fresh output whose declared media type matches the produced bytes. Those are pipeline requirements, not claims that any one numeric limit is universally correct.

## Critical path in Go

The critical path should make publication conditional and replayable. This abbreviated Go example leaves codecs, storage, and moderation behind interfaces, but keeps the ordering rule explicit. A production implementation should persist state transitions and the outbox event in one database transaction.

```go
package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

type Policy struct {
	Version       string
	MaxImageBytes int
	Format        string
}

type Artifact struct {
	URL       string
	MediaType string
	Bytes     int
	Digest    string
}

type Services interface {
	LoadSource(context.Context, string) ([]byte, error)
	Transform(context.Context, []byte, Policy) ([]byte, string, error)
	Moderate(context.Context, []byte, string) (string, bool, error)
	PutImmutable(context.Context, string, []byte, string) (string, error)
	PublishIfCurrent(context.Context, string, Artifact, string) error
	AppendAudit(context.Context, string, string, string) error
}

func BuildEmailThumbnail(ctx context.Context, svc Services, listingID, sourceDigest string, p Policy) error {
	keyBytes := sha256.Sum256([]byte(sourceDigest + "\x00" + p.Version))
	idempotencyKey := hex.EncodeToString(keyBytes[:])

	source, err := svc.LoadSource(ctx, sourceDigest)
	if err != nil {
		return fmt.Errorf("load immutable source: %w", err)
	}

	derived, mediaType, err := svc.Transform(ctx, source, p)
	if err != nil {
		return fmt.Errorf("normalize and encode: %w", err)
	}
	if len(derived) == 0 || len(derived) > p.MaxImageBytes {
		return errors.New("derived image violates byte policy")
	}

	decisionID, approved, err := svc.Moderate(ctx, derived, mediaType)
	if err != nil || !approved {
		_ = svc.AppendAudit(ctx, idempotencyKey, "publication_denied", decisionID)
		if err != nil {
			return fmt.Errorf("moderation unavailable: %w", err)
		}
		return errors.New("moderation denied derivative")
	}

	digestBytes := sha256.Sum256(derived)
	digest := hex.EncodeToString(digestBytes[:])
	url, err := svc.PutImmutable(ctx, digest, derived, mediaType)
	if err != nil {
		return fmt.Errorf("store approved derivative: %w", err)
	}

	artifact := Artifact{URL: url, MediaType: mediaType, Bytes: len(derived), Digest: digest}
	if err := svc.PublishIfCurrent(ctx, listingID, artifact, decisionID); err != nil {
		return fmt.Errorf("publish manifest: %w", err)
	}
	return svc.AppendAudit(ctx, idempotencyKey, "publication_approved", decisionID)
}
```

The example intentionally moderates before immutable storage promotion and binds the decision to the derived byte sequence. In a system where moderation reads from object storage, the equivalent design uses a quarantined namespace and makes only an approved manifest public. The namespace is implementation detail; the invariant is not.

There is also a subtle audit defect in treating `AppendAudit` as best-effort, as the denial branch does for brevity. A real workflow should make the durable decision record authoritative and derive operational events from an outbox. Otherwise an outage can leave the system correctly closed but unable to prove why it refused publication, which is weak evidence for policy review and reconciliation.

That limitation matters.

## Operations, tests, and compliance boundaries

Test the state machine, not just the resizer. Replay the same upload event twice and verify one published digest. Deliver an approval after the source version has changed and verify no promotion. Fail after object creation but before manifest publication, then retry and verify that the same digest is reused. Supply an oversized derivative, a decode failure, and an indeterminate moderation response; each must produce the same safe rendering outcome while retaining a distinct audit reason.

Observability should separate processing health from policy outcomes. Track decode failures, transformation failures, byte-budget rejections, moderation denials, indeterminate decisions, stale approvals, and manifest conflicts as different counters. A single "image failed" metric hides whether capacity, corrupt input, or a policy boundary caused the result. Log digests and decision identifiers, but avoid retaining source URLs or user-provided filenames when they are unnecessary for diagnosis.

Compliance limits deserve an explicit statement: the audit log proves which policy and moderation decision authorized a given digest; it does not prove that the moderation policy was legally sufficient, nor does it grant a right to retain the source indefinitely. Retention, deletion, access control, and regional handling should be separate policy inputs with their own evidence. When a listing is removed, the publication manifest must cease resolving through the mail asset layer according to the documented lifecycle, even if immutable storage is retained temporarily under another authorized rule.

Roll out policy changes by version. Shadow-generate new derivatives without publishing them, compare rejection categories and byte distributions, and promote only after the moderation and rendering evidence is complete. Preserve the old policy long enough to reproduce active manifests; reproducibility is part of incident analysis.

## Rejected option and the case where it works

The rejected design is to embed the uploader's original image directly in the email template after a single source-level moderation decision. It minimizes processing and can be valid inside a tightly controlled system where every producer emits a pre-normalized, bounded artifact and the approved bytes are exactly the delivered bytes.

That is not the marketplace boundary described here. Untrusted uploads, responsive crops, and multiple template slots create multiple renderable artifacts, so source-only approval leaves a coverage gap and mutable URLs weaken later reconstruction. The conservative decision is therefore to publish byte-addressed derivatives through an approved manifest, keep JPEG and PNG as the baseline choices, treat GIF as a separate moderation policy, and make missing evidence resolve to the no-image fallback.

## References

- MDN Web Docs, "Image file type and format guide": https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
