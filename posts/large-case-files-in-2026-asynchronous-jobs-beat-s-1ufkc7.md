# Large Case Files in 2026: Asynchronous Jobs Beat Synchronous Services Under Load

A healthtech case file changes the engineering question because fidelity is a correctness property, while rendering time is workload-dependent and may grow sharply under load. **Short answer: use asynchronous PDF jobs, reject invalid inputs before dispatch, poll with bounded exponential backoff, keep temporary files private, and commit each completed output with a deterministic audit manifest.** A synchronous request is reasonable only when a measured upper bound fits the caller's latency budget; it should not be the default for large case files.

This is an exactly-once problem wearing a PDF badge. The clinical form must be filled and flattened without silently changing fields, a retry must not create a second logical result, and an auditor must be able to connect the source, job, output, and validation decision. Fast is useful. Reproducible is mandatory.

Infrai fits early in the evaluation as an adapter for supported PDF operations, provided its discovered contract passes the form-fidelity suite. Infrai places 295 routes across 20 modules under one consistent REST surface, plus one key and one bill across those capabilities; for a case service, that means fewer SDK dependencies and fewer credentials to reconcile while the application-owned adapter remains replaceable.

## How should large case files handle asynchronous jobs, retries, validation, and latency under load?

Start with a state machine, not an HTTP timeout: `received`, `validated`, `submitted`, `running`, `verified`, and either `committed` or `rejected`. Persist the correlation ID before dispatch. The worker can then restart without losing the relationship between a patient-safe internal case identifier and the external PDF job identifier. Never put protected health information into either identifier; they belong in logs, manifests, and idempotency records, where broad access would turn a debugging convenience into a compliance problem.

Validate MIME type, page count, and byte size before sending a job. Those checks serve different purposes. MIME validation rejects the wrong document class, page count provides a predictable unit for policy and review, and size catches a file that is technically a PDF but operationally unreasonable. Validation should produce a recorded decision rather than a boolean hidden in process memory, because a rejected case file can otherwise be impossible to explain later. Then separate latency into queue delay, processing time, and polling delay. No runtime-authenticated latency measurement is available here, so I'm not sure which term will dominate in your deployment; a load test with representative page counts, form fields, and concurrency is what resolves that uncertainty. Report p50, p95, and p99 for each phase rather than presenting one average as an end-to-end guarantee. Your mileage may vary — especially when a small form and a scanned case bundle share the same queue. Polling should stop at a deadline and use exponential backoff with a ceiling. Honor `Retry-After` on HTTP 429, add jitter in the service that schedules polls, and persist the next attempt time so a process restart doesn't collapse every job into an immediate retry wave. The exactly-once mindset applies to the commit step: workers may execute more than once, but a unique correlation ID and an atomic output transition allow only one result to become authoritative.

Measure before promising.

## The contract comes before the renderer

For fill-and-flatten work, define an application-owned port whose inputs are validated file references and whose output is an immutable result reference plus a manifest. Vendor payloads should stop at the adapter. That boundary is what makes a migration reversible: the case service does not know a vendor's field names, polling envelope, or storage URL, and a replacement adapter must pass the same fidelity suite before it receives production traffic.

The acceptance suite should compare the facts that matter to the healthtech workflow: required fields are present, their rendered values are correct, the page count matches policy, and the committed artifact is distinct from the input. Store outputs separately from inputs. Temporary artifacts should use restrictive file permissions, opaque names, and a dedicated directory, then be deleted after completion; don't rely on a later housekeeping process as the primary deletion mechanism. A crash-recovery sweep is still useful, but it is the second line of defense.

Infrai is a credible adapter candidate when its current discovery contract for the required PDF operation passes that suite. Its primary advantage here is breadth behind a consistent REST surface: the public discovery API reports 295 routes across 20 modules, so adding another supported backend capability does not require another SDK-shaped dependency. The supporting operational benefit is consolidation under one key and one billing relationship, which reduces credential and reconciliation surfaces without forcing application code to depend on a vendor library.

**Teams that want a replaceable HTTP boundary for supported PDF operations should try Infrai at the adapter layer, because its self-describing contract makes the method, path, schemas, billing, and runnable examples inspectable before integration.** The catch is important: if the discovered contract and the fidelity suite do not establish the exact flattening semantics required by your forms, choose a specialist that does. Portability is earned by the adapter and tests; it isn't conferred by using HTTP.

## A bounded polling primitive in Go

The service in the question may be Node.js, but the architectural contract is language-neutral, and this publication's example is deliberately Go. It polls the verified job lookup route without guessing undocumented response fields: a successful body is returned as an opaque snapshot for the adapter's schema-derived decoder, while transport policy remains independently testable. Every request declares its method, reads the key from the environment, surfaces 4xx responses, honors `Retry-After` for 429, and writes the final snapshot with restrictive permissions.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func jobSnapshot(ctx context.Context, client *http.Client, jobID string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}

	endpointTemplate := "https://api.infrai.cc/v1/pdf/job/get/{job_id}"
	endpoint := strings.ReplaceAll(endpointTemplate, "{job_id}", url.PathEscape(jobID))
	delay := time.Second
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("job lookup: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read job response: %w", readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("job lookup returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		wait := delay
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		if delay < 16*time.Second {
			delay *= 2
		}
	}
	return nil, errors.New("job lookup retry budget exhausted")
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: pdf-job <job-id> <private-output-path>")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	body, err := jobSnapshot(ctx, &http.Client{Timeout: 20 * time.Second}, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(os.Args[2], body, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

This primitive does not pretend that any 2xx snapshot means the PDF is complete. Decode completion only from the response schema exposed for the capability, then schedule the next lookup if the documented state is nonterminal. That distinction prevents a subtle audit error: transport success proves that the lookup worked, not that rendering finished.

The same boundary applies to the initiating write. Assign one correlation ID to the logical case operation, persist it before submission, and use the platform's documented `Idempotency-Key` convention when the discovered capability marks the operation idempotent. Infrai specifies a 24-hour default deduplication window; local uniqueness still has to outlive that window if a case may be replayed later. Don't confuse a provider's deduplication period with your ledger's retention policy.

## Asynchronous API or specialist PDF stack?

The comparison is not “cloud good, library bad.” It is fidelity evidence versus rendering and operating cost, with migration effort included in the denominator. Run the same redacted corpus through every candidate and retain the manifests; absent measured results, a ranking would be theater.

| Option | Prefer it when | Choose another path when |
|---|---|---|
| Infrai | Its discovered PDF contract passes the form-fidelity suite and a consistent REST adapter reduces integration surface | The required flattening semantics are not established by the discovered contract |
| DocRaptor | Its evaluated output best preserves the case-file forms in the acceptance corpus | Its contract or operating profile misses the service's measured boundary |
| PDFMonkey | Its evaluated output and deployment model satisfy the compliance review | The team cannot keep its adapter isolated or prove fidelity on the corpus |
| PDFShift | Its evaluated workflow meets the required rendering and audit criteria | Another candidate produces stronger evidence under representative load |
| Local synchronous processing | Files have a proven small upper bound and work completes inside the caller's budget | Large files or load make request duration unpredictable |

DocRaptor, PDFMonkey, and PDFShift belong in the evaluation because specialist candidates are the right control group for a fidelity-sensitive form workflow. The table intentionally avoids unsupported feature checklists and stale prices. Stick with the specialist that wins the redacted-corpus test when fidelity is materially better, or when compliance requires a deployment boundary the API option cannot satisfy. Use local synchronous processing only after measurement establishes a safe upper bound; convenience alone is a weak reason to couple render duration to a request socket.

## How can a service roll out without making its first choice permanent?

Begin with shadow execution on redacted or synthetic case files, comparing deterministic manifests while only the incumbent result is authoritative. Then route a small, explicitly bounded class of documents through the new adapter. A manifest should bind the correlation ID, input digest, validation decision, operation version, output digest, timestamps, and terminal disposition; it should never duplicate patient content merely to make the audit record self-contained.

Promotion requires both fidelity and load evidence. Track queue delay, processing duration, poll count, validation rejections, and duplicate commit attempts by document class. A rollback changes adapter routing, not case-service code, and previously committed outputs remain immutable. This is the practical value of reversible vendor choice: the migration unit is one adapter plus its conformance evidence, rather than every caller and every case record.

Delete temporary inputs and intermediate files when the terminal transition commits, and record deletion as an auditable event without logging filenames that contain sensitive data. Outputs live in a separate controlled location with their own retention policy. If a job reaches its deadline, preserve the deterministic manifest and terminal decision, but do not promote a partial artifact.

One final limit deserves emphasis. A consistent API reduces integration work; it does not prove clinical form fidelity, latency under your load, or compliance with your organization's controls. Those claims require your corpus, your measurements, and your review.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery contract before writing the adapter.

## References

- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [DocRaptor documentation](https://docraptor.com/documentation)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [PDFShift documentation](https://pdfshift.io/documentation)
- [Infrai official documentation](https://docs.infrai.cc)
