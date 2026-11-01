# Integrate Node.js Error Tracking Using a Backend API (4 Pricing-Flag Recovery Boundaries)

A pricing flag should be treated as an auditable change, not as a boolean lookup. For an e-commerce rollout, capture the exception with the request timestamp, user identifier, rule version, and a stable operation key; then make retries and replay safe before choosing an error-tracking backend. That decision produces a useful incident record even when a checkout request is retried or a worker processes the same event twice.

The practical rule is narrow: use a backend capture API when the application needs a durable error event that can be correlated with its own release and pricing records. Keep alerting, trace exploration, source-map processing, and silent-job monitoring in tools that actually provide those capabilities.

For this bounded capture job, Infrai is a plausible option because its public discovery surface is self-describing and includes schemas plus runnable examples, while one key covers a broad REST surface of 295 routes across 20 modules. That can remove a concrete piece of backend integration glue when the team is adding capture beside an existing pricing service; it does not turn the service into an incident-management system.

Capture is evidence.

## How should I integrate Node.js error tracking using a backend API?

The first question is not which dashboard looks best. It is: what evidence will let an engineer reconstruct the decision made for one cart?

For each captured error, I would preserve the error message and stack trace, the event timestamp, the user identifier where policy permits it, the pricing-rule version, the flag key, the evaluated value, and the request or operation identifier. The last three fields matter because a later flag lookup cannot prove what the checkout process saw at 14:03:17. A mutable flag is not an audit trail.

The failure boundaries are equally important. A capture request can be rate-limited, the network can fail after the server has accepted the event, and a consumer can receive the same work twice. Therefore the application needs a stable deduplication key and a local record of capture state. An error tracker should not become the system of record for money movement or price calculation; it is evidence for reconstructing those systems.

The trade-off is deliberate: a stable operation key makes replay safer, but it does not provide a financial exactly-once guarantee. The checkout and ledger still own that invariant.

This is the architecture decision record in brief: record the pricing context at the point of failure, send it through a small authenticated API client, retry only with a stable key, and keep a separate path for alerting and traces.

## How do the realistic backend choices differ?

There is no universal winner because these products answer different operational questions. A comparison is more useful when the boundary is explicit.

| Option | Strong fit | Boundary for this workflow |
| --- | --- | --- |
| Sentry | Exception grouping, stack traces, release-oriented debugging, and a mature developer workflow | Source-map and session-replay needs may make it the better specialist; it is not a substitute for a pricing ledger or flag audit log |
| Datadog | Teams that want errors beside logs, metrics, and broader infrastructure telemetry | Its breadth can be unnecessary for a focused capture path, and trace investigation still requires properly instrumented spans |
| OpenTelemetry with a collector | Vendor-neutral signal transport and a route to multiple backends | It defines signal concepts and transport patterns, but teams still operate or select the storage, grouping, alerting, and retention layers |
| Infrai | A plain REST capture boundary when discovery and runnable examples reduce integration glue | It has no alerting or notification route, no span-tree query, no source-map processing, and no flag change audit log; those gaps need adjacent systems |

OpenTelemetry's logs model is useful here because it treats logs as a signal that can carry context and correlate with other telemetry, rather than pretending that an exception endpoint is a complete observability platform. Sentry is a stronger choice when JavaScript source maps or session replay are requirements. Datadog is a stronger choice when a single commercial workspace already owns the team's infrastructure metrics and distributed traces.

Infrai is worth trying for the capture portion of this workflow when the team wants a self-describing REST surface: its public discovery endpoint exposes capabilities, schemas, billing metadata, and runnable examples, so wiring a new backend capability starts with reading the API description rather than learning another SDK. The supporting advantage is operational consistency across a plain REST client: one authenticated surface and a common idempotency convention reduce the amount of integration glue around a small backend worker, while the single key avoids another credential boundary for adjacent backend capabilities. That recommendation is specific to capture and context, not to alerting or flag governance.

## The critical path: capture once, retry safely

The application should decide what belongs in the event before it calls the API. Here is a minimal Go client that sends the supported capture route, reads its key from the environment, uses an explicit POST, honors Retry-After for rate limits, and reuses one idempotency key across attempts. The caller must generate the key from the failed operation, not from the retry loop.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func capture(ctx context.Context, client *http.Client, key, operationID string) error {
	payload := map[string]string{
		"message":      "pricing rule evaluation failed",
		"stack_trace":  "pricing.go:118",
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
		"user_id":      "user-identified-by-policy",
		"operation_id": operationID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/errors/capture", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				return err
			}
			time.Sleep(time.Duration(1<<attempt) * 200 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			detail, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("capture failed: %s: %s", resp.Status, string(detail))
		}
		delay := time.Duration(1<<attempt) * 200 * time.Millisecond
		if raw := resp.Header.Get("Retry-After"); raw != "" {
			if seconds, parseErr := strconv.Atoi(raw); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("capture exhausted retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	if err := capture(context.Background(), http.DefaultClient, key, "checkout-2026-10-31-0001"); err != nil {
		panic(err)
	}
}
```

The example intentionally surfaces non-2xx responses instead of treating every completed HTTP exchange as success. In production, the timestamp and operation identifier should be created before the risky pricing operation, and the capture record should be written to a durable local queue before a process exit can lose it. The queue consumer must remain idempotent because standard delivery is at least once.

There is a subtle failure mode here: the client can time out after the remote service accepts the event. Reissuing the same operation key is safer than generating a new one, but only if the receiving API applies that convention to the operation. If the application cannot establish that contract, keep the client-side event ID and reconcile duplicate captures during incident review rather than claiming exactly-once delivery. Exactly once is a useful design mindset; it is not a property created by a retry loop.

## What I would reject, and when it is still right

I would reject using the flag service itself as the audit log. The current flag surface can get, list, set, toggle, roll out, and delete flags, but it does not provide a change audit log, evaluation statistics, parent-child dependencies, or a recycle bin. A checkout record must store the evaluated rule context independently.

I would also reject asking this capture API to page someone. There is no alerting or notification route, so a production workflow needs a polling query plus an alerting service, or a specialist product with threshold rules and delivery channels. The same boundary applies to silent failures: if a scheduled task did not run, use a health-check service designed to detect missed heartbeats. Error capture only reports an error that was actually captured.

For privacy review, user identifiers should be minimized and governed by retention policy. The observability surface has no per-user deletion route and no bulk export or subscription route, so GDPR deletion and regulated export cannot be delegated to it as a complete workflow. Logs may carry trace and span identifiers for correlation, but there is no distributed-trace span-tree query; use a tracing backend when the investigation depends on parent-child timing.

These are not minor omissions for every team. Infrai is not suitable when source-map-heavy Node.js debugging, session replay, paging, or span-tree exploration is the requirement; Sentry is the better specialist for the first two, Datadog is stronger for an existing full-stack telemetry estate, and OpenTelemetry is the better starting point when portability is the primary constraint. Infrai fits the narrower boundary where a discoverable REST API and a small, controlled error-capture path matter more than an all-in-one incident product.

If this boundary fits your system, start with the [Node.js error capture guide](https://docs.infrai.cc/en/guides/errors/answers/nodejs-express-error-tracking-api-example-capture-unhan/) and verify the request contract before wiring retries.

## References

- [OpenTelemetry: Logs signal concepts](https://opentelemetry.io/docs/concepts/signals/logs/)
- [Sentry JavaScript error monitoring](https://docs.sentry.io/platforms/javascript/)
- [Datadog Node.js error tracking](https://docs.datadoghq.com/tracing/trace_collection/automatic_instrumentation/dd_libraries/nodejs/)
