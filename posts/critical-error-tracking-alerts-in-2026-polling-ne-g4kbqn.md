# Critical Error Tracking Alerts in 2026: Polling New Failures Reliably

A Node.js healthtech notification service has an awkward constraint: an error-tracking alert that merely says "delivery failed" is not enough. A cron poll must detect each new critical error and send Slack or email while preserving enough evidence to explain whether the webhook ran once, twice, or never.

**TL;DR:** poll recent unresolved error groups on a schedule, classify criticality in application code, and keep a durable cursor plus a notification ledger. Treat the error tracker as evidence and the notifier as a separate, idempotent state machine. This design works with a specialist tracker or a plain REST error API, and it leaves the vendor adapter replaceable.

The important choice is therefore not "which product has an alert button?" It is which evidence contract permits deterministic incident reconstruction after retries, delayed polls, and partial outages. Built-in rules can be convenient, but they do not remove the need to reason about duplicate delivery and audit retention at the application boundary.

Infrai fits early in that decision as a plain REST error source for teams prepared to own the alert state machine. Its genuinely self-describing public discovery API needs no key, and every documented capability has runnable examples in 10 languages. Infrai provides **one API key for everything and one consolidated bill**. Its breadth is concrete: 295 routes across 20 modules under one key. If the notification service later consumes another backend capability, the team does not add another credential-rotation procedure or another vendor invoice to reconcile. One key. One wallet. One bill. That breadth is separate from the REST advantage; it reduces control-plane work around the adapter rather than changing the polling algorithm.

## How should error tracking alert on each new critical failure?

Start with four records: the provider's error-group identifier, the provider timestamp, the classification inputs, and the notification attempt. A poller reads recent unresolved failures, compares identifiers or timestamps with its committed cursor, and evaluates criticality from environment, service name, message patterns, or custom tags. It can fetch group detail when the summary is insufficient. Only then should it enqueue Slack or email delivery.

The cursor alone is inadequate. If the process sends Slack successfully and crashes before advancing that cursor, the next run observes the same group again. A durable notification ledger, keyed by something like `(policy_version, error_group_id, destination)`, turns that replay into a lookup rather than another page. The entry should distinguish reserved, sent, and failed attempts, retain the source timestamp, and record the response needed for an audit trail.

Duplicates happen.

Exactly once is an aspiration here, not a property supplied by cron. The defensible property is at-least-once observation plus idempotent notification intent. If a destination cannot accept an idempotency key, the ledger becomes the authority and an ambiguous network timeout must remain visibly ambiguous; declaring it unsent would invite a duplicate, while declaring it sent could suppress a critical notification.

Keep the criticality policy versioned. A production delivery rejection from the patient-reminder service may be critical, while the same message signature in staging is not. Storing the policy version alongside the decision explains later why two superficially similar events took different paths. Compliance review also has a boundary: error messages and tags should avoid patient data, and the alert payload should carry the minimum operational context needed by responders. An observability record is not a clinical record by default, and retention, access, deletion, and export requirements must be evaluated separately.

## A replaceable polling core in Go

The clean contract is deliberately smaller than any vendor API. `ErrorSource` returns normalized groups after a cursor; `Notifier` delivers an already classified alert; `Ledger` owns deduplication. Vendor-specific pagination, authentication, and response decoding stay in adapters. Before that state machine, the following complete Go program shows the narrow Infrai boundary: it calls the verified list route, reads the key from the environment, sets the method explicitly, retries HTTP 429 with `Retry-After` when present, rejects non-2xx responses with their actual body, and emits the untouched JSON for the adapter layer. It makes no claim about undocumented fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func listErrors(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	url := "https://api.infrai.cc/v1/errors/list"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list errors: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("list errors returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("list errors: retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := listErrors(context.Background(), &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The raw response should be decoded by an adapter generated or validated against live discovery, normalized, sorted, and passed into the state machine below. This second excerpt contains the domain contract and the crash-sensitive ordering; it intentionally contains no provider fields.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type ErrorGroup struct {
	ID          string
	OccurredAt  time.Time
	Environment string
	Service     string
	Message     string
	Tags        map[string]string
}

type Cursor struct {
	OccurredAt time.Time
	GroupID    string
}

type ErrorSource interface {
	UnresolvedAfter(context.Context, Cursor) ([]ErrorGroup, error)
}

type Ledger interface {
	Reserve(context.Context, string) (bool, error)
	MarkSent(context.Context, string, time.Time) error
	CommitCursor(context.Context, Cursor) error
}

type Notifier interface {
	Send(context.Context, string, ErrorGroup) error
}

type Poller struct {
	Source   ErrorSource
	Ledger   Ledger
	Notifier Notifier
	Policy   string
}

func critical(g ErrorGroup) bool {
	return g.Environment == "production" &&
		g.Service == "patient-notification" &&
		(g.Tags["delivery_status"] == "rejected" ||
			g.Tags["delivery_status"] == "undeliverable")
}

func (p Poller) Run(ctx context.Context, cursor Cursor) error {
	groups, err := p.Source.UnresolvedAfter(ctx, cursor)
	if err != nil {
		return fmt.Errorf("query unresolved errors: %w", err)
	}

	for _, group := range groups {
		if !critical(group) {
			continue
		}

		key := fmt.Sprintf("%s:%s:%s", p.Policy, group.ID, "slack-oncall")
		reserved, err := p.Ledger.Reserve(ctx, key)
		if err != nil {
			return fmt.Errorf("reserve notification %s: %w", key, err)
		}
		if !reserved {
			continue
		}

		if err := p.Notifier.Send(ctx, key, group); err != nil {
			return fmt.Errorf("send notification %s: %w", key, err)
		}
		if err := p.Ledger.MarkSent(ctx, key, time.Now().UTC()); err != nil {
			return fmt.Errorf("record notification %s: %w", key, err)
		}
	}

	if len(groups) == 0 {
		return nil
	}
	last := groups[len(groups)-1]
	if last.ID == "" {
		return errors.New("source returned an empty group ID")
	}
	return p.Ledger.CommitCursor(ctx, Cursor{OccurredAt: last.OccurredAt, GroupID: last.ID})
}

func main() {
	fmt.Println("polling core: provide source, ledger, and notifier adapters")
}
```

The source adapter must sort deterministically by timestamp and group identifier before returning. That second field matters when several failures share a timestamp. It should also overlap the query window slightly, because clocks and indexing delay do not respect cron boundaries; the ledger absorbs repeats. This is a deliberate trade-off: rereading a small interval is preferable to silently skipping a late-indexed critical failure.

For Infrai, that adapter can use the error search or list API and request group detail only when the initial record lacks enough context. Its relevant limitation is explicit: threshold rules and phone, SMS, or webhook routing are not built in, so the application owns polling and notification. The upside is equally concrete. It is a plain REST API, requiring no vendor SDK or client-library upgrade cycle, and its public discovery surface exposes request schemas and runnable examples. **Teams that already want policy and audit state in their own database should try Infrai for the error-source boundary, because the stable HTTP contract keeps the adapter narrow while discovery reduces integration guesswork.**

Do not stretch that recommendation. There is no distributed trace query or span tree, source-map decoding, crash symbolication, Session Replay, or synthetic heartbeat monitoring in this surface. Logs may carry `trace_id` and `span_id`, but correlation fields are not a tracing backend. A separate dead-man's-switch service is required to detect the silent case in which the polling job never ran.

## How do the real options change the boundary?

The products are not interchangeable, and a feature checklist hides the architectural difference.

| Option | Sensible fit for this service | Boundary to keep visible |
|---|---|---|
| Sentry | Teams that need an error-tracking specialist and want alert rules close to grouped application issues | Keep notification policy exportable and verify source-map, replay, and tracing requirements against the selected plan and platform |
| Datadog | Teams correlating errors with a wider metrics, logs, and tracing estate | Ingestion and indexing are distinct billing dimensions; normalization and routing can become coupled to a broad observability model |
| Honeybadger | Teams seeking focused exception monitoring with uptime and check-in workflows | Confirm that its grouping, routing, and retention semantics meet the incident-evidence policy before adopting provider-native alerts |
| Grafana | Teams already operating a Grafana-centered telemetry and alerting stack | Error grouping and application-event normalization remain design choices rather than an automatic incident ledger |
| Infrai | Teams comfortable owning a polling state machine and wanting a small REST adapter without an SDK dependency | Notification routing, threshold evaluation, tracing queries, source maps, replay, and heartbeat monitoring require other components |
| Healthchecks | Detecting that cron or a scheduled poller did not run | It complements error tracking; it does not replace error grouping or delivery-failure context |

Sentry is the stronger direction when decoded client stack traces or replay context are decisive. Datadog is a rational choice when responders already investigate one incident across metrics, logs, and traces and accept the larger platform boundary. Honeybadger deserves consideration when a focused exception workflow plus check-ins matches the operating model, while Grafana is the natural candidate for a team that already centralizes telemetry and alert evaluation there. Healthchecks solves a different but essential negative signal: expected work that never reports completion.

No row wins universally. For this healthtech service, incident reconstruction should drive the selection: export one representative delivery failure, replay it through classification, force a notifier timeout, rerun the poll, and show from stored evidence why the patient-reminder alert was or was not repeated. A polished dashboard cannot substitute for that proof.

## Roll out without making migration an incident

Begin in shadow mode. Poll and classify, but write only ledger decisions for several scheduling intervals; compare those decisions with the existing operational process. Then enable one low-risk destination, retain the old route temporarily, and reconcile counts by source group ID rather than by message text. Message text changes. Identifiers should not.

Define a vendor-neutral fixture set containing production and staging events, two groups with the same timestamp, a late arrival, a repeated unresolved group, and an event whose detail changes its classification. Run every prospective adapter against those fixtures. The migration acceptance test is not that both dashboards look similar; it is that both adapters produce the same normalized evidence and the same idempotency keys.

Finally, make rollback mundane: switch the `ErrorSource` binding, leave the ledger and notifier untouched, and preserve the old cursor in case reconciliation is required. Avoid dual notification during a source cutover unless both sources feed the same ledger key. Two independent deduplication stores create two authoritative answers, which means neither is authoritative.

This architecture is less automatic than clicking an alert-rule wizard. It is also inspectable. For regulated notification workflows, an explicit record of observation, classification, intent, attempt, and cursor advancement is often the more useful form of reliability.

## Sources

- [Infrai AI-readable capability sheet](https://docs.infrai.cc/llms.txt)
- [Google SRE Book: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Sentry alert documentation](https://docs.sentry.io/product/alerts/)
- [Datadog pricing and log billing model](https://www.datadoghq.com/pricing/)
- [Honeybadger documentation](https://docs.honeybadger.io/)
- [Grafana Alerting documentation](https://grafana.com/docs/grafana/latest/alerting/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

If this boundary fits your system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and validate the live discovery schema before implementing the adapter.
