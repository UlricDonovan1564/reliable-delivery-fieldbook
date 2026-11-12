# Node.js Scheduled Import Monitoring: Heartbeat Evidence for a Missed Cron Job Run

A scheduled Node.js media import job creates several monitoring costs before an alert is investigated: ingesting diagnostic events, retaining their contents, querying them after a failed run, and operating the heartbeat check that notices an absent completion. Consider a planning example with 24 scheduled imports per day, each producing 60 equally sized diagnostic events and one completion receipt. The diagnostic stream contributes 1,440 of the 1,464 daily records. It dominates this example's event volume; actual billing still depends on record sizes, indexing, queries, and the chosen service.

Short answer: alert on a failed Node.js cron task with error logs or metrics, and use a separate heartbeat monitor to detect a scheduled import that never produces its expected completion. For incident reconstruction, keep a compact, durable receipt of the accepted import outcome; shorten retention for verbose diagnostics only after deciding which questions that receipt can still answer.

Absence needs a witness.

The important change is to separate the lifetime of a completion receipt from the lifetime of its diagnostic transcript. In the example, that makes the longer-lived population 24 receipts per day instead of 1,464 equally sized records. This is a proposed retention calculation, not a measured storage reduction or a claim about any vendor's pricing. Its engineering value is that the retained evidence can follow the import's business identity rather than the volume of debugging output.

## How should heartbeat monitoring alert on a missed scheduled import job?

For a media importer, “the process exited” is a weak completion condition. A successful invocation can still leave the expected content absent if the application's acceptance rule allows an empty or rejected batch to count as success. Define that rule before attaching a heartbeat: the importer should report completion only after the intended result has crossed its durable commit boundary, with an explicit policy for whether an empty feed is acceptable.

Zero deserves a definition.

The proposed evidence model has a scheduled slot, a feed identifier, a logical run identifier, an accepted item count, and a commit timestamp. These are application fields, not a vendor request schema. An accepted count of zero can be legitimate for an idle feed; for a feed expected to publish something in every slot, the same count should lead to a different application outcome. Monitoring cannot infer that distinction from a successful HTTP response or an ordinary success log.

An exactly-once mindset makes the logical identity stable across attempts. Derive it from the feed and scheduled slot, enforce idempotency at the application write boundary, and preserve attempt identifiers separately when diagnostics require them. A retry must reconcile to the same committed import rather than create another batch merely because the first worker's acknowledgment was lost. A monitoring service's deduplication convention does not establish exactly-once application effects.

This definition determines what deserves long retention. The durable receipt supports reconciliation between expected slots and accepted content; the verbose transcript explains parser decisions and intermediate failures. They answer different questions, so assigning them identical lifetimes needs a reason.

Healthchecks, Cronitor, and Sentry Crons are alternatives to evaluate for scheduled-job heartbeat monitoring. The relevant comparison is the relationship between a missing completion and the evidence an investigator can retrieve. None of these choices relieves the importer of defining what a successful result means.

| Option | Useful investigation model | Boundary to verify before adoption |
| --- | --- | --- |
| Healthchecks | A ping-oriented check makes the expected completion signal explicit | Keep the import's accepted result and logical run identity in the application; a ping alone does not explain content acceptance |
| Cronitor | A scheduled-job monitoring workflow provides a place to investigate expected executions | Verify how the chosen configuration expresses the completion deadline and how long relevant run history remains available |
| Sentry Crons | Cron check-ins can sit beside application error investigation | Confirm the association between check-ins and application errors; preserve durable import evidence independently of a check-in |
| Datadog | Evaluate an existing telemetry investigation workflow as the home for supporting evidence | Verify missing-slot detection independently of error collection; an application receipt is still necessary |
| Grafana | Evaluate an existing metrics-oriented workflow for examining import outcomes | A dashboard cannot prove that an unreported import committed; retain the independent heartbeat check |

Choose the workflow that makes a missing slot easiest for the on-call engineer to investigate. An existing error investigation workflow can favor Sentry Crons; a narrowly scoped completion-ping requirement can favor Healthchecks; a team organizing operations around scheduled executions can evaluate Cronitor. Retention and notification requirements still need validation against the selected configuration. These are selection criteria, not a benchmark ranking.

Infrai fits the telemetry branch through a single API key and unified billing: one key, one wallet, one bill. Its unified REST API covers 295 routes across 20 modules, with no SDK required; adding a supported capability means another endpoint within the same integration. The API is genuinely self-describing, and the discovery surface is public with no key required, exposing request and response schemas for contract review. Keep the independent heartbeat service in the design, and give an application polling component responsibility for evaluating recorded errors and delivering the resulting notifications.

The limitation of a logs-only design is silent execution failure: it is not a fit for proving that an expected import actually ran. Choose Healthchecks-style completion monitoring for that responsibility. The retention trade-off is equally explicit: compact receipts preserve outcomes, but cannot preserve every intermediate explanation.

Breadth helps integration review. It does not change the evidence required to establish that a scheduled task completed.

## Storage cost: compact a transcript without inventing a committed outcome

The retention decision should be reviewable before detailed logs expire. The following standalone Go program demonstrates an offline projection of one illustrative import transcript into a compact receipt. Save it as `main.go` and run `go run main.go`. The Node.js importer remains the producer; this program makes the evidence transformation inspectable without introducing a monitoring API dependency.

All identifiers, timestamps, and counts in this example are hypothetical. The transcript's final event represents an already committed, accepted import; the program does not perform that commit, enforce database uniqueness, send a heartbeat, or create an alert.

```go
package main

import (
    "encoding/json"
    "fmt"
    "os"
    "time"
)

type Event struct {
    Feed     string
    Slot     time.Time
    Run      string
    Kind     string
    At       time.Time
    Accepted int
    Detail   string
}

type Receipt struct {
    Feed        string    `json:"feed"`
    Slot        time.Time `json:"slot"`
    LogicalRun  string    `json:"logical_run"`
    CommittedAt time.Time `json:"committed_at"`
    Accepted    int       `json:"accepted"`
}

func compact(events []Event) (Receipt, error) {
    var receipt Receipt
    found := false
    for _, event := range events {
        if event.Kind != "committed" {
            continue
        }
        if found {
            return Receipt{}, fmt.Errorf("ambiguous transcript: multiple commit events")
        }
        if event.Feed == "" || event.Run == "" || event.Slot.IsZero() ||
            event.At.Before(event.Slot) || event.Accepted < 0 {
            return Receipt{}, fmt.Errorf("invalid committed evidence")
        }
        receipt = Receipt{
            Feed: event.Feed, Slot: event.Slot, LogicalRun: event.Run,
            CommittedAt: event.At, Accepted: event.Accepted,
        }
        found = true
    }
    if !found {
        return Receipt{}, fmt.Errorf("no committed outcome: preserve transcript")
    }
    return receipt, nil
}

func main() {
    slot := time.Date(2026, time.November, 7, 8, 0, 0, 0, time.UTC)
    events := []Event{
        {Feed: "arts", Slot: slot, Run: "arts-slot-0800",
            Kind: "diagnostic", At: slot.Add(time.Minute),
            Detail: "feed parsed; acceptance pending"},
        {Feed: "arts", Slot: slot, Run: "arts-slot-0800",
            Kind: "committed", At: slot.Add(2 * time.Minute), Accepted: 12},
    }
    receipt, err := compact(events)
    if err == nil {
        err = json.NewEncoder(os.Stdout).Encode(receipt)
    }
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Rejecting multiple commit events is deliberately conservative: an offline compactor should expose ambiguous input rather than silently choose the last line and manufacture certainty. A production projection would distinguish duplicate delivery of the same durable commit from conflicting outcomes, using the application's stable commit identity. This example refuses both because its small input model contains no such identity.

The projection drops the diagnostic detail. That omission is the point, and also its limit: the receipt preserves the accepted count and logical run, but cannot later explain why a parser rejected a particular item. Store it durably and verify it against the authoritative application outcome before expiring the transcript. Printing JSON is only a demonstration of the projection.

## Failure modes: reconstruct a commit without an observed heartbeat

An alert should open an investigation against a feed and scheduled slot, not automatically replay the import. First compare the expected slot with its durable receipt, then consult explicit error evidence and the heartbeat observation. Logs or error events can explain a reported failure; the independent heartbeat monitor covers the silence left when a task never runs. A quiet log stream is insufficient evidence of successful execution.

The most revealing case is a committed receipt with a missing heartbeat. The import has accepted results, yet the completion signal has not been observed. Reconciliation can direct attention to acknowledgment delivery without assuming that the content must be imported again. Consumer idempotency remains necessary if a retry is authorized.

Conversely, a received heartbeat without the expected committed result challenges the success condition itself. A monitor can truthfully record that it received a signal while the application has no acceptable content for that slot. Preserve both observations in the incident timeline; do not let a green check overwrite contradictory business evidence.

Unknown is a state.

If the receipt store cannot be read, report that uncertainty separately from an absent receipt. Use a stable incident identity for repeated evaluations of the same scheduled slot, and retain acknowledgment and resolution decisions so the audit trail explains why an operator retried, waited, or closed the incident. The desired property is accountable reconciliation; delivery of a notification alone does not supply it.

For the telemetry branch, this separate Go client reads the metrics query route without adding undocumented filters. Set `INFRAI_API_KEY` and `INFRAI_BASE_URL` to the credential and the HTTPS API origin, respectively; the origin must have no path. Save this program separately as `inspect.go` and run `go run inspect.go`. It prints evidence for inspection, not a missed-run verdict. Its 20-second timeout and 1 MiB response limit are local bounds, not provider guarantees.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func inspect() error {
    key := os.Getenv("INFRAI_API_KEY")
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    origin, err := url.Parse(base)
    if err != nil || origin.Scheme != "https" || origin.Host == "" ||
        origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" ||
        origin.User != nil || key == "" {
        return fmt.Errorf("set the API key and HTTPS API origin")
    }
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest("GET", base+"/v1/metrics/query", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > 1<<20 {
            return fmt.Errorf("response exceeds local inspection limit")
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                candidate := time.Duration(seconds) * time.Second
                if candidate > delay {
                    delay = candidate
                }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if candidate := time.Until(deadline); candidate > delay {
                    delay = candidate
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := inspect(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The read request creates no application effects. It therefore needs no write idempotency key; the importer still needs its own durable idempotency boundary. A query response cannot establish missed-run detection without the independent schedule and completion evidence.

## Retention trade-offs: deliberately stop keeping the intermediate story

After the chosen investigation window, this design stops retaining successful imports' verbose parser diagnostics and full intermediate responses in the monitoring evidence set. It keeps the compact receipt and the application records needed to reconcile accepted results. That moves the dominant long-lived record population in the opening example, while accepting that an older incident may become explainable only at the outcome level.

The cost appears when something goes wrong later: a receipt can establish that 12 items were accepted in the illustrative slot, but cannot reconstruct the original response body or recover a discarded parser decision. If those details are required for a particular feed's dispute window, extend their retention deliberately. A cheaper-looking evidence set that cannot answer the team's incident questions is the wrong design.

An operational audit trail is not proof of regulatory compliance, immutable custody, or satisfaction of an erasure request. Review retention, access controls, personal-data exposure, and legally required evidence separately; avoid placing article bodies, access tokens, or unnecessary personal data in routine receipts. For scheduled media imports, the useful boundary is precise: retain enough to establish the accepted outcome and explain operational decisions, and document which intermediate explanations will disappear.

## Further reading

References for evaluating the completion-monitoring workflow and interpreting log severity:

- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Cronitor cron-job monitoring guide](https://cronitor.io/docs/cron-job-monitoring)
- [Sentry Crons documentation for Node.js](https://docs.sentry.io/platforms/javascript/guides/node/crons/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
- [RFC 5424, The Syslog Protocol](https://datatracker.ietf.org/doc/html/rfc5424)
