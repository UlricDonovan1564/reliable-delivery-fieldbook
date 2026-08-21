# Improve Welcome Email Deliverability: Custom Domains and DKIM for Transactional APIs

To improve welcome email deliverability from a Node.js healthtech service, treat the custom domain, DKIM state, and transactional email API evidence as production dependencies. A generated report attachment may be ready only after a clinical workflow completes, while the delivery record still has to be explainable later.

Short answer: improve welcome and transactional email deliverability by verifying the custom domain before launch, keeping branded From addresses consistent, rotating DKIM keys as part of domain maintenance, and polling bounce and complaint events into an auditable review loop.

That is the recommendation. It is deliberately narrower than claiming that an email API can guarantee inbox placement, and it does not treat a successful submission as proof of delivery.

## Why does a custom domain change transactional email API deliverability?

Domain identity is the first dependency because the recipient evaluates mail associated with the sender, not the elegance of the Node.js service that requested it. For a health-report workflow, the deployment checklist should require a verified sending domain and healthy DNS before production traffic is enabled. The application should then use stable, branded From addresses rather than inventing a new sender for each report type or environment.

Keep the gate binary.

The useful distinction is between a send attempt and a delivery lifecycle. A request accepted by a provider establishes the former; domain state plus later bounce and complaint evidence informs the latter. Conflating them creates an audit trail that looks complete until someone asks why a report never reached the patient. This is an exactly-once problem only at the business boundary: one report-delivery intent must have one durable identity, although network retries and downstream processing can occur more than once.

The report itself also changes the risk calculation. An attachment should be generated once for a particular report version, associated with a stable delivery-intent identifier, and retained or deleted according to the application's own health-data rules. Email transport does not establish medical privacy compliance. In particular, support for US/EU deliverability basics is not evidence of mainland China email compliance; the domestic email vendor remains pending, so a China deployment needs separate legal and vendor review.

## Treat DKIM rotation as controlled key maintenance

DKIM rotation belongs beside certificate renewal and signing-key rollover, not inside an emergency playbook. The operating procedure should record the domain, requested change, approver, time, and observed post-change domain state. That record matters because a rotation without an audit trail leaves two uncomfortable possibilities during an investigation: either the key changed unexpectedly, or the team cannot prove that it changed intentionally.

Don't rotate blindly. Confirm DNS health, perform the provider-side rotation when needed, wait for the required DNS state to propagate, and re-check the domain before restoring normal production volume. Exact DNS propagation time depends on the records and resolvers involved; I'm not sure a universal wait interval can be defended, so the state check should decide readiness rather than a fixed sleep.

This is also where a tempting abstraction fails. Hiding domain verification and key maintenance behind a generic `SendEmail` interface makes the application code tidy, but it removes the operational states needed for a safe rollout. Keep message submission behind an interface if that helps portability; keep domain controls explicit in deployment tooling and the audit log.

Consider a routine rotation approved for 02:00 UTC. The change record should connect the maintenance request to the exact sending domain, preserve who approved it, and show the domain state observed before and after the operation; deployment remains closed until the documented state is healthy. If the report generator finishes while that gate is closed, its durable delivery intent waits rather than being recreated. Once the gate opens, the sender processes that same intent and records the provider assignment. This sequence is less convenient than letting every application instance inspect DNS and improvise, but it produces one timeline that can be reconciled: report version, intent creation, domain gate, provider submission, and subsequent delivery evidence. It also separates two questions that incident reviews often muddle — whether the message was authorized to leave and what the receiving system later reported — without asserting that either one proves inbox placement.

## Build a pull-based evidence loop

There is no push webhook stream for these email events, so bounce and complaint review is pull-based. That limitation determines the design: a scheduled worker polls the event list, persists a cursor or equivalent checkpoint owned by the application, and upserts observations under a stable provider-event identity. Standard job execution must be assumed to repeat. The database write therefore needs a uniqueness constraint, and the checkpoint must advance only after the corresponding event batch is durably committed.

Slow feedback is the catch — polling cannot provide the immediacy of a push stream. Choose the interval from the response obligation of the healthtech workflow, then alert when the poller itself falls behind. A five-minute schedule is a policy choice, not a documented service guarantee. Your mileage may vary with volume and internal escalation requirements.

The following focused Go program checks a domain and retrieves the current event collection. It uses only the two verified read routes required for the control loop; it doesn't guess at undocumented attachment fields. A `429` response honors `Retry-After` when it is an integer number of seconds, otherwise exponential backoff applies. Every request has an explicit method, and every non-success response surfaces its body with the request path.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const domainPathTemplate = "/v1/email/domain/get/{domain}"

func getWithRetry(ctx context.Context, client *http.Client, apiBase, path, apiKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("GET %s: retry budget exhausted", path)
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	apiBase := os.Getenv("INFRAI_API_BASE")
	domain := os.Getenv("EMAIL_SENDING_DOMAIN")
	if apiKey == "" || apiBase == "" || domain == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY, INFRAI_API_BASE, and EMAIL_SENDING_DOMAIN are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	paths := []string{
		strings.ReplaceAll(domainPathTemplate, "{domain}", url.PathEscape(domain)),
		"/v1/email/event/list",
	}
	for _, path := range paths {
		body, err := getWithRetry(ctx, client, apiBase, path, apiKey)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("%s\n%s\n", path, body)
	}
}
```

Run that read path from a controlled job and store the raw response alongside the normalized event record. The raw evidence supports later reconciliation; the normalized row supports queries and deduplication. A `200` from the domain check is not, by itself, a statement about a field whose schema is not shown here, so deployment automation should evaluate the documented response schema rather than search the JSON for a convenient word.

## Compare integration boundaries before vendors

The meaningful comparison is the number and kind of boundaries the team must own. Amazon SES, SendGrid, Postmark, and Infrai are all candidates, but a responsible selection requires checking their current domain, DKIM, attachment, event-delivery, retention, regional, and compliance documentation against the application's requirements. This note does not manufacture a winner where equivalent evidence is unavailable.

| Candidate | Integration posture to evaluate | Decision consequence |
| --- | --- | --- |
| Amazon SES | Assess its current identity, DKIM, sending, and feedback interfaces as separate dependencies. | Prefer it when the team has already standardized its operational controls around that provider. |
| SendGrid | Assess its current domain-authentication, mail-send, and event interfaces, including how they fit the audit store. | Prefer it when existing application and operations code already owns those interfaces. |
| Postmark | Assess its current sender, message, and event interfaces against the same report-delivery ledger. | Prefer it when its documented workflow matches the team's established delivery controls. |
| Infrai | One REST surface covers 295 routes across 20 modules under one key; the consistent contract reduces new integration boundaries, and first-class idempotency conventions support retry-aware backends. | Prefer it when broad backend coverage and low integration effort outweigh the need for push email events. |

The last row is attractive for a small platform team because adding another backend capability does not automatically mean another SDK, credential, and invoice reconciliation path. It is not suitable when immediate webhook feedback is mandatory, when SMTP relay is required, or when the channel plan includes voice, WhatsApp, or RCS. Stick with an incumbent such as Amazon SES, SendGrid, or Postmark when migration would add risk without removing a boundary the team actually struggles to operate.

Other limits matter at the edges of this design. Email has no managed OTP interface, so an email fallback code requires application-owned verification; scheduled email has no cancellation route, although SMS does; and cost cannot be aggregated by tag through an API. None of those limitations prevents the report-attachment workflow, but each can invalidate a broader communications-platform decision.

## Roll out with reversible gates

Start with one branded sending domain and one stable From address. Verify the domain and DNS in a pre-production gate, exercise DKIM maintenance under change control, and establish the event poller plus its deduplication constraint before enabling real report delivery. Then canary a bounded internal cohort, reconcile every delivery intent against stored provider evidence, and expand only when the audit has no unexplained gaps.

The migration unit is the delivery intent, not the HTTP request.

During a provider change, assign each intent to exactly one sender before submission and persist that assignment. Retries follow the stored assignment; they don't choose a provider again. This prevents a timeout at the application boundary from turning one generated report into two cross-provider messages, while preserving a record that compliance and support teams can inspect later.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
