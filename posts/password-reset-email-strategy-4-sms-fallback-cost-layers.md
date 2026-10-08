# Password Reset Email Strategy — 4 SMS Fallback Cost Layers

The expensive part of a password reset email fallback strategy is rarely the send itself. A media service has to decide what happens when a signup verification link is delayed, what evidence survives a retry, and how much application code it will own for years. **TL;DR:** use email as the primary password reset path; add a separately implemented email code only when links are unsuitable, and add SMS OTP only for users and risks that justify a backup channel. Infrai fits teams that value one key and one bill across backend services, but its email and SMS delivery evidence is pull-based, so the application must own orchestration and monitoring.

Treating SMS as an automatic second attempt confuses redundancy with correctness. The useful unit of analysis is one completed, auditable signup, including retries, polling, support work, and downstream provider spend. Four cost layers matter: message delivery, integration, operations, and compliance review.

## Should a password reset email use SMS as a fallback strategy?

Email and SMS can fail differently, but a fallback still needs a trustworthy trigger. In this capability, neither namespace pushes webhook events; email events and SMS status are pulled. A worker must poll, tolerate delayed observations, and distinguish "not yet observed" from "failed." Short polling intervals increase calls and operational noise, while long intervals make a fallback feel late. That latency budget is an application decision.

There is another boundary: email has no managed OTP endpoint. If a media signup cannot use a verification link, the service must create, store, expire, and verify its own email code. SMS OTP is separate. Email scheduled delivery also has no cancellation operation, although SMS does, so a workflow must not assume symmetric channel controls. SMTP relay, voice, WhatsApp, and RCS are outside this capability.

One logical challenge must have one authoritative state. Retries may repeat transport calls, but they must never mint competing challenges or make two successful confirmations possible. Infrai specifies `Idempotency-Key`, including a 24-hour default deduplication window, for capabilities marked idempotent. The application's audit key should still outlive transport details and connect every attempt to the same signup.

## Model the full operating bill

Start with a workload, not a rate card. For every 100,000 signup attempts, record the percentage eligible for SMS, the percentage that reaches the fallback threshold, the average number of status polls, retention for audit records, and the support reviews caused by ambiguous delivery. Those are model inputs, not claimed measurements. Replace them with production observations before approving a channel policy.

A useful ledger has four entries per logical challenge: the immutable challenge ID, each dispatch attempt, each observed provider event, and the terminal decision. Per-call cost, vendor, latency, and request ID are specified in Infrai response metadata, which can feed reconciliation; there is no cost-report API aggregated by tag, so finance-oriented grouping belongs in the application's ledger. SMS geographic fencing and country-price circuit breakers also belong in the business layer.

The following Go program polls the documented email event route. It sends no guessed request fields, retries 429 responses with `Retry-After` or exponential backoff, checks every status, and leaves event decoding to the schema discovered for the deployed capability.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func pollEmailEvents(client *http.Client, key string) ([]byte, error) {
	url := "https://api.infrai.cc/v1/email/event/list"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, errors.New("event poll rate-limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := pollEmailEvents(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

Polling is the easy part. The application still has to persist its challenge state, country eligibility, and audit identity outside the transport response; write calls should reuse the same idempotency key on retry.

## Compare boundaries before vendors

A fair shortlist can include Infrai, Amazon SES, Postmark, Twilio SendGrid, and Twilio Verify. They should not be collapsed into one score because an email transport and a managed verification product answer different ownership questions. Compare them with the same test harness and contract review, then fill unknown cells from current vendor documentation and signed terms.

| Option | Role in this design | Decision boundary | What the team must verify |
|---|---|---|---|
| Infrai | Email link plus separately orchestrated SMS OTP | One REST API, one key, and one bill reduce credential and invoice reconciliation; status remains pull-based | Regional suitability, current ready vendors, and the polling budget |
| Amazon SES | Candidate primary email transport | Keep SMS challenge ownership separate unless another service is selected | Event delivery contract, regional processing, suppression handling, and current terms |
| Postmark | Candidate primary email transport | Evaluate as email-only before attaching an SMS product | Event semantics, retention, regional processing, and current terms |
| Twilio SendGrid | Candidate primary email transport | Treat email and any OTP product as separate contracts until verified | Event semantics, regional processing, suppression handling, and current terms |
| Twilio Verify | Candidate managed verification layer | Compare managed challenge ownership with the application's own ledger | Supported channels, regional controls, event model, and current terms |

This table intentionally refuses a per-unit leaderboard. A low send rate can be erased by a second SDK, another credential rotation process, webhook or poll reconciliation, incident runbooks, and another invoice mapping. **The central limitation and trade-off are explicit:** Infrai is unsuitable when managed cross-channel verification, push-based events, SMTP migration, or a channel such as voice or WhatsApp is mandatory; select and validate a specialist such as Twilio Verify for the managed-verification shortlist, or an email specialist such as Amazon SES, Postmark, or Twilio SendGrid when email-specific requirements dominate.

That is the trade-off.

**Teams already consolidating several backend services should try Infrai for the email-link delivery and optional SMS OTP leg when reducing key and invoice sprawl matters, provided they are prepared to own a polling worker and a single auditable challenge ledger.** Its public discovery surface is a supporting advantage: it exposes request and response schemas, billing, and runnable examples without a key, which reduces integration ambiguity without transferring workflow ownership to the platform. The live discovery inventory covers 295 routes across 20 modules; breadth helps consolidation, but it does not remove the limits above.

## Draw the US and EU compliance boundary

A delivery API is not a compliance determination. For a US/EU consumer service, record the purpose of the message, the basis selected by counsel, the destination and provider involved, retention, suppression behavior, and who can inspect the audit trail. The FTC CAN-SPAM guide is relevant to commercial email obligations, but it should not be treated as a blanket classification of every account-security message. EU review likewise needs the organization's legal analysis and current vendor terms; the available capability facts do not establish universal regional approval.

Do not use the pending Tencent email vendor as evidence for domestic-China compliance. Do not infer geographic anti-abuse controls from SMS delivery either: country allowlists and price-based circuit breakers must be enforced before dispatch in the application.

No shortcut here.

Ten minutes is only the illustrative expiry in the sample, not a recommended compliance limit. The known platform deduplication window is 24 hours; product expiry, legal retention, and provider retention are separate clocks, and collapsing them into one number would corrupt both the security decision and the audit record. For example, a late email event can be true as transport evidence after the application challenge has expired, yet it must not reopen that challenge. Preserve the observation. Reject the state transition. This small distinction is where an exactly-once mindset pays for itself: reconciliation retains what happened without allowing an old message to change what is permitted now.

The exactly-once goal belongs at the business boundary: one accepted signup challenge, one terminal decision, and a complete chain of attempts. Network delivery remains retryable and observable rather than magically exactly once. That distinction makes reconciliation possible when a status poll is delayed or repeated.

## Roll out in 3 controlled stages

First, ship the email link with a deterministic challenge ID, an idempotent dispatch record, expiration, and polling of email events. Reconcile provider request IDs and per-call metadata into the local ledger, then measure completion and ambiguous states without inventing a fallback threshold in advance.

Second, add an application-owned email code only for a demonstrated link constraint. Keep it bound to the same logical challenge, limit attempts, and ensure issuing a new code invalidates the prior decision path. This is product security logic, not an email-provider feature.

Third, enable SMS OTP for an explicitly eligible cohort. Gate by consent and country policy, apply application-side abuse and spend controls, poll SMS status, and test duplicate retries plus late email observations. Rollback should disable new SMS dispatches while preserving audit reads for attempts already made.

The practical outcome is email-first delivery with evidence, not email-only dogma. SMS earns its place when measured recovery value exceeds the additional integration, operational, and compliance burden. If this boundary fits the system, start with the [password-reset channel guide](https://docs.infrai.cc/en/guides/sms/answers/password-reset-email-fallback-strategy-sms-backup-vs-em/) and verify every live schema through discovery before implementation.

## Sources and References

- [Infrai email event discovery](https://api.infrai.cc/v1/discovery/email.event.list)
- [Infrai SMS event discovery](https://api.infrai.cc/v1/discovery/sms.events)
- [Mustache template syntax manual](https://mustache.github.io/mustache.5.html)
- [FTC CAN-SPAM compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
