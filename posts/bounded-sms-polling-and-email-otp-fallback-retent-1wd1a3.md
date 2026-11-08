# Bounded SMS Polling and Email OTP Fallback Retention for Marketplace SaaS Login

A marketplace needs recipients to receive a generated report as an email attachment and to authenticate reliably through SMS login with email fallback when they return to the application. The operating bill includes message sends, status reads, report storage, and retained authentication evidence. In an illustrative workload of 10,000 login challenges, a 60-second observation window checked every 5 seconds permits 120,000 status reads if every challenge consumes its entire allowance. That is a request-volume budget, not a measured workload or a claim about provider charges; it identifies the term that a polling-only design must bound before adding another delivery channel.

TL;DR: Keep SMS OTP as the primary login factor, and add email fallback only if bounded polling and application-owned email code issuance and validation meet the marketplace's recovery requirements. Keep report attachment delivery separate from authentication recovery. The least complex viable arrangement is one challenge lifecycle, a finite observation budget, and an explicit transition that retires the SMS challenge before activating its email replacement.

## What actually grows when delivery requires polling?

For this design, the resource budget can be expressed as message attempts plus status reads plus retained evidence. Those quantities have different units and should remain separate until actual billing and storage rates are known. The calculation above deliberately counts only status reads: dividing 60 seconds by 5 seconds gives 12 checks per challenge, while a policy allowing 3 checks would bound the same illustrative workload at 30,000 reads. Neither policy establishes that a message arrived. Shortening observation also reduces the evidence available before making a recovery decision, so the smaller number alone cannot justify the change.

Bound the uncertainty.

A delivery receipt and a successful OTP verification establish different things. Polling can inform routing, but possession of a valid, unexpired code must determine authentication; treating a delivery status as permission to release a sensitive report would collapse the transport boundary into the authorization boundary. For a marketplace, this distinction matters because the report email, the SMS challenge, and any email recovery message can progress independently, and their records need separate purposes even when they share an internal workflow identifier.

The report itself should not become the recovery message. A design that attaches the report to an OTP email ties financial content to a message whose primary purpose is proving access to a mailbox, complicating both resend policy and evidence retention. Specify the attachment delivery requirements independently, including the provider's supported attachment representation and limits, before choosing a sender; no attachment payload contract is assumed in the example below.

## When should email OTP fallback replace an SMS login challenge?

In a polling-only integration, the application owns the interval between observing an ambiguous SMS outcome and deciding to offer email recovery. The useful decision is a bounded transition, with a recorded reason and a clearly defined validity boundary, rather than an attempt to infer instantaneous delivery from repeated reads. A late SMS can still be seen by the user after the application has chosen another channel. Retiring its challenge prevents that old message from creating a second authentication path.

Use one logical recovery operation per challenge generation.

Repeated requests for that operation should return its recorded outcome, while a second operation against an already retired generation should be rejected or resolved against current state. The durable implementation needs a transaction or equivalent conditional write covering the challenge transition and an outbox entry for the email request. Keeping those operations together closes the gap in which the application records recovery but never schedules a message.

There is another gap at the network boundary: a send can succeed while its response is lost. Provider-supported idempotency helps control repeated submissions, but local transaction success does not prove an external message was sent exactly once. Store the submission identity and reconcile ambiguous results before authorizing another send. Exactly-once reasoning belongs in the business state transition; duplicate or delayed transport remains something the design must tolerate.

Email recovery also needs its own code issuance, expiry, attempt limits, and validation. Never derive a recovery code from a report identifier, and never copy plaintext codes into the audit trail. A secure implementation must protect short codes against offline guessing as well as online abuse; merely hashing a small numeric space is insufficient protection if the stored verifier is exposed. The OWASP recovery guidance is a useful baseline for those controls, although a recovery flow still needs its own threat analysis before it can be accepted as a second factor.

## A bounded status read before the transition

The following standalone Go program reads an existing SMS message's status. Supply INFRAI_API_KEY, SMS_MESSAGE_ID, and INFRAI_BASE_URL, with the last variable set to the provider's documented HTTPS API base including its version prefix. Keeping that base in configuration avoids embedding a vendor URL in this independent note. The response is printed without interpreting undocumented status fields; the application must obtain their contract from discovery before mapping them to policy.

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

func run() error {
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    id := os.Getenv("SMS_MESSAGE_ID")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" || key == "" || id == "" {
        return fmt.Errorf("configure the HTTPS API base, API key, and message ID")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            base+"/sms/status/"+url.PathEscape(id), nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if err != nil {
            return err
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(body))
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            return fmt.Errorf("status %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if requested := time.Duration(seconds) * time.Second; requested > delay {
                delay = requested
            }
        } else if deadline, err := http.ParseTime(retry); err == nil {
            if requested := time.Until(deadline); requested > delay {
                delay = requested
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("status read budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The retry budget is three requests. It does not renew itself.

This example retries rate-limited reads with exponential backoff, honors a longer Retry-After, and abandons waiting when its overall deadline expires. It makes no write request, so it cannot itself create duplicate messages. A deployed service must still persist the total polling allowance across worker restarts, scope operation identities to the account and challenge generation, and check current authorization before revealing an earlier operation's result. The outbox, expiry enforcement, and code verifier are separate implementation requirements. Activating an email generation means accepting only that generation's valid code; it does not mean that transport has succeeded.

Although the surrounding SaaS may use Node.js, this state transition does not depend on its runtime. The example isolates the correctness question that survives a language change: can two workers, retries, or a crash authorize two distinct recovery outcomes for one challenge?

## Choosing the verification and report delivery boundary

Compare Twilio Verify, Amazon SES, and Twilio SendGrid as candidates for different responsibilities. Twilio Verify belongs in the managed verification evaluation; Amazon SES and Twilio SendGrid belong in the email delivery evaluation for the generated report and application-issued recovery messages. These are objective differences in product scope, not evidence that any candidate meets a particular marketplace's reliability target. None should receive credit for successful attachment delivery, acceptable regional processing, or a desired fallback contract without checking the applicable documentation and testing the actual integration.

| Candidate | Responsibility to evaluate | Acceptance boundary |
| --- | --- | --- |
| Twilio Verify | Managed verification lifecycle | Establish which channel transitions and verification controls the selected integration actually owns. |
| Amazon SES | Application-controlled email delivery | Confirm attachment support, delivery observation, and the recovery logic the application must retain. |
| Twilio SendGrid | Transactional report and recovery email delivery | Confirm attachment constraints and distinguish message events from successful authentication. |

Infrai offers one key, one wallet, one bill, and a single REST API with no SDK to install. Its public, self-describing discovery surface requires no key and includes request and response schemas and runnable examples, so a developer can read a capability contract before wiring a new operation instead of learning another SDK. Its specified per-call cost, vendor, latency, and request metadata also supports reconciliation between a marketplace's internal send ledger and provider activity. For this workflow, choose it only when polling-based observation and application-owned email OTP logic satisfy the recovery contract.

That integration boundary is a substantive trade-off. A system requiring tightly timed, event-driven authentication routing should select an arrangement whose documented delivery and verification contracts support that requirement. For the bounded polling design discussed here, issue recovery email immediately after the authorized transition rather than relying on delayed code delivery; its security deadline should be controlled by challenge expiry and validation.

Do not confuse report email reliability with proof of regional compliance. For US and EU deployment, review processing locations, retention terms, subprocessors, and the legal basis for each message class. The FTC's CAN-SPAM guidance helps distinguish transactional or relationship messages from commercial messages; it does not establish EU compliance, approve an authentication factor, or certify a provider's data residency. Country-specific SMS abuse controls and spending guardrails also belong in the application acceptance criteria.

## Retain the decision and discard the secret

The change that bounds the dominant request-volume term is a finite polling allowance attached to a single challenge generation, followed by one recorded recovery decision. Stop polling when verification succeeds, the generation expires, or the transition commits. A retry that reads an existing decision should not restart the allowance. This preserves a useful reconciliation boundary even when messages arrive out of order.

Retain the logical operation identity, challenge generation, state transition, decision reason, relevant timestamps, and correlation identifiers under an explicit access and retention policy. A request identifier can connect an ambiguous submission to later investigation without requiring every worker to keep the full message body.

Retention needs a reason.

The exact retention duration needs a documented business and legal decision; neither a convenient database default nor the phrase audit trail supplies that decision.

Deliberately stop keeping plaintext OTPs, redundant report attachments in authentication records, and raw polling responses once the approved diagnostic window closes. Preserve the report under its own access and retention rules. This choice reduces secret exposure and repeated payload storage, but it has a cost: after detailed transport evidence is deleted, an investigator may reconstruct the application's decision without reconstructing every intermediate provider response or the exact message content. Record that limitation before an incident, while the team can still decide whether the retained evidence is sufficient.

## Further reading

References for verification scope, email delivery evaluation, and security and compliance boundaries:

- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Amazon SES developer guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio SendGrid mail send documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [FTC CAN-SPAM Act compliance guide for business](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
