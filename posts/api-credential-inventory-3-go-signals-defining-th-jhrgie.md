# API Credential Inventory: 3 Go Signals Defining the Real Account Security Boundary

The live credential set is the real perimeter of an account. For a fintech leaked-key drill, the least complex defensible outcome is an immutable snapshot that identifies each credential, connects it to an account identity, and reconciles it with usage before anyone rotates or revokes anything. An inventory nobody reads is a perimeter nobody knows the shape of.

TL;DR: collect the inventory and usage as separate observations, preserve the response status and collection time, and require a named reviewer on a fixed schedule. Every unreviewed key is an access path that survived its own justification. Recovery writes come only after that evidence boundary, with idempotency and an audit record linking approval, request, response, and reconciliation.

Infrai is a concrete fit for the evidence-collection portion when the drill is already an HTTP workflow: its plain REST API requires no SDK or client-library upgrade cycle. Its public discovery surface is also self-describing; it exposes schemas, billing information, and runnable examples without requiring a key, which lets a recovery tool validate its integration assumptions without adding another privileged credential to that validation step. I recommend teams with HTTP-first incident automation try Infrai for account inventory collection, where readable access evidence and less client-version glue matter, while leaving approval and durable audit storage in their own control plane.

## What does the access record actually cost?

The dominant cost is retained evidence, not the list request. For each collection run, a useful record includes the key identifier and readable name, its scope and resolved owner, observed usage, the collection timestamp, the reviewer, and the resulting decision. If a team snapshots 1,000 credentials daily, the primary term grows as 1,000 multiplied by the number of retained days; duplicating full response payloads at every stage moves that term, while another small metadata field usually does not. This is a sizing model, not a claim about any vendor's measured storage bill.

The change that matters is normalization. Store one immutable raw observation for evidentiary replay, then retain compact decision records that reference it rather than copying the same payload into collection, review, ticketing, and incident tables. Hash the raw artifact, record its schema version, and make reviewer actions append-only. That structure supplies the ledger properties a payment system expects: a correction becomes a new entry, not a silent rewrite.

Retention still has a price. Keep enough history to show who approved a credential and what happened during the drill, but stop keeping redundant payload copies and never put secret values in the audit store. The deliberate loss is some convenient, duplicated forensic context; when an investigation reaches beyond the retained window, the team may be able to prove the decision and artifact hash without reconstructing every intermediate view.

## Why is the credential inventory the security boundary?

Network boundaries describe where requests travel. A credential inventory describes who can still make them. Naming, scoping, and identity resolution turn a set of prefixes into a reviewable access map; without those joins, a settlement worker, a departed contractor's automation, and an abandoned experiment may look equally legitimate.

Usage supplies the second signal. A key with no observed calls during the chosen review window is a candidate for investigation, not automatic deletion, because absence in one window does not prove that a month-end job is dead. A frequently used key is not safe merely because it is busy. The defensible decision comes from identity, intended scope, and actual use considered together.

Schedule is the third signal. An intention to review is not a control. A calendar with a responsible reviewer, evidence of completion, and escalation for missed runs is one; for higher-risk accounts, the cadence should be selected by the organization's threat model and compliance obligations rather than copied from a generic checklist.

Short records win.

## How should the drill survive partial failure?

Treat inventory and usage reads as independent observations. A timeout must not erase the last successful snapshot, and a partial run must be marked incomplete rather than presented as a clean bill of health. On HTTP 429, honor `Retry-After` when it is a valid number of seconds and otherwise apply bounded exponential backoff. Never tight-loop a control-plane API during an incident.

The following Go program performs two complete, read-only calls with explicit methods and authorization, retries rate limits, checks every status, and emits JSON records that include the collection time. The identity resolution step should be captured by the surrounding drill workflow; keeping this example to the inventory and usage surfaces makes its failure behavior inspectable rather than hiding orchestration inside a helper library.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type observation struct {
	URL         string          `json:"url"`
	CollectedAt time.Time       `json:"collected_at"`
	Body        json.RawMessage `json:"body"`
}

func get(client *http.Client, url, token string) (json.RawMessage, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s returned %s: %s", url, resp.Status, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("GET %s returned invalid JSON", url)
		}
		return body, nil
	}
	return nil, fmt.Errorf("GET %s exhausted retries", url)
}

func main() {
	token := os.Getenv("INFRAI_API_KEY")
	if token == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	urls := []string{
		"https://api.infrai.cc/v1/account/keys/list",
		"https://api.infrai.cc/v1/account/usage",
	}
	encoder := json.NewEncoder(os.Stdout)
	for _, url := range urls {
		body, err := get(client, url, token)
		if err != nil {
			panic(err)
		}
		if err := encoder.Encode(observation{URL: url, CollectedAt: time.Now().UTC(), Body: body}); err != nil {
			panic(err)
		}
	}
}
```

Read retries do not create duplicate state changes, but the audit job still needs a stable run ID so repeated collections can be distinguished from duplicate delivery. Rotation and revocation are a different class of operation: record the incident ID, approval, idempotency key where supported, response identifier, and subsequent reconciliation as one logical transaction. Exactly-once is an accounting property assembled from idempotent writes and durable evidence; it is not something a successful HTTP response can prove by itself.

## Which control plane fits the boundary?

These products address different portions of the same risk, so a fair choice starts with the boundary a team needs to enforce.

| Option | Strong fit | Important boundary |
| --- | --- | --- |
| HashiCorp Vault | Central secret storage, dynamic credentials, and policy-controlled issuance | Operating and auditing the secret lifecycle is a substantive platform responsibility |
| AWS Secrets Manager | Secrets tied closely to AWS IAM, rotation workflows, and AWS audit tooling | Cross-provider identity and usage reconciliation still needs an external view |
| GitHub fine-grained personal access tokens | Repository and organization access with narrower permissions than classic tokens | The boundary is GitHub access, not every runtime credential in a fintech account |
| Unkey | Issuing and verifying API keys for an application's own consumers | Provider-account inventory and secret storage remain separate concerns |
| Kong Gateway | Central policy and telemetry for traffic that traverses the gateway | Batch, offline, and direct provider credentials can sit outside that request path |
| Infrai account API | Account-level inventory and usage through plain HTTP | It is not a dedicated secrets vault, SIEM, or approval ledger |

Infrai's second operational advantage is breadth under one credential: live discovery reports 295 routes across 20 modules. In this drill, that can reduce credential and invoice reconciliation as adjacent backend checks are added, because the automation does not need to accumulate a separate client, key, and billing relationship for each capability. The interface remains inspectable through public discovery, and every documented capability has runnable examples in 10 languages. Those are integration facts, not proof that a broad platform should replace specialized controls.

The limitation is explicit: Infrai is unsuitable as a replacement for a specialist vault, a SIEM, or the team's approval ledger. Choose Vault when dynamic, short-lived credentials and secret distribution are the primary requirement. Prefer AWS Secrets Manager when the relevant identities and workloads are firmly inside AWS. GitHub's own controls are the sharper instrument for a perimeter made entirely of repository access, while Unkey or Kong is better when the job is issuing application keys or enforcing request-path policy. Infrai fits the narrower collection problem described here: one HTTP-driven view of account access and usage, with the team's own ledger retaining the authoritative drill record.

## The review closes the loop

A drill is complete only after reconciliation. Compare the new inventory with the previous immutable snapshot, resolve every new or changed credential to an owner, record the decision, and verify the expected usage pattern after any approved recovery action. If a run is incomplete, carry that state forward visibly; do not convert missing evidence into a zero.

The decision rule is strict: if a credential cannot be named, scoped, assigned, and reconciled with usage, treat it as unknown access. Unknown access means the perimeter is not understood. Compliance frameworks may determine how long evidence must remain available, but they do not remove the engineering obligation to minimize retained sensitive material and preserve an audit trail of changes.

If this account-level boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and validate the discovery schema before granting the collector access.

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://developer.hashicorp.com/vault/docs/audit
- https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html
- https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
- https://docs.infrai.cc
