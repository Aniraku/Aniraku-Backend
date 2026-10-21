# Security

If you find a vulnerability, report it privately before publishing details. Good reports help protect people's accounts, watch history, and data.

## Report a concern

Send a concise report to **[security@aniraku.tech](mailto:security@aniraku.tech)**. Include the affected version or endpoint, a clear description, reproduction steps, impact, and a safe proof of concept where possible. Do not include credentials, access tokens, private keys, user data, or destructive payloads in the report or in public issues.

| `SCOPE` | `TRIAGE` | `DISCLOSURE` |
| --- | --- | --- |
| The deployed backend service and the code in this repository. | Reports are assessed for impact, reproducibility, affected scope, and a viable mitigation. | Credible findings are acknowledged and resolved responsibly before public disclosure where practical. |

## Service security boundary

The repository must never carry real credentials. Supabase keys, service tokens, JWT material, and other secrets belong in environment variables at deploy time, never in committed files; see [the configuration notes](README.md#configuration). Requests run through an SSRF dialer guard, a host allowlist, and no-redirect handling so the server dials only intended upstream hosts. Stream locations are short-lived runtime values, not durable data.

---

`See also`

[README](./README.md) · [Privacy](./PRIVACY_POLICY.md) · [Terms](./TERMS.md) · [DMCA](./DMCA.md) · [Contributing](./CONTRIBUTING.md)
