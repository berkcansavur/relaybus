# Security Policy

## Supported versions

relaybus is pre-1.0. Only the latest published minor version of every `@relaybus/*` package receives security fixes. After 1.0 the latest two minor versions will be supported.

| Version      | Supported |
| ------------ | --------- |
| latest `0.x` | Yes       |
| older `0.x`  | No        |

## Reporting a vulnerability

Please do not open a public issue.

Use GitHub private vulnerability reporting: go to the [Security tab](https://github.com/berkcansavur/relaybus/security/advisories/new) and choose "Report a vulnerability".

Include the affected package and version, a description of the impact, and steps or a proof of concept to reproduce.

You can expect an acknowledgement within 3 business days and a status update within 10 days. Fixes are released as patch versions with a published GitHub Security Advisory crediting the reporter, unless you prefer to stay anonymous.

## Scope

In scope: the code published as `@relaybus/*` packages in this repository, for example event loss or duplication that violates documented guarantees, injection through store adapters, or unsafe handling of event payloads.

Out of scope: vulnerabilities in third-party dependencies without a relaybus-specific exploit path (report those upstream), and issues requiring a malicious application developer.

## Supply chain

Releases are published from GitHub Actions with npm provenance, through npm Trusted Publishing (no long-lived npm tokens). You can verify a package with `npm audit signatures`.
