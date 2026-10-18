---
title: relaybus releases and versioning
description: How relaybus packages are versioned and released with semantic-release and Conventional Commits, and where to find the changelog on GitHub Releases.
---

# Releases

Every push to `main` with releasable [Conventional Commits](https://www.conventionalcommits.org/) publishes all seven `@relaybus/*` packages to npm with **one shared version**, computed by semantic-release. There is no committed changelog: the [GitHub Releases](https://github.com/berkcansavur/relaybus/releases) page is the changelog.

- Current release line: **0.1.x**. While the version is below 1.0.0, a breaking change bumps the minor version.
- `feat` commits bump the minor version; `fix`, `perf` and `revert` bump the patch version. `docs`, `chore`, `ci`, `test`, `build`, `refactor` and `style` do not release.
- Prereleases are published from the `next` and `beta` branches under the matching npm dist-tags.
- Releases are published by CI with npm Trusted Publishing (OIDC) and provenance, so a tarball links back to the workflow run that built it.

## Install a specific version

```sh
pnpm add @relaybus/core@0.1.0 @relaybus/nestjs@0.1.0 @relaybus/postgres@0.1.0
```

Keep all `@relaybus/*` packages on the same version.

## Links

- [GitHub Releases](https://github.com/berkcansavur/relaybus/releases)
- [npm: @relaybus/core](https://www.npmjs.com/package/@relaybus/core)
- [Security policy](https://github.com/berkcansavur/relaybus/blob/main/SECURITY.md)
- [Contributing](https://github.com/berkcansavur/relaybus/blob/main/CONTRIBUTING.md)
