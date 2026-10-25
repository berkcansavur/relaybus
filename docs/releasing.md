# Releasing

Releases are fully automated with [semantic-release](https://semantic-release.org). Every push to `main` that contains releasable Conventional Commits publishes **all seven `@relaybus/*` packages with one shared version** (lockstep). There is no version-bump commit, no committed `CHANGELOG`, and no npm token: GitHub Releases are the changelog and npm Trusted Publishing (OIDC) authenticates the publish.

Configuration lives in `release.config.mjs`; the workflow is `.github/workflows/release.yml`.

## How versions are computed

The baseline is the git tag `v0.0.0` (see [bootstrap](#bootstrap-one-time)). semantic-release analyzes the commits between the last `v*` tag reachable from the branch and `HEAD`, takes the highest bump, and applies it. PRs are squash-merged, so the PR title is the commit message that counts (`pr-title.yml` validates it).

| Commit (0.x phase)                                          | Bump  | Example                         |
| ----------------------------------------------------------- | ----- | ------------------------------- |
| `feat`                                                      | minor | `feat(postgres): lease batches` |
| `fix`, `perf`, `revert`                                     | patch | `fix(core): clamp retry delay`  |
| Any type with `!` or a `BREAKING CHANGE:` footer            | minor | `feat(core)!: rename publish()` |
| `docs`, `chore`, `test`, `ci`, `build`, `refactor`, `style` | none  | `chore(deps): bump vitest`      |

Examples: from `v0.0.0`, a `feat` gives `0.1.0`, only `fix` gives `0.0.1`, a breaking change gives `0.1.0` (not `1.0.0`). From `v0.3.2`, a breaking change gives `0.4.0`. Only `docs`/`chore`/... commits produce no release and the workflow ends successfully.

Allowed scopes (commitlint and PR title check): `core`, `nestjs`, `postgres`, `mongodb`, `kafka`, `rabbitmq`, `testing`, `eslint-plugin`, `otel`, `repo`, `ci`, `deps`, `release`. Scopes do not influence the version because all packages release together; they only group the release notes.

Because the packages are in lockstep, `peerDependencies` on `@relaybus/core` are published as `^<version>`. In the 0.x range `^0.1.0` means `>=0.1.0 <0.2.0`, so consumers upgrade the adapters together with core on every minor.

### Dependabot

Dependabot uses `chore(deps)` (npm, production and development) and `chore(ci)` (GitHub Actions). Both are non-releasing types, so dependency bumps never publish on their own. When a dependency change must reach users (a security fix in a runtime dependency, a widened or raised peer range), retitle the PR to `fix(deps): ...` before squash-merging.

### Graduating to 1.0.0

While `release.config.mjs` contains `{ breaking: true, release: 'minor' }`, breaking changes cannot produce 1.0.0. To graduate:

1. Open a PR that deletes that rule from `releaseRules` (the preset then maps breaking changes to `major`). Title it `ci(release): enable major releases`.
2. Merge it, then merge a change whose squash commit carries a breaking marker, for example `feat(core)!: stable 1.0 API` with a `BREAKING CHANGE:` footer. The next release is `1.0.0`.

## Prerelease channels

| Branch | Version example | npm dist-tag | GitHub Release |
| ------ | --------------- | ------------ | -------------- |
| `main` | `0.2.0`         | `latest`     | release        |
| `next` | `0.2.0-next.1`  | `next`       | prerelease     |
| `beta` | `0.2.0-beta.1`  | `beta`       | prerelease     |

Push or merge into `next` or `beta` to cut a prerelease; install it with `npm i @relaybus/core@next`. The same gate (`ci.yml`) runs first. Merging a prerelease branch into `main` releases the stable version. Keep `next` and `beta` in sync with `main` by merging `main` into them.

## What the workflow does

1. `gate` runs the full CI as a reusable workflow.
2. The `release` job (in `environment: npm-publish`, with `id-token: write`, `contents: write`, `issues: write`, `pull-requests: write`) installs npm 11.20.0 and runs `semantic-release`.
3. semantic-release analyzes commits, generates notes (`conventionalcommits` preset) and, if a release is due, runs:
   - `node scripts/set-version.mjs <version>`: writes the version into every `packages/*/package.json` inside the CI workspace only. Nothing is committed.
   - `pnpm build`.
   - `node scripts/publish-packages.mjs <version> <channel>`: for each package in the order `core`, `testing`, then the rest, it skips the package if `name@version` already exists on npm; otherwise `pnpm pack` (which rewrites `workspace:*` / `workspace:^` to the real version) and `npm publish <tarball> --access public --provenance --tag <channel>`.
   - `@semantic-release/github`: pushes the tag `v<version>` and creates the GitHub Release.

Local, offline-safe checks:

```bash
node scripts/set-version.mjs 0.1.0     # edits package.json files; revert with git checkout
pnpm build
node scripts/publish-packages.mjs 0.1.0 next --dry-run   # packs and runs `npm publish --dry-run`
```

## Bootstrap (one time)

npm only allows attaching a Trusted Publisher to a package that already exists, so each package is first published once from your machine.

1. Create the npm organization `relaybus` (done) and make sure your account has 2FA.
2. Log in and publish the placeholders. The script publishes every package at `0.0.0` with `--access public` from `pnpm pack` tarballs and skips ones that already exist:

   ```bash
   npm login
   scripts/bootstrap-npm.sh
   ```

3. Create and push the baseline tag on the first commit. semantic-release counts commits after this tag, so the existing history releases as `0.1.0` on the first run:

   ```bash
   git tag v0.0.0 28d39d8
   git push origin v0.0.0
   ```

4. Create the GitHub environment `npm-publish` (Settings, Environments).
5. For each package (`core`, `nestjs`, `postgres`, `mongodb`, `kafka`, `rabbitmq`, `testing`, `eslint-plugin`, `otel`) open npmjs.com, Settings, Trusted Publisher, GitHub Actions, and enter organization or user `berkcansavur`, repository `relaybus`, workflow filename `release.yml`, environment `npm-publish`. Do this **within two days of the first CI publish at the latest**; until it is configured the CI publish fails with an authentication error (the run can be repeated, see below).
6. After OIDC publishing works, optionally set "Require two-factor authentication and disallow tokens" on each package.

### Adding a package later

A package that is new on npm cannot be published by CI until it exists and trusts the workflow. For
`@relaybus/eslint-plugin` and `@relaybus/otel` (both added in 0.3.0), before the release that first ships it:

1. `npm login` (with 2FA), then `scripts/bootstrap-npm.sh`. It publishes each new package at `0.0.0`
   and skips every package that already exists.
2. `scripts/trust-packages.sh` (or npmjs.com, Settings, Trusted Publisher for each new package with
   the values of step 5 above).
3. Optionally `scripts/harden-npm.sh` to require 2FA and deprecate the `0.0.0` placeholder.

`scripts/set-version.mjs` and `scripts/publish-packages.mjs` find every `packages/*` directory, so
the new package already releases in lockstep with the others.

## Troubleshooting

**`ENEEDAUTH`, `E404` on publish, or "OIDC token exchange failed".** The Trusted Publisher does not match. Check, per package: repository `berkcansavur/relaybus`, workflow filename exactly `release.yml`, environment exactly `npm-publish`. The publish must run in `release.yml`'s own `release` job; publishing from inside a `workflow_call` workflow reports the wrong workflow to npm. Also confirm `npm --version` in the job log is at least 11.5.1 and that `setup-node` has no `registry-url` and no `NODE_AUTH_TOKEN` is set (both override OIDC).

**Provenance errors.** Provenance needs a public repository, `id-token: write` on the job, `repository.url` in each `package.json` matching `https://github.com/berkcansavur/relaybus`, and a publish from CI (it cannot be created locally, so `--dry-run` omits it).

**Partially failed release (some packages published, some not).** Publishing is idempotent. semantic-release has already created the tag at that point, so a plain re-run would see "no new commits". Recover with:

```bash
git push origin :refs/tags/v0.1.0     # delete the remote tag (and any local one)
```

then re-run the failed `Release` workflow run from the Actions tab ("Re-run all jobs"; a new `chore` commit would not trigger a release). semantic-release recomputes the same version, `publish-packages.mjs` skips every package already on npm and publishes the rest, and the GitHub Release is created last. If a GitHub Release already exists for the tag, delete it first.

**A release was not created although commits were merged.** Only `feat`, `fix`, `perf`, `revert` and breaking changes release. Check the squash commit message on `main` (it is the PR title) and the "Analyzing commit" lines in the `Release` job log.

**"This branch is not in the branches configuration" or first release is `1.0.0`.** The `v0.0.0` tag is missing or not reachable from the branch; create it as described in the bootstrap section.

**Hook does not run locally.** Run `pnpm install` (the `prepare` script runs `husky`). Skipping the hook is possible with `git commit --no-verify`, but the PR title check still applies.

## Enabling releases

The release job runs only when the repository variable `RELEASE_ENABLED` is `true`
(Settings → Secrets and variables → Actions → Variables). Set it after the bootstrap
publish and the Trusted Publisher setup above; until then pushes to `main` run the full
CI gate and skip the release without creating tags.

## Trusted Publisher permissions

npm creates new trusted publishers with **staged publishing only** (since 2026-09-03):
a CI publish then fails with `E403 OIDC permission denied for this action`. Each package
needs direct publishing allowed (`createPackage`). `scripts/trust-packages.sh` configures
all packages with `npm trust github … --allow-publish --allow-stage-publish`, replacing a
staged-only configuration of the same workflow (npm allows one per workflow).
