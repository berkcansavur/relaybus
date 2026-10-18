#!/usr/bin/env bash
# One-time bootstrap: publishes every @relaybus/* package at 0.0.0 from your machine
# so that a Trusted Publisher can be attached to each package on npmjs.com.
# Run `npm login` (with 2FA) first. Packages that already exist at 0.0.0 are skipped.
set -euo pipefail

cd "$(dirname "$0")/.."
VERSION=0.0.0

npm whoami >/dev/null 2>&1 || { echo "Run 'npm login' first." >&2; exit 1; }

pnpm install --frozen-lockfile
pnpm build

PACK_DIR="$(mktemp -d)"
trap 'rm -rf "$PACK_DIR"' EXIT

for dir in core testing nestjs postgres mongodb kafka rabbitmq; do
  pkg="packages/$dir"
  name="$(node -p "require('./$pkg/package.json').name")"
  current="$(node -p "require('./$pkg/package.json').version")"
  if [ "$current" != "$VERSION" ]; then
    echo "$name is at $current in package.json, expected $VERSION. Aborting." >&2
    exit 1
  fi
  if npm view "$name@$VERSION" version >/dev/null 2>&1; then
    echo "skip   $name@$VERSION (already on npm)"
    continue
  fi
  echo "pack   $name@$VERSION"
  (cd "$pkg" && pnpm pack --pack-destination "$PACK_DIR" >/dev/null)
  tarball="$PACK_DIR/$(echo "${name#@}" | tr '/' '-')-$VERSION.tgz"
  echo "publish $name@$VERSION"
  # Provenance needs a CI identity; the local bootstrap publish cannot have it.
  npm publish "$tarball" --access public --provenance=false
done

echo "Done. Now attach a Trusted Publisher to each package (see docs/releasing.md)."
