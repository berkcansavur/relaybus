#!/usr/bin/env bash
# Hardens every @relaybus package on npm:
#  - publishing requires 2FA and refuses tokens (trusted publishing still works)
#  - deprecates the bootstrap placeholders 0.0.0 and the stray 0.0.0-stage
# Run in an interactive terminal: npm opens a browser 2FA prompt for the first
# command. Tick "do not challenge for 5 minutes" so the rest go through.
set -uo pipefail
npm_cli() { npx -y npm@11.20.0 "$@"; }
message="Bootstrap placeholder. Install the latest version."
for name in core testing mongodb kafka nestjs postgres rabbitmq eslint-plugin; do
  pkg="@relaybus/$name"
  echo "== $pkg"
  npm_cli access set mfa=publish "$pkg"
  npm_cli deprecate "$pkg@0.0.0" "$message"
  npm_cli deprecate "$pkg@0.0.0-stage" "$message"
done
