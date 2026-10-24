#!/usr/bin/env bash
# Configures npm Trusted Publishing for every @relaybus package so the release
# workflow may publish directly (and stage). Replaces older configurations of the
# same workflow that lack direct publishing (npm allows only one per workflow).
# Run after `npm login`; npm asks for 2FA in the browser. Tick "do not challenge
# for 5 minutes" so the remaining packages go through without another prompt.
set -uo pipefail

npm_cli() { npx -y npm@11.20.0 "$@"; }
# `npm trust list --json` writes its JSON to stderr.
list_json() { npm_cli trust list "$1" --json 2>&1 | grep -v '^npm notice'; }
ids_without_publish() {
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const t=s.trim();if(!t)return;const v=JSON.parse(t);const a=Array.isArray(v)?v:[v];console.log(a.filter(c=>!(c.permissions||[]).includes("createPackage")).map(c=>c.id).join(" "))})'
}
permissions() {
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const t=s.trim();if(!t){console.log("none");return}const v=JSON.parse(t);const a=Array.isArray(v)?v:[v];console.log(a.map(c=>c.permissions.join("+")).join(" | "))})'
}

for name in core testing mongodb kafka nestjs postgres rabbitmq eslint-plugin; do
  pkg="@relaybus/$name"
  for id in $(list_json "$pkg" | ids_without_publish); do
    npm_cli trust revoke "$pkg" --id="$id" 2>&1 | grep -v '^npm notice' | tail -1
  done
  npm_cli trust github "$pkg" --file release.yml --repository berkcansavur/relaybus \
    --environment npm-publish --allow-publish --allow-stage-publish --yes 2>&1 \
    | grep -E 'error|E40' | head -2
  echo "== $pkg: $(list_json "$pkg" | permissions)"
done
