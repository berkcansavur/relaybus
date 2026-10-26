// The mixed-version rollout test runs the published @relaybus/postgres@0.4.0, which must resolve
// the published @relaybus/core@0.4.0 rather than the workspace core. Scoped to that exact version.
function readPackage(pkg) {
  if (pkg.name === '@relaybus/postgres' && pkg.version === '0.4.0') {
    delete pkg.peerDependencies['@relaybus/core'];
    pkg.dependencies = { ...pkg.dependencies, '@relaybus/core': 'npm:@relaybus/core@0.4.0' };
  }
  return pkg;
}

module.exports = { hooks: { readPackage } };
