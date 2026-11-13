// semantic-release configuration (lockstep release of every @relaybus/* package).
// See docs/releasing.md for the rules and the 1.0.0 graduation steps.

/**
 * Commit analyzer rules.
 *
 * For each commit the analyzer takes the highest release of every matching rule. A
 * matching custom rule replaces the preset's default rules, so the `breaking` rule must
 * stay: without it a breaking `feat!` commit would only match the `feat` rule and
 * release a minor.
 */
export const releaseRules = [
  { breaking: true, release: 'major' },
  { revert: true, release: 'patch' },
  { type: 'revert', release: 'patch' },
  { type: 'feat', release: 'minor' },
  { type: 'fix', release: 'patch' },
  { type: 'perf', release: 'patch' },
  { type: 'docs', release: false },
  { type: 'chore', release: false },
  { type: 'test', release: false },
  { type: 'ci', release: false },
  { type: 'build', release: false },
  // Refactors change the published code (same behaviour), so they ship as a patch.
  { type: 'refactor', release: 'patch' },
  { type: 'style', release: false },
];

export const commitAnalyzerOptions = {
  preset: 'conventionalcommits',
  releaseRules,
};

export default {
  branches: [
    'main',
    { name: 'next', prerelease: true, channel: 'next' },
    { name: 'beta', prerelease: true, channel: 'beta' },
  ],
  tagFormat: 'v${version}',
  plugins: [
    ['@semantic-release/commit-analyzer', commitAnalyzerOptions],
    [
      '@semantic-release/release-notes-generator',
      {
        preset: 'conventionalcommits',
        presetConfig: {
          types: [
            { type: 'feat', section: 'Features' },
            { type: 'fix', section: 'Bug Fixes' },
            { type: 'perf', section: 'Performance Improvements' },
            { type: 'revert', section: 'Reverts' },
            { type: 'docs', section: 'Documentation', hidden: true },
            { type: 'chore', hidden: true },
            { type: 'test', hidden: true },
            { type: 'ci', hidden: true },
            { type: 'build', hidden: true },
            { type: 'refactor', section: 'Code Refactoring' },
            { type: 'style', hidden: true },
          ],
        },
      },
    ],
    [
      '@semantic-release/exec',
      {
        // Tag-only strategy: versions are written into the CI workspace only.
        prepareCmd: 'node scripts/set-version.mjs ${nextRelease.version} && pnpm build',
        publishCmd:
          "node scripts/publish-packages.mjs ${nextRelease.version} ${nextRelease.channel || 'latest'}",
      },
    ],
    '@semantic-release/github',
  ],
};
