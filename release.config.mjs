// semantic-release configuration (lockstep release of every @relaybus/* package).
// See docs/releasing.md for the rules and the 1.0.0 graduation steps.

/**
 * Commit analyzer rules for the 0.x phase.
 *
 * The first matching rule wins. Breaking changes map to a MINOR bump while the
 * packages are below 1.0.0. To graduate to 1.0.0 remove the `breaking` rule
 * (the preset default is then `major`) and cut the release with a
 * `BREAKING CHANGE:` commit.
 */
export const releaseRules = [
  { breaking: true, release: 'minor' },
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
  { type: 'refactor', release: false },
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
            { type: 'refactor', hidden: true },
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
