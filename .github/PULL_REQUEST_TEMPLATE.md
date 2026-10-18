## Summary

<!-- What does this change and why? Link related issues: Closes #123 -->

## Checklist

- [ ] Tests added or updated (unit, and integration where a store or transport is touched)
- [ ] `pnpm coverage` passes with 100% statements, branches, functions and lines (no ignore comments)
- [ ] New event store implementations run the shared contract suite from `@relaybus/testing`
- [ ] The PR title follows Conventional Commits with a valid scope; it becomes the squash commit and drives the release version (see `docs/releasing.md`)
- [ ] Documentation updated (README, `docs/`, JSDoc)
- [ ] `pnpm format:check`, `pnpm lint` and `pnpm typecheck` pass
