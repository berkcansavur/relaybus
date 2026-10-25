// Generates the Prisma Client the @relaybus/postgres integration tests use (schema in
// packages/postgres/test/prisma/schema.prisma) into the git-ignored `generated` folder next to it.
// Runs from `prepare` after every `pnpm install`, so typecheck, lint and tests find the client.
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join } from 'node:path';

const postgres = join(import.meta.dirname, '..', 'packages', 'postgres');
const prisma = join(postgres, 'node_modules', '.bin', 'prisma');

if (existsSync(prisma)) {
  execFileSync(prisma, ['generate', '--schema', 'test/prisma/schema.prisma'], {
    cwd: postgres,
    stdio: ['ignore', 'ignore', 'inherit'],
    env: { ...process.env, CHECKPOINT_DISABLE: '1', PRISMA_HIDE_UPDATE_MESSAGE: '1' },
  });
}
