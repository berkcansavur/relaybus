// Generates the Prisma Client the @relaybus/postgres integration tests use (schema in
// packages/postgres/test/prisma/schema.prisma) into the git-ignored `generated` folder next to it.
// Runs from `prepare` after every `pnpm install`, so typecheck, lint and tests find the client.
import { execFileSync } from 'node:child_process';
import { existsSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const postgres = join(import.meta.dirname, '..', 'packages', 'postgres');
const prisma = join(postgres, 'node_modules', '.bin', 'prisma');
const generated = join(postgres, 'test', 'prisma', 'generated');

if (existsSync(prisma)) {
  // A client another Prisma major generated would otherwise stay next to the new one.
  rmSync(generated, { recursive: true, force: true });
  execFileSync(prisma, ['generate', '--schema', 'test/prisma/schema.prisma'], {
    cwd: postgres,
    stdio: ['ignore', 'ignore', 'inherit'],
    env: { ...process.env, CHECKPOINT_DISABLE: '1', PRISMA_HIDE_UPDATE_MESSAGE: '1' },
  });
}

// Prisma 5 has no `prisma-client` generator: its CI job generates with `prisma-client-js`, which writes
// a CommonJS `index.js` instead of `client.ts`. Two files then give the tests the same import path.
if (existsSync(join(generated, 'index.js')) && !existsSync(join(generated, 'client.ts'))) {
  writeFileSync(
    join(generated, 'client.js'),
    "import { createRequire } from 'node:module';\n\nexport const { PrismaClient } = createRequire(import.meta.url)('./index.js');\n",
  );
  writeFileSync(join(generated, 'client.d.ts'), "export { PrismaClient } from './index.js';\n");
}
