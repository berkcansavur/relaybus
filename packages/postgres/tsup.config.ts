import { defineConfig } from 'tsup';
import { packageBuild } from '../../tsup.base';

export default defineConfig({
  ...packageBuild,
  entry: {
    index: 'src/index.ts',
    kysely: 'src/kysely/index.ts',
    knex: 'src/knex/index.ts',
    typeorm: 'src/typeorm/index.ts',
    drizzle: 'src/drizzle/index.ts',
    prisma: 'src/prisma/index.ts',
  },
});
