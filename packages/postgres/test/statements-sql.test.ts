import { describe, expect, it } from 'vitest';
import type { DeliveryQuery } from '@relaybus/core';
import { resolvePostgresNames, type PostgresSchemaOptions } from '../src/names.js';
import { createPostgresSchemaStatements } from '../src/schema.js';
import { PostgresStatements } from '../src/statements.js';

const configurations: Record<string, PostgresSchemaOptions> = {
  defaults: {},
  'custom names with completion history': {
    schema: 'bus',
    tablePrefix: 'x_',
    completionHistory: true,
  },
};

const listQueries: readonly DeliveryQuery[] = [
  {},
  { status: 'pending' },
  { status: 'processing' },
  { status: 'completed', eventType: 'order.placed.v1', eventId: 'e-1' },
  { status: 'dead', handler: 'h', limit: 3 },
];

/** Every statement text of a group, by member name; methods are called with fixed arguments. */
function render(group: object): Record<string, unknown> {
  const rendered: Record<string, unknown> = {};
  const descriptors = Object.getOwnPropertyDescriptors(Object.getPrototypeOf(group));
  for (const [name, descriptor] of Object.entries(descriptors).sort(([a], [b]) =>
    a.localeCompare(b),
  )) {
    const member = descriptor.get ?? descriptor.value;
    if (name === 'constructor' || typeof member !== 'function') {
      continue;
    }
    rendered[name] =
      name === 'listDeliveries'
        ? listQueries.map((query) => (member as (q: DeliveryQuery) => unknown).call(group, query))
        : (member as () => unknown).call(group);
  }
  return rendered;
}

// The SQL the store sends. A refactoring must leave every text byte for byte as it is; a deliberate
// change of a statement updates the snapshot (`vitest -u`) and says so in the commit.
describe('statement SQL', () => {
  it.each(Object.entries(configurations))('is unchanged with %s', (_label, options) => {
    const statements = new PostgresStatements(resolvePostgresNames(options));
    const groups = Object.fromEntries(
      Object.entries(statements).map(([groupName, group]) => [groupName, render(group as object)]),
    );
    expect(groups).toMatchSnapshot();
  });

  it.each(Object.entries(configurations))('creates the same schema with %s', (_label, options) => {
    expect(createPostgresSchemaStatements(options)).toMatchSnapshot();
  });
});
