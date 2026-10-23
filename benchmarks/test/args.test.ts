import { describe, expect, it } from 'vitest';
import { parseArguments } from '../lib/args.ts';

describe('parseArguments', () => {
  it('applies defaults', () => {
    expect(parseArguments(['quick'])).toEqual({
      mode: 'quick',
      base: 'main',
      candidate: 'HEAD',
      pairs: undefined,
      only: [],
      accept: [],
      interleave: false,
      useBaseline: true,
      store: true,
      force: false,
      strict: false,
      prepareOnly: false,
    });
  });

  it('reads values and switches', () => {
    const options = parseArguments([
      'full',
      '--base',
      'v1',
      '--candidate',
      'feat',
      '--pairs',
      '4',
      '--only',
      'pg-pub, mongo',
      '--accept',
      'deliveries.delPerUnit',
      '--interleave',
      '--no-baseline',
      '--no-store',
      '--force',
      '--strict',
      '--prepare',
    ]);
    expect(options).toMatchObject({
      mode: 'full',
      base: 'v1',
      candidate: 'feat',
      pairs: 4,
      only: ['pg-pub', 'mongo'],
      accept: ['deliveries.delPerUnit'],
      interleave: true,
      useBaseline: false,
      store: false,
      force: true,
      strict: true,
      prepareOnly: true,
    });
  });

  it('rejects unknown input instead of measuring the wrong thing', () => {
    expect(() => parseArguments([])).toThrow('Usage');
    expect(() => parseArguments(['slow'])).toThrow('Usage');
    expect(() => parseArguments(['quick', '--bogus'])).toThrow('Unknown option');
    expect(() => parseArguments(['quick', '--base'])).toThrow('needs a value');
    expect(() => parseArguments(['quick', '--base', '--force'])).toThrow('needs a value');
    expect(() => parseArguments(['quick', '--pairs', '0'])).toThrow('positive integer');
  });
});
