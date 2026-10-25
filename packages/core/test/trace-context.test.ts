import { describe, expect, it } from 'vitest';
import { TRACEPARENT_HEADER, TRACESTATE_HEADER, traceContextOf } from '@relaybus/core';

const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';
const SPAN_ID = 'b7ad6b7169203331';

describe('traceContextOf', () => {
  it('reads a W3C trace context with and without tracestate', () => {
    const traceparent = `00-${TRACE_ID}-${SPAN_ID}-01`;
    expect(traceContextOf({ [TRACEPARENT_HEADER]: traceparent })).toEqual({ traceparent });
    expect(
      traceContextOf({ [TRACEPARENT_HEADER]: traceparent, [TRACESTATE_HEADER]: 'congo=t61' }),
    ).toEqual({ traceparent, tracestate: 'congo=t61' });
  });

  it('accepts later versions with extra fields', () => {
    const traceparent = `01-${TRACE_ID}-${SPAN_ID}-00-extra`;
    expect(traceContextOf({ traceparent })).toEqual({ traceparent });
  });

  it.each([
    ['missing', undefined],
    ['version 00 with an extra field', `00-${TRACE_ID}-${SPAN_ID}-01-extra`],
    ['the forbidden version ff', `ff-${TRACE_ID}-${SPAN_ID}-01`],
    ['an all-zero trace id', `00-${'0'.repeat(32)}-${SPAN_ID}-01`],
    ['an all-zero span id', `00-${TRACE_ID}-${'0'.repeat(16)}-01`],
    ['upper-case hex', `00-${TRACE_ID.toUpperCase()}-${SPAN_ID}-01`],
    ['a short trace id', `00-${TRACE_ID.slice(1)}-${SPAN_ID}-01`],
  ])('ignores %s', (_case, traceparent) => {
    const headers = traceparent === undefined ? {} : { traceparent, tracestate: 'congo=t61' };
    expect(traceContextOf(headers)).toBeUndefined();
  });
});
