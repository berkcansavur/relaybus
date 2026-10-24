/**
 * Helpers shared by the relaybus packages, published as `@relaybus/core/internal`.
 *
 * @internal Not part of the public API and not covered by semantic versioning: it changes
 * with the other relaybus packages of the same release line. Do not import it from
 * application code.
 *
 * It lives in core, not in a copy per package, because its errors must be the
 * `ConfigurationError` of the one core instance an application loads, so `instanceof`
 * keeps working across packages.
 */
export { assertPositiveInteger } from './validation.js';
export { fromHeader, fromPayloadField } from './field-extractors.js';
export {
  createCloudEventReaders,
  isJsonObject,
  mediaTypeOf,
  writeStructuredCloudEvent,
  type CloudEventReaders,
} from './cloudevents.js';
export { schedule, sleep, type CancelTimer, type ScheduledTask } from './timers.js';
export {
  createJsonEnvelopeFormat,
  type BodyEnvelopeMapping,
  type BodyField,
  type FixedType,
  type HeaderField,
  type JsonEnvelopeFailures,
  type JsonEnvelopeFields,
  type JsonEnvelopeFormat,
  type JsonEnvelopeMapping,
  type JsonWireMessage,
  type PayloadOnlyMapping,
  type WireField,
} from './json-envelope.js';
