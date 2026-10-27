import { describe, expect, it } from 'vitest';
import * as core from '@relaybus/core';
import * as kafka from '@relaybus/kafka';
import * as mongodb from '@relaybus/mongodb';
import * as nestjs from '@relaybus/nestjs';
import * as postgres from '@relaybus/postgres';
import * as rabbitmq from '@relaybus/rabbitmq';
import * as testing from '@relaybus/testing';

type ErrorClass = new (message: string) => Error;

const packages = { core, kafka, mongodb, nestjs, postgres, rabbitmq, testing };

function isErrorClass(value: unknown): value is ErrorClass {
  return typeof value === 'function' && value.prototype instanceof Error;
}

/** Every error class exported by a published package, found by walking the exports. */
const errorClasses = Object.entries(packages).flatMap(([packageName, exported]) =>
  Object.entries(exported)
    .filter((entry): entry is [string, ErrorClass] => isErrorClass(entry[1]))
    .map(([exportName, ErrorClass]) => ({ packageName, exportName, ErrorClass })),
);

describe('exported error classes', () => {
  it('finds every error class of every package', () => {
    expect(
      errorClasses.map(({ packageName, exportName }) => `${packageName}.${exportName}`).sort(),
    ).toEqual([
      'core.AttemptsExhaustedError',
      'core.ConfigurationError',
      'core.DispatcherStoppedError',
      'core.DrainIncompleteError',
      'core.DuplicateRouteEventError',
      'core.EventValidationError',
      'core.HandlerTimeoutError',
      'core.InvalidEnvelopeError',
      'core.LeaseLostError',
      'core.LifecycleError',
      'core.NonRetryableError',
      'core.RelayBusError',
      'core.TransactionIsolationConflictError',
      'core.TransactionRequiredError',
      'core.TransactionTimeoutError',
      'core.UnknownCatalogEventError',
      'core.WireTypeCollisionError',
      'core.WireTypesUnsupportedError',
      'kafka.GroupJoinTimeoutError',
      'kafka.HandlerFailedError',
      'kafka.InvalidKafkaMessageError',
      'kafka.PublishTimeoutError',
      'kafka.TransportClosedError',
      'kafka.UnencodableEventError',
      'postgres.ReconcileFailedError',
      'rabbitmq.AmqpLimitExceededError',
      'rabbitmq.MalformedMessageError',
      'rabbitmq.RabbitMqPublishRejectedError',
      'rabbitmq.RabbitMqUnavailableError',
      'rabbitmq.RabbitMqUnencodableEventError',
      'rabbitmq.UnroutableMessageError',
      'rabbitmq.UnserializablePayloadError',
      'testing.CodecContractError',
    ]);
  });

  it.each(
    errorClasses.map((entry) => [`${entry.packageName}.${entry.exportName}`, entry] as const),
  )('%s names its instances after the class', (_, { exportName, ErrorClass }) => {
    const error = new ErrorClass('boom');
    expect(error.name).toBe(ErrorClass.name);
    expect(error.name).toBe(exportName);
    expect(core.toErrorInfo(error).name).toBe(exportName);
  });

  it('gives every class a distinct name', () => {
    const names = errorClasses.map(({ ErrorClass }) => new ErrorClass('boom').name);
    expect(new Set(names).size).toBe(names.length);
  });

  it('gives every class a distinct code', () => {
    const codes = errorClasses.map(
      ({ ErrorClass }) => (new ErrorClass('boom') as core.RelayBusError).code,
    );
    expect(new Set(codes).size).toBe(codes.length);
  });

  // The documented code table (packages/core/README.md, docs-site/errors.md). Codes are part of
  // the public contract: change this table only on purpose, never rename or reuse a code.
  it('keeps the code table stable', () => {
    const table = Object.fromEntries(
      errorClasses.map(({ exportName, ErrorClass }) => [
        exportName,
        (new ErrorClass('boom') as core.RelayBusError).code,
      ]),
    );
    expect(table).toEqual({
      ConfigurationError: 'RELAYBUS_CONFIGURATION',
      DispatcherStoppedError: 'RELAYBUS_DISPATCHER_STOPPED',
      EventValidationError: 'RELAYBUS_EVENT_VALIDATION',
      HandlerTimeoutError: 'RELAYBUS_HANDLER_TIMEOUT',
      InvalidEnvelopeError: 'RELAYBUS_INVALID_ENVELOPE',
      LeaseLostError: 'RELAYBUS_LEASE_LOST',
      NonRetryableError: 'RELAYBUS_NON_RETRYABLE',
      RelayBusError: 'RELAYBUS_ERROR',
      TransactionIsolationConflictError: 'RELAYBUS_TRANSACTION_ISOLATION_CONFLICT',
      TransactionRequiredError: 'RELAYBUS_TRANSACTION_REQUIRED',
      TransactionTimeoutError: 'RELAYBUS_TRANSACTION_TIMEOUT',
      AttemptsExhaustedError: 'RELAYBUS_ATTEMPTS_EXHAUSTED',
      LifecycleError: 'RELAYBUS_LIFECYCLE',
      DrainIncompleteError: 'RELAYBUS_DRAIN_INCOMPLETE',
      DuplicateRouteEventError: 'RELAYBUS_ROUTE_DUPLICATE_EVENT',
      UnknownCatalogEventError: 'RELAYBUS_CATALOG_UNKNOWN_EVENT',
      WireTypeCollisionError: 'RELAYBUS_WIRE_TYPE_COLLISION',
      WireTypesUnsupportedError: 'RELAYBUS_WIRE_TYPES_UNSUPPORTED',
      TransportClosedError: 'RELAYBUS_KAFKA_TRANSPORT_CLOSED',
      GroupJoinTimeoutError: 'RELAYBUS_KAFKA_GROUP_JOIN_TIMEOUT',
      ReconcileFailedError: 'RELAYBUS_POSTGRES_RECONCILE_FAILED',
      HandlerFailedError: 'RELAYBUS_KAFKA_HANDLER_FAILED',
      InvalidKafkaMessageError: 'RELAYBUS_KAFKA_INVALID_MESSAGE',
      PublishTimeoutError: 'RELAYBUS_KAFKA_PUBLISH_TIMEOUT',
      UnencodableEventError: 'RELAYBUS_KAFKA_UNENCODABLE_EVENT',
      AmqpLimitExceededError: 'RELAYBUS_RABBITMQ_AMQP_LIMIT_EXCEEDED',
      MalformedMessageError: 'RELAYBUS_RABBITMQ_MALFORMED_MESSAGE',
      RabbitMqPublishRejectedError: 'RELAYBUS_RABBITMQ_PUBLISH_REJECTED',
      RabbitMqUnavailableError: 'RELAYBUS_RABBITMQ_UNAVAILABLE',
      RabbitMqUnencodableEventError: 'RELAYBUS_RABBITMQ_UNENCODABLE_EVENT',
      UnroutableMessageError: 'RELAYBUS_RABBITMQ_UNROUTABLE_MESSAGE',
      UnserializablePayloadError: 'RELAYBUS_RABBITMQ_UNSERIALIZABLE_PAYLOAD',
      CodecContractError: 'RELAYBUS_TESTING_CODEC_CONTRACT',
    });
  });

  it.each(
    errorClasses.map((entry) => [`${entry.packageName}.${entry.exportName}`, entry] as const),
  )('%s carries a RELAYBUS_ code into its ErrorInfo', (_, { ErrorClass }) => {
    const error = new ErrorClass('boom') as core.RelayBusError;
    expect(error).toBeInstanceOf(core.RelayBusError);
    expect(error.code).toMatch(/^RELAYBUS_[A-Z0-9]+(?:_[A-Z0-9]+)*$/);
    expect(core.toErrorInfo(error)).toEqual({
      name: error.name,
      message: 'boom',
      stack: error.stack,
      code: error.code,
    });
  });

  it('leaves the ErrorInfo of other errors without a code', () => {
    expect(core.toErrorInfo(new TypeError('boom'))).not.toHaveProperty('code');
    expect(
      core.toErrorInfo(Object.assign(new Error('boom'), { code: 'ECONNRESET' })),
    ).not.toHaveProperty('code');
    expect(core.toErrorInfo('boom')).toEqual({ name: 'NonError', message: 'boom' });
  });

  it('lets a subclass outside relaybus inherit the generic code', () => {
    class CustomError extends core.RelayBusError {}
    expect(new CustomError('boom').code).toBe('RELAYBUS_ERROR');
  });
});
