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
      'core.ConfigurationError',
      'core.DispatcherStoppedError',
      'core.EventValidationError',
      'core.HandlerTimeoutError',
      'core.InvalidEnvelopeError',
      'core.LeaseLostError',
      'core.NonRetryableError',
      'core.RelayBusError',
      'core.TransactionRequiredError',
      'kafka.HandlerFailedError',
      'kafka.InvalidKafkaMessageError',
      'kafka.PublishTimeoutError',
      'kafka.UnencodableEventError',
      'rabbitmq.AmqpLimitExceededError',
      'rabbitmq.MalformedMessageError',
      'rabbitmq.RabbitMqPublishRejectedError',
      'rabbitmq.RabbitMqUnavailableError',
      'rabbitmq.UnroutableMessageError',
      'rabbitmq.UnserializablePayloadError',
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
});
