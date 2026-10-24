import { assertEventType } from '../event.js';

/** The media type of a content type, without parameters and lower-cased; undefined for anything but a string. */
export function mediaTypeOf(contentType: unknown): string | undefined {
  return typeof contentType === 'string'
    ? contentType.split(';')[0]!.trim().toLowerCase()
    : undefined;
}

export function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Readers of structured CloudEvent attributes that fail with the error of the calling transport. */
export interface CloudEventReaders {
  readonly optionalAttribute: (body: Record<string, unknown>, name: string) => string | undefined;
  readonly requiredAttribute: (body: Record<string, unknown>, name: string) => string;
  readonly assertValidType: (type: string, label: string) => void;
  readonly assertJsonDataContentType: (dataContentType: string | undefined) => void;
}

export function createCloudEventReaders(invalid: (reason: string) => Error): CloudEventReaders {
  const optionalAttribute = (body: Record<string, unknown>, name: string): string | undefined => {
    const value = body[name];
    if (value !== undefined && typeof value !== 'string') {
      throw invalid(`Attribute '${name}' must be a string`);
    }
    return value;
  };
  return {
    optionalAttribute,
    requiredAttribute: (body, name) => {
      const value = optionalAttribute(body, name);
      if (value === undefined || value.length === 0) {
        throw invalid(`Attribute '${name}' is missing or empty`);
      }
      return value;
    },
    assertValidType: (type, label) => {
      try {
        assertEventType(type);
      } catch (error) {
        throw invalid(`${label} is not a valid event type: ${(error as Error).message}`);
      }
    },
    assertJsonDataContentType: (dataContentType) => {
      const mediaType = mediaTypeOf(dataContentType);
      if (
        mediaType !== undefined &&
        mediaType !== 'application/json' &&
        mediaType !== 'text/json' &&
        !mediaType.endsWith('+json')
      ) {
        throw invalid(`datacontenttype must be JSON, received '${dataContentType}'`);
      }
    },
  };
}
