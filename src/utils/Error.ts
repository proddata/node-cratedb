import { CrateDBErrorResponse } from '../interfaces';

export class CrateDBError extends Error {
  constructor(
    message: string,
    public readonly code: number,
    public readonly error_trace?: string,
    public readonly statusCode?: number
  ) {
    super(message);
    this.name = 'CrateDBError';
    Object.setPrototypeOf(this, CrateDBError.prototype);
  }

  static fromResponse(response: CrateDBErrorResponse, statusCode?: number): CrateDBError {
    return new CrateDBError(response.error.message, response.error.code, response.error_trace, statusCode);
  }
}

export class DeserializationError extends Error {
  constructor(message = 'Deserialization failed', options?: { cause?: Error }) {
    super(message, options);
    this.name = 'DeserializationError';
    Object.setPrototypeOf(this, DeserializationError.prototype);
  }
}

export class RequestError extends Error {
  constructor(message: string, options?: { cause?: Error }) {
    super(message, options);
    this.name = 'RequestError';
    Object.setPrototypeOf(this, RequestError.prototype);
  }
}
