import { ColumnTypes } from './utils/ColumnTypes.js';
import { CrateDBBaseResponse, DeserializationConfig } from './interfaces.js';
import { DeserializationError } from './utils/Error.js';

type Context = {
  source: string;
};

export class Serializer {
  static serialize(obj: unknown): string {
    return JSON.stringify(obj, this.replacer);
  }

  private static replacer(_: unknown, value: unknown) {
    if (typeof value === 'bigint') {
      return JSON.rawJSON(value.toString());
    }
    if (value instanceof Map) {
      return Object.fromEntries(value);
    }
    if (value instanceof Set) {
      return Array.from(value);
    }
    return value;
  }

  static deserialize(str: string, config: DeserializationConfig): CrateDBBaseResponse {
    try {
      return this._deserialize(str, config);
    } catch {
      throw new DeserializationError('Deserialization of response body failed');
    }
  }

  private static _deserialize(str: string, config: DeserializationConfig): CrateDBBaseResponse {
    const obj = config.long === 'bigint' ? JSON.parse(str, this.reviver) : JSON.parse(str);

    obj.col_types?.forEach((type: number | number[], index: number) => {
      // Extract the base type even from nested arrays
      const baseType = this.extractBaseType(type);

      switch (baseType) {
        case ColumnTypes.BIGINT:
          if (config.long === 'bigint') {
            obj.rows?.forEach((row: Array<unknown>) => {
              row[index] = this.recursiveConvert(row[index], (val: number) => BigInt(String(val)));
            });
          }
          break;
        case ColumnTypes.DATE:
          if (config.date === 'date') {
            obj.rows?.forEach((row: Array<unknown>) => {
              row[index] = this.recursiveConvert(row[index], (val: number) => new Date(val));
            });
          }
          break;
        case ColumnTypes.TIMESTAMP_WITH_TIME_ZONE:
        case ColumnTypes.TIMESTAMP_WITHOUT_TIME_ZONE:
          if (config.timestamp === 'date') {
            obj.rows?.forEach((row: Array<unknown>) => {
              row[index] = this.recursiveConvert(row[index], (val: number) => new Date(val));
            });
          }
          break;
        default:
        // No special handling for other types
      }
    });

    return obj;
  }

  private static extractBaseType(type: number | number[]): number {
    return Array.isArray(type) ? this.extractBaseType(type[1]) : type;
  }

  private static recursiveConvert(cell: unknown, converter: (val: number) => unknown): unknown {
    if (Array.isArray(cell)) {
      return cell.map((item) => this.recursiveConvert(item, converter));
    }
    if (typeof cell === 'number') {
      return converter(cell);
    }
    return cell;
  }

  private static reviver(_: unknown, value: unknown, context: Context | null = null): unknown {
    //if number is greater than Number.MAX_SAFE_INTEGER and not a float
    if (
      typeof value === 'number' &&
      value > Number.MAX_SAFE_INTEGER &&
      context !== null &&
      !context.source.includes('.')
    ) {
      return BigInt(context.source);
    }
    return value;
  }
}
